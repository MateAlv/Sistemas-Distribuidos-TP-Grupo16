import logging
import zlib
from dataclasses import dataclass

from common.fault_tolerance.handler import PersistentStateHandler
from common.fault_tolerance.handler.action import Action
from common.fault_tolerance.handler.sender_sequencer import EdgeSpec
from common.fault_tolerance.inbox import MsgKind
from common.logging_utils import should_log_progress
from common.message_protocol.external import FileChunk, FileEof
from common.message_protocol.external.types import (
    MSG_CHUNK,
    MSG_EOF,
    MSG_ABORT,
    file_ingestor_routing_key,
)
from common.message_protocol.internal import InternalProtocol, MessageType
from common.middleware import (
    MessageMiddlewareQueueRabbitMQ,
    ShardedPublisher,
)
from common.middleware.middleware_rabbitmq import (
    MessageMiddlewareExchangeRabbitMQ,
    ensure_exchange_queue_bindings,
)
from common.routing import queue_name_for_worker
from workers.file_splitter.file_splitter_state import (
    ACCOUNTS_EDGE,
    LINE_BATCH_EDGE,
    FileKey,
    FileSplitterState,
)

_CLIENT_EOF_SENDER = 0xFFFFFFFF
# Fixed sender_id for abort messages (MsgKind.ABORT has its own inbox bucket
# so there is no collision with real DATA or EOF senders).
_ABORT_SENDER = 0


@dataclass(frozen=True)
class FileSplitterConfig:
    id: int
    mom_host: str
    input_exchange: str
    queue_name: str
    output_exchange: str
    output_routing_prefix: str
    output_shard_count: int
    max_line_bytes: int
    max_batch_bytes: int
    logging_level: str
    input_routing_key: str | None = None
    accounts_output_queue: str | None = None
    state_dir: str = ""
    snapshot_interval: int = 1000


class FileSplitter:
    def __init__(self, config: FileSplitterConfig) -> None:
        self._config = config
        self._consumer: MessageMiddlewareExchangeRabbitMQ | None = None
        self._internal_protocol = InternalProtocol()
        self._state = FileSplitterState(
            max_line_bytes=config.max_line_bytes,
            max_batch_bytes=config.max_batch_bytes,
            splitter_id=config.id,
            accounts_enabled=config.accounts_output_queue is not None,
        )
        self._handler: PersistentStateHandler | None = None
        self._publishers: dict[str, object] = {}
        self._chunks_received = 0
        self._eofs_received = 0

    def start(self) -> None:
        routing_key = self._input_routing_key()
        self._ensure_line_batch_bindings()
        self._handler = self._build_handler()
        self._publishers = self._build_publishers()
        self._handler.recover()
        self._publish(self._handler.outbox_to_republish())

        logging.info(
            "file_splitter_start | id=%s | mom_host=%s | exchange=%s | queue=%s | "
            "routing_key=%s | output_exchange=%s | output_prefix=%s | "
            "output_shards=%s | max_batch_bytes=%s | snapshot_every=%s",
            self._config.id,
            self._config.mom_host,
            self._config.input_exchange,
            self._config.queue_name,
            routing_key,
            self._config.output_exchange,
            self._config.output_routing_prefix,
            self._config.output_shard_count,
            self._config.max_batch_bytes,
            self._config.snapshot_interval,
        )

        with MessageMiddlewareExchangeRabbitMQ(
            host=self._config.mom_host,
            exchange_name=self._config.input_exchange,
            routing_keys=[routing_key],
            queue_name=self._config.queue_name,
            exclusive=False,
        ) as consumer:
            self._consumer = consumer
            try:
                consumer.start_consuming(self._on_message)
            finally:
                self._consumer = None
                self._close_outputs()

    def stop(self) -> None:
        logging.info("file_splitter_stop | id=%s", self._config.id)
        if self._consumer is not None:
            self._consumer.request_stop_consuming()

    def _build_handler(self) -> PersistentStateHandler:
        return PersistentStateHandler(
            state_dir=self._config.state_dir,
            node_id=f"file_splitter_{self._config.id}",
            worker_state=self._state,
            snapshot_every=self._config.snapshot_interval,
            output_edges=self._output_edges(),
        )

    def _output_edges(self) -> dict[str, EdgeSpec]:
        edges = {
            LINE_BATCH_EDGE: EdgeSpec(self._config.id, self._config.output_shard_count)
        }
        if self._config.accounts_output_queue is not None:
            edges[ACCOUNTS_EDGE] = EdgeSpec(self._config.id, 1)
        return edges

    def _build_publishers(self) -> dict[str, object]:
        publishers: dict[str, object] = {
            LINE_BATCH_EDGE: ShardedPublisher(
                self._config.mom_host,
                self._config.output_exchange,
                self._config.output_routing_prefix,
                self._config.output_shard_count,
            ),
        }
        if self._config.accounts_output_queue is not None:
            publishers[ACCOUNTS_EDGE] = _AccountsQueuePublisher(
                self._config.mom_host, self._config.accounts_output_queue
            )
        return publishers

    def _close_outputs(self) -> None:
        for edge, publisher in self._publishers.items():
            try:
                publisher.close()
            except Exception as e:
                logging.warning(
                    "file_splitter_close_output_error | id=%s | output=%s | error=%s",
                    self._config.id,
                    edge,
                    e,
                )
        self._publishers = {}

    def _on_message(self, message: bytes, ack, nack) -> None:
        try:
            if not message:
                raise ValueError("empty file splitter message")

            msg_type = message[0]
            payload = message[1:]

            if msg_type == MSG_CHUNK:
                self._dispatch_chunk(FileChunk.deserialize(payload), ack)
            elif msg_type == MSG_EOF:
                eof = _deserialize_eof(payload)
                if isinstance(eof, FileEof):
                    self._dispatch_file_eof(eof, ack)
                else:
                    self._dispatch_client_eof(eof, ack)
            elif msg_type == MSG_ABORT:
                client_id = int.from_bytes(payload[:4], "big")
                self._dispatch_abort(client_id, ack)
            else:
                raise ValueError(f"unknown file splitter message type: {msg_type}")
        except Exception as e:
            logging.error(
                "file_splitter_message_error | id=%s | error=%s",
                self._config.id,
                e,
                exc_info=True,
            )
            nack(requeue=True)

    def _dispatch_chunk(self, chunk: FileChunk, ack) -> None:
        client_id = chunk.client_id()
        rel_path = chunk.path()
        key = FileKey(client_id=client_id, rel_path=rel_path)
        sender_id = _sender_id_for_path(rel_path)
        offset = chunk.offset()
        payload = chunk.payload()
        expected = self._state.expected_offset(key)

        if offset == expected:
            seq = self._state.next_ordinal(key)
        elif offset + len(payload) == expected:
            # Chunks arrive in order, so after a crash only the last applied chunk
            # can be uncommitted; re-drive it.
            seq = self._state.next_ordinal(key) - 1
        else:
            # offset + len < expected: already committed, nothing to redo.
            ack()
            return

        change = FileSplitterState.chunk_change(
            client_id, rel_path, chunk.file_type(), offset, payload
        )
        self._run(f"d:{sender_id}:{offset}", client_id, sender_id, seq, change, MsgKind.DATA)
        ack()

        self._chunks_received += 1
        if should_log_progress(self._chunks_received):
            logging.info(
                "file_splitter_chunk | id=%s | client_id=%s | path=%s | offset=%s | "
                "payload_bytes=%s | chunks_received=%s",
                self._config.id,
                client_id,
                rel_path,
                offset,
                len(payload),
                self._chunks_received,
            )

    def _dispatch_file_eof(self, eof: FileEof, ack) -> None:
        client_id = eof.client_id()
        rel_path = eof.path()
        sender_id = _sender_id_for_path(rel_path)
        change = FileSplitterState.file_eof_change(client_id, rel_path)
        self._run(
            f"e:{sender_id}", client_id, sender_id, 0, change, MsgKind.CTRL_UPSTREAM_EOF
        )
        ack()
        self._eofs_received += 1
        logging.info(
            "file_splitter_eof | id=%s | client_id=%s | path=%s | eofs_received=%s",
            self._config.id,
            client_id,
            rel_path,
            self._eofs_received,
        )

    def _dispatch_client_eof(self, client_id: int, ack) -> None:
        # Dead path now (the gateway emits per-file FileEof); kept so an aggregate
        # client EOF still finishes open files.
        change = FileSplitterState.client_eof_change(client_id)
        self._run(
            f"c:{client_id}",
            client_id,
            _CLIENT_EOF_SENDER,
            0,
            change,
            MsgKind.CTRL_UPSTREAM_EOF,
        )
        ack()
        self._eofs_received += 1

    def _dispatch_abort(self, client_id: int, ack) -> None:
        """Handle an ABORT from the gateway: drop per-client state and propagate
        MessageType.ABORT to every shard of every downstream edge.

        Uses sender_id=0 / seq=0 under MsgKind.ABORT so the inbox can deduplicate
        re-deliveries (e.g. from multiple file-splitter instances or RabbitMQ
        redelivery after a crash) without colliding with real DATA or EOF buckets.
        """
        change = FileSplitterState.abort_change(client_id)
        # Build one ABORT packet per downstream shard; the _business_fn wraps them
        # as plain (unaddressed) packets so the SenderSequencer stamps sender+seq.
        self._run(
            f"abort:{client_id}",
            client_id,
            _ABORT_SENDER,
            0,
            change,
            MsgKind.ABORT,
        )
        ack()
        logging.info("file_splitter_abort | id=%s | client_id=%s", self._config.id, client_id)

    def _run(
        self,
        msg_id: str,
        client_id: int,
        sender_id: int,
        seq: int,
        change: dict,
        kind: MsgKind,
    ) -> None:
        instruction = self._handler.handle(
            msg_id,
            client_id,
            sender_id,
            seq,
            b"",
            lambda _payload: self._business_fn(change),
            kind=kind,
        )
        if instruction.action is Action.PUBLISH_THEN_COMMIT:
            self._publish(instruction.outputs)
            self._handler.commit_done(*instruction.ctx)

    def _business_fn(self, change: dict):
        client_id = change["client_id"]
        client_bytes = client_id.to_bytes(16, byteorder="big")
        logical = []

        if change["type"] == "abort":
            # Fan-out ABORT to every shard of every edge so downstream workers
            # receive and process the tombstone regardless of which shard holds
            # state for this client.
            abort_body = self._internal_protocol.create_packet(
                msg_type=MessageType.ABORT, client_id_bytes=client_bytes, payload=b""
            )
            for edge in self._publishers:
                for shard in range(self._config.output_shard_count):
                    logical.append((edge, abort_body, shard))
            return change, logical

        for edge, msg_type, payload in self._state.simulate(change):
            body = self._internal_protocol.create_packet(
                msg_type=msg_type, client_id_bytes=client_bytes, payload=payload
            )
            logical.append((edge, body))
        return change, logical

    def _publish(self, entries) -> None:
        for entry in entries:
            publisher = self._publishers.get(entry.destination)
            if publisher is None:
                raise KeyError(f"no publisher for destination {entry.destination!r}")
            if entry.shard is None:
                publisher.send(entry.body)
            else:
                publisher.send_to_shard(entry.body, entry.shard)

    def _ensure_line_batch_bindings(self) -> None:
        def queue_name(index: int) -> str:
            return queue_name_for_worker(self._config.output_routing_prefix, index)

        bindings = {
            queue_name(index): queue_name(index)
            for index in range(self._config.output_shard_count)
        }
        ensure_exchange_queue_bindings(
            self._config.mom_host,
            self._config.output_exchange,
            bindings,
        )

    def _input_routing_key(self) -> str:
        return self._config.input_routing_key or file_ingestor_routing_key(
            self._config.id
        )


class _AccountsQueuePublisher:
    """Single-queue publisher with the (send, send_to_shard) interface the outbox
    expects. The accounts edge has one shard, so send_to_shard ignores the index."""

    def __init__(self, mom_host: str, queue_name: str) -> None:
        self._queue = MessageMiddlewareQueueRabbitMQ(mom_host, queue_name)

    def send(self, message: bytes) -> None:
        self._queue.send(message)

    def send_to_shard(self, message: bytes, shard: int) -> None:
        self._queue.send(message)

    def close(self) -> None:
        self._queue.close()


def _sender_id_for_path(rel_path: str) -> int:
    # Stable per-file sender bucket so a redelivered chunk hits the same dedup
    # tracker. crc32, not hash(), so it's deterministic across restarts.
    return zlib.crc32(rel_path.encode("utf-8")) & 0xFFFFFFFF


def _deserialize_eof(payload: bytes) -> int | FileEof:
    if len(payload) == 4:
        return int.from_bytes(payload, byteorder="big")
    return FileEof.deserialize(payload)
