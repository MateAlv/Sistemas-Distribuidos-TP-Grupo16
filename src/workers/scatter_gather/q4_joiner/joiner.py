import logging
import os
import threading
from collections import Counter, defaultdict

from common.upstream_eof_counter import UpstreamEofCounter

from common.batch_buffer import BatchBuffer
from common.logging_utils import should_log_progress
from common.message_protocol.internal import (
    InternalProtocol,
    Q4BlockJoinEdgeSerializer,
    Q4PairPaths,
    Q4PairPathsSerializer,
    Q4_EDGE_INCOMING,
    Q4_EDGE_OUTGOING,
    Q4_QUALIFY_THRESHOLD,
    Q4AccountId,
    partition_for_parts,
)
from common.message_protocol.internal.common import ControlMessage, MessageType
from common.message_protocol.internal.control_message_serializer import (
    ControlMessageSerializer,
)
from common.middleware.middleware_rabbitmq import (
    ensure_exchange_queue_bindings,
    MessageMiddlewareExchangeRabbitMQ,
)
from common.routing import queue_name_for_worker


ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
Q4_JOINER_EXCHANGE = os.environ["Q4_JOINER_EXCHANGE"]
Q4_JOINER_ROUTING_PREFIX = os.environ.get(
    "Q4_JOINER_ROUTING_PREFIX", "q4_joiner"
)
Q4_SUM_AMOUNT = int(os.environ["Q4_SUM_AMOUNT"])
Q4_AGGREGATOR_EXCHANGE = os.environ["Q4_AGGREGATOR_EXCHANGE"]
Q4_AGGREGATOR_AMOUNT = int(os.environ["Q4_AGGREGATOR_AMOUNT"])
Q4_AGGREGATOR_ROUTING_PREFIX = os.environ.get(
    "Q4_AGGREGATOR_ROUTING_PREFIX", "q4_aggregator"
)
Q4_JOINER_BATCH_BYTES = int(
    os.environ.get("Q4_JOINER_BATCH_BYTES", str(1024 * 1024))
)
Q4_JOINER_BATCH_MAX_DELTAS = int(
    os.environ.get("Q4_JOINER_BATCH_MAX_DELTAS", "5000")
)


class Q4JoinerWorker:
    """Computes weighted A->M->B contributions for one salted block shard."""

    def __init__(self):
        self._input = MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST,
            Q4_JOINER_EXCHANGE,
            [self._input_routing_key()],
            queue_name=self._input_routing_key(),
            exclusive=False,
        )
        self._pair_reducer_output = self._new_pair_reducer_output()

        self._proto = InternalProtocol()
        self._block_edge_serializer = Q4BlockJoinEdgeSerializer()
        self._pair_paths_serializer = Q4PairPathsSerializer()
        self._control_serializer = ControlMessageSerializer()
        self._batcher = BatchBuffer(
            Q4_JOINER_BATCH_BYTES,
            Q4_JOINER_BATCH_MAX_DELTAS,
        )

        self._lock = threading.Lock()
        self._incoming_by_client = defaultdict(lambda: defaultdict(Counter))
        self._outgoing_by_client = defaultdict(lambda: defaultdict(Counter))
        self._processed_by_client: dict[int, int] = {}
        self._eof_counter = UpstreamEofCounter(Q4_SUM_AMOUNT)
        self._forwarded_by_partition_by_client: dict[int, dict[int, int]] = {}
        self._closed_by_client: set[int] = set()

        self._closed = False
        self._stopped = False
        self._blocks_emitted = 0

    def _input_routing_key(self) -> str:
        return queue_name_for_worker(Q4_JOINER_ROUTING_PREFIX, ID)

    def _new_pair_reducer_output(self):
        return MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST,
            Q4_AGGREGATOR_EXCHANGE,
            [],
        )

    def _ensure_output_bindings(self) -> None:
        ensure_exchange_queue_bindings(
            MOM_HOST,
            Q4_AGGREGATOR_EXCHANGE,
            {
                queue_name_for_worker(Q4_AGGREGATOR_ROUTING_PREFIX, index): (
                    queue_name_for_worker(Q4_AGGREGATOR_ROUTING_PREFIX, index)
                )
                for index in range(Q4_AGGREGATOR_AMOUNT)
            },
        )

    def _packet(self, msg_type: MessageType, client_id: int, payload: bytes) -> bytes:
        return self._proto.create_packet(
            msg_type=msg_type,
            client_id_bytes=client_id.to_bytes(16, byteorder="big"),
            payload=payload,
        )

    def _control_payload(
        self,
        sender_id: int,
        expected_total: int,
        processed_count: int,
    ) -> bytes:
        return self._control_serializer.serialize(
            ControlMessage(
                sender_id=sender_id,
                expected_total=expected_total,
                processed_count=processed_count,
            )
        )

    def _account_parts(self, account: Q4AccountId):
        return (account.bank_id, account.account)

    def _block_key(self, edge):
        return (
            edge.intermediate,
            edge.a_bucket,
            edge.b_bucket,
        )

    def _pair_partition(self, source: Q4AccountId, target: Q4AccountId) -> int:
        return partition_for_parts(
            (*self._account_parts(source), *self._account_parts(target)),
            Q4_AGGREGATOR_AMOUNT,
        )

    def _pair_routing_key(self, partition: int) -> str:
        return f"{Q4_AGGREGATOR_ROUTING_PREFIX}_{partition}"

    def _send_batch(
        self,
        client_id: int,
        partition: int,
        batch_payload: bytes,
        output=None,
    ) -> None:
        output = output or self._pair_reducer_output
        output.send(
            self._packet(MessageType.DATA, client_id, batch_payload),
            routing_key=self._pair_routing_key(partition),
        )

    def _append_pair_paths(
        self,
        client_id: int,
        pair_paths: Q4PairPaths,
        output=None,
    ) -> None:
        partition = self._pair_partition(pair_paths.source, pair_paths.target)
        payload = self._pair_paths_serializer.serialize(pair_paths)
        batch_payload = self._batcher.append((client_id, partition), payload)
        counts = self._forwarded_by_partition_by_client.setdefault(client_id, {})
        counts[partition] = counts.get(partition, 0) + 1
        if batch_payload is not None:
            self._send_batch(client_id, partition, batch_payload, output)

    def _flush_client_buffers(self, client_id: int, output=None) -> None:
        for (_, partition), batch_payload in self._batcher.flush(
            lambda k: k[0] == client_id
        ):
            self._send_batch(client_id, partition, batch_payload, output)

    def _accept_block_edges(self, client_id: int, payload: bytes) -> int:
        """Main data path. For each half-edge, file it into its block's incoming
        or outgoing dict by role. Nothing is paired yet — only sorting and counting."""
        edges = self._block_edge_serializer.deserialize_batch(payload)
        if not edges:
            return 0

        with self._lock:
            if client_id in self._closed_by_client:
                logging.info(
                    "q4_joiner_message_for_closed_client | id=%s | "
                    "client_id=%s",
                    ID,
                    client_id,
                )
                return 0

            for edge in edges:
                block = self._block_key(edge)
                if edge.role == Q4_EDGE_INCOMING:
                    self._incoming_by_client[client_id][block][edge.endpoint] += (
                        edge.count
                    )
                elif edge.role == Q4_EDGE_OUTGOING:
                    self._outgoing_by_client[client_id][block][edge.endpoint] += (
                        edge.count
                    )
                else:
                    raise ValueError(f"unexpected Q4 block edge role: {edge.role}")

            self._processed_by_client[client_id] = (
                self._processed_by_client.get(client_id, 0) + len(edges)
            )
            processed_total = self._processed_by_client[client_id]

        if should_log_progress(processed_total):
            logging.info(
                "q4_joiner_data_batch | id=%s | client_id=%s | "
                "batch_size=%s | processed_total=%s",
                ID,
                client_id,
                len(edges),
                processed_total,
            )
        return len(edges)

    def _handle_eof(self, client_id: int, payload: bytes) -> None:
        control = self._control_serializer.deserialize(payload)

        with self._lock:
            should_emit = self._eof_counter.on_eof(client_id, control.sender_id)
            eof_count = self._eof_counter.count(client_id)
            processed_total = self._processed_by_client.get(client_id, 0)

        logging.info(
            "q4_joiner_eof_received | id=%s | client_id=%s | "
            "edge_store_id=%s | eof_count=%s | expected_eofs=%s | "
            "sender_expected_total=%s | processed_total=%s",
            ID, client_id, control.sender_id, eof_count, Q4_SUM_AMOUNT,
            control.expected_total, processed_total,
        )

        if should_emit:
            self._emit_and_close_client(client_id)

    def _emit_client_pairs(self, client_id: int, output=None) -> None:
        """The actual A->M->B join. For every block that has both incoming and
        outgoing records, do the cartesian product, multiply the counts to get
        the block's path_count for (A, B), skip A == B, cap at the threshold,
        and emit one Q4PairPaths per pair, routed by hash(A, B) to an aggregator."""
        incoming = self._incoming_by_client.get(client_id, {})
        outgoing = self._outgoing_by_client.get(client_id, {})
        blocks = set(incoming) & set(outgoing)

        for block in blocks:
            incoming_counts = incoming[block]
            outgoing_counts = outgoing[block]
            if not incoming_counts or not outgoing_counts:
                continue

            emitted_for_block = 0
            for source, incoming_count in incoming_counts.items():
                for target, outgoing_count in outgoing_counts.items():
                    if source == target:
                        continue
                    path_count = incoming_count * outgoing_count
                    if path_count <= 0:
                        continue
                    self._append_pair_paths(
                        client_id,
                        Q4PairPaths(
                            source=source,
                            target=target,
                            path_count=min(path_count, Q4_QUALIFY_THRESHOLD),
                        ),
                        output,
                    )
                    emitted_for_block += 1

            self._blocks_emitted += 1
            if should_log_progress(self._blocks_emitted):
                intermediate, a_bucket, b_bucket = block
                logging.info(
                    "q4_joiner_emit_block | id=%s | client_id=%s | "
                    "intermediate_bank=%s | intermediate_account=%s | "
                    "a_bucket=%s | b_bucket=%s | incoming_endpoints=%s | "
                    "outgoing_endpoints=%s | pair_pathss=%s",
                    ID,
                    client_id,
                    intermediate.bank_id,
                    intermediate.account,
                    a_bucket,
                    b_bucket,
                    len(incoming_counts),
                    len(outgoing_counts),
                    emitted_for_block,
                )

    def _forward_eof_to_aggregators(
        self,
        client_id: int,
        counts_by_partition: dict[int, int],
        output=None,
    ) -> None:
        """Send one EOF to every aggregator partition, each carrying how many
        Q4PairPaths we sent that aggregator so it knows when we're done."""
        output = output or self._pair_reducer_output
        for partition in range(Q4_AGGREGATOR_AMOUNT):
            expected_total = int(counts_by_partition.get(partition, 0))
            output.send(
                self._packet(
                    MessageType.EOF,
                    client_id,
                    self._control_payload(
                        sender_id=ID,
                        expected_total=expected_total,
                        processed_count=0,
                    ),
                ),
                routing_key=self._pair_routing_key(partition),
            )
            logging.info(
                "q4_joiner_forward_eof | id=%s | client_id=%s | "
                "pair_reducer_partition=%s | expected_total=%s",
                ID,
                client_id,
                partition,
                expected_total,
            )

    def _emit_and_close_client(self, client_id: int, output=None) -> None:
        """End-of-client. Run the per-block joins, flush batched buffers, drop
        per-client state, then forward EOF to every aggregator partition."""
        with self._lock:
            if client_id in self._closed_by_client:
                return

        self._emit_client_pairs(client_id, output)
        self._flush_client_buffers(client_id, output)

        with self._lock:
            counts_by_partition = dict(
                self._forwarded_by_partition_by_client.get(client_id, {})
            )
            self._incoming_by_client.pop(client_id, None)
            self._outgoing_by_client.pop(client_id, None)
            self._processed_by_client.pop(client_id, None)
            self._eof_counter.close(client_id)
            self._forwarded_by_partition_by_client.pop(client_id, None)
            self._closed_by_client.add(client_id)

        self._forward_eof_to_aggregators(client_id, counts_by_partition, output)

    def _on_message(self, raw, ack, nack):
        try:
            msg_type, client_id, payload = self._proto.unpack_packet(raw)
            if msg_type == MessageType.DATA:
                self._accept_block_edges(client_id, payload)
            elif msg_type == MessageType.EOF:
                self._handle_eof(client_id, payload)
            else:
                raise ValueError(f"unexpected q4 block joiner message type: {msg_type}")
            ack()
        except Exception:
            logging.exception("q4_joiner_error | id=%s", ID)
            nack()

    def start(self) -> None:
        self._ensure_output_bindings()
        logging.info(
            "q4_joiner_start | id=%s | input_exchange=%s | input_key=%s | "
            "sum_amount=%s | pair_reducer_exchange=%s | "
            "aggregator_amount=%s",
            ID,
            Q4_JOINER_EXCHANGE,
            self._input_routing_key(),
            Q4_SUM_AMOUNT,
            Q4_AGGREGATOR_EXCHANGE,
            Q4_AGGREGATOR_AMOUNT,
        )
        try:
            if not self._stopped:
                self._input.start_consuming(self._on_message)
        finally:
            self.handle_sigterm()
            self.close()

    def handle_sigterm(self) -> None:
        if self._stopped:
            return
        self._stopped = True
        logging.info("q4_joiner_shutdown | id=%s", ID)
        self._input.request_stop_consuming()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        for resource in (self._input, self._pair_reducer_output):
            try:
                resource.close()
            except Exception as e:
                logging.warning(
                    "q4_joiner_close_error | id=%s | error=%s",
                    ID,
                    e,
                )
