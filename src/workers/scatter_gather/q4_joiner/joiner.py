import logging
import os
import threading
from collections import defaultdict

from common.fault_tolerance.handler import Action, PersistentStateHandler
from common.fault_tolerance.inbox import InboxStatus
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
from common.middleware import ShardedPublisher
from common.middleware.middleware_rabbitmq import (
    ensure_exchange_queue_bindings,
    MessageMiddlewareExchangeRabbitMQ,
)
from common.routing import queue_name_for_worker

try:
    from q4_joiner_state import Q4JoinerState
except ImportError:
    from workers.scatter_gather.q4_joiner.q4_joiner_state import Q4JoinerState


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
Q4_JOINER_BATCH_MAX_DELTAS = int(
    os.environ.get("Q4_JOINER_BATCH_MAX_DELTAS", "5000")
)
STATE_DIR = os.environ.get("STATE_DIR", "")
SNAPSHOT_INTERVAL = int(os.environ.get("SNAPSHOT_INTERVAL", "1000"))
Q4_AGGREGATOR_EDGE = "q4_aggregator"


class Q4JoinerWorker:
    """Computes weighted A->M->B contributions for one salted block shard."""

    def __init__(self):
        self._proto = InternalProtocol()
        self._pair_paths_serializer = Q4PairPathsSerializer()
        self._control_serializer = ControlMessageSerializer()

        self._lock = threading.Lock()
        self._state = Q4JoinerState(Q4_SUM_AMOUNT)
        self._handler = PersistentStateHandler(
            state_dir=STATE_DIR,
            node_id=f"q4_joiner_{ID}",
            worker_state=self._state,
            snapshot_every=SNAPSHOT_INTERVAL,
        )

        self._input = MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST,
            Q4_JOINER_EXCHANGE,
            [self._input_routing_key()],
            queue_name=self._input_routing_key(),
            exclusive=False,
        )
        self._pair_reducer_output = self._new_pair_reducer_output()
        self._publishers = {Q4_AGGREGATOR_EDGE: self._pair_reducer_output}

        self._closed = False
        self._stopped = False
        self._blocks_emitted = 0

        self._handler.recover()
        self._republish_pending()

    def _input_routing_key(self) -> str:
        return queue_name_for_worker(Q4_JOINER_ROUTING_PREFIX, ID)

    def _new_pair_reducer_output(self) -> ShardedPublisher:
        return ShardedPublisher(
            MOM_HOST,
            Q4_AGGREGATOR_EXCHANGE,
            Q4_AGGREGATOR_ROUTING_PREFIX,
            Q4_AGGREGATOR_AMOUNT,
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

    def _publish(self, entries) -> None:
        for entry in entries:
            publisher = self._publishers.get(entry.destination)
            if publisher is None:
                raise KeyError(f"no publisher for destination {entry.destination!r}")
            if entry.shard is None:
                publisher.send(entry.body)
            else:
                publisher.send_to_shard(entry.body, entry.shard)

    def _publish_then_commit(self, instruction) -> None:
        if instruction.action is Action.PUBLISH_THEN_COMMIT:
            self._publish(instruction.outputs)
            with self._lock:
                self._handler.commit_done(*instruction.ctx)

    def _republish_pending(self) -> None:
        for entry in self._handler.outbox_to_republish():
            try:
                self._publish([entry])
            except Exception:
                logging.exception(
                    "q4_joiner_republish_error | id=%s | destination=%s",
                    ID,
                    entry.destination,
                )

    # ---------- block joining (pure) ----------

    def _account_parts(self, account: Q4AccountId):
        return (account.bank_id, account.account)

    def _pair_partition(self, source: Q4AccountId, target: Q4AccountId) -> int:
        return partition_for_parts(
            (*self._account_parts(source), *self._account_parts(target)),
            Q4_AGGREGATOR_AMOUNT,
        )

    def _build_flush_outputs(self, client_id: int) -> list:
        incoming = self._state.incoming_for(client_id)
        outgoing = self._state.outgoing_for(client_id)
        blocks = set(incoming) & set(outgoing)

        pairs_by_partition: dict[int, list] = defaultdict(list)
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
                    pair_paths = Q4PairPaths(
                        source=source,
                        target=target,
                        path_count=min(path_count, Q4_QUALIFY_THRESHOLD),
                    )
                    pairs_by_partition[
                        self._pair_partition(pair_paths.source, pair_paths.target)
                    ].append(pair_paths)
                    emitted_for_block += 1

            self._blocks_emitted += 1
            if should_log_progress(self._blocks_emitted):
                intermediate, a_bucket, b_bucket = block
                logging.info(
                    "q4_joiner_emit_block | id=%s | client_id=%s | "
                    "intermediate_bank=%s | intermediate_account=%s | "
                    "a_bucket=%s | b_bucket=%s | incoming_endpoints=%s | "
                    "outgoing_endpoints=%s | pair_paths=%s",
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

        outputs = []
        for partition in sorted(pairs_by_partition):
            for chunk in _chunks(pairs_by_partition[partition], Q4_JOINER_BATCH_MAX_DELTAS):
                payload = self._pair_paths_serializer.serialize_batch(chunk)
                outputs.append(
                    (
                        Q4_AGGREGATOR_EDGE,
                        self._packet(MessageType.DATA, client_id, payload),
                        partition,
                    )
                )

        for partition in range(Q4_AGGREGATOR_AMOUNT):
            expected_total = len(pairs_by_partition.get(partition, ()))
            outputs.append(
                (
                    Q4_AGGREGATOR_EDGE,
                    self._packet(
                        MessageType.EOF,
                        client_id,
                        self._control_payload(ID, expected_total, 0),
                    ),
                    partition,
                )
            )
            logging.info(
                "q4_joiner_forward_eof | id=%s | client_id=%s | "
                "pair_reducer_partition=%s | expected_total=%s",
                ID,
                client_id,
                partition,
                expected_total,
            )
        return outputs

    # ---------- data path ----------

    def _handle_data(self, msg_id, client_id, sender_id, seq, payload, ack) -> None:
        def bfn(data):
            return Q4JoinerState.data_change(client_id, data), []

        with self._lock:
            instruction = self._handler.handle(
                msg_id, client_id, sender_id, seq, payload, bfn
            )
        self._publish_then_commit(instruction)
        ack()

        processed = self._state.processed_count(client_id)
        if should_log_progress(processed):
            logging.info(
                "q4_joiner_data_batch | id=%s | client_id=%s | processed_total=%s",
                ID,
                client_id,
                processed,
            )

    def _handle_eof(self, msg_id, client_id, sender_id, seq, payload, ack) -> None:
        control = self._control_serializer.deserialize(payload)
        upstream_id = control.sender_id

        with self._lock:
            status = self._handler.inbox.classify(client_id, sender_id, seq)
            if self._state.is_closed(client_id) and status is not InboxStatus.APPLIED:
                ack()
                return
            completes = self._state.eof_would_complete(client_id, upstream_id)
            eof_count = self._state.eof_count(client_id)
            processed_total = self._state.processed_count(client_id)

            def bfn(_data):
                eof_change = Q4JoinerState.eof_change(client_id, upstream_id)
                if completes:
                    return (
                        Q4JoinerState.compound_change(
                            eof_change, Q4JoinerState.close_change(client_id)
                        ),
                        self._build_flush_outputs(client_id),
                    )
                return eof_change, []

            instruction = self._handler.handle(
                msg_id, client_id, sender_id, seq, payload, bfn
            )

        self._publish_then_commit(instruction)
        ack()
        logging.info(
            "q4_joiner_eof_received | id=%s | client_id=%s | "
            "edge_store_id=%s | eof_count=%s | expected_eofs=%s | "
            "sender_expected_total=%s | processed_total=%s | flushed=%s",
            ID,
            client_id,
            upstream_id,
            eof_count + 1,
            Q4_SUM_AMOUNT,
            control.expected_total,
            processed_total,
            completes,
        )

    def _on_message(self, raw, ack, nack):
        try:
            msg_type, client_id, sender_id, seq, payload = (
                self._proto.unpack_addressed_packet(raw)
            )
            msg_id = f"{sender_id}:{seq}"
            if msg_type == MessageType.DATA:
                self._handle_data(msg_id, client_id, sender_id, seq, payload, ack)
            elif msg_type == MessageType.EOF:
                self._handle_eof(msg_id, client_id, sender_id, seq, payload, ack)
            else:
                raise ValueError(f"unexpected q4 block joiner message type: {msg_type}")
        except Exception:
            logging.exception("q4_joiner_error | id=%s", ID)
            nack(requeue=True)

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


def _chunks(items: list, size: int):
    for i in range(0, len(items), size):
        yield items[i : i + size]
