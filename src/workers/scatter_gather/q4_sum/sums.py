import logging
import os
import threading
from collections import defaultdict

from common.fault_tolerance.handler import EdgeSpec, PersistentStateHandler, WorkerRunner
from common.fault_tolerance.inbox import InboxStatus
from common.logging_utils import should_log_progress
from common.message_protocol.internal import (
    InternalProtocol,
    Q4BlockJoinEdge,
    Q4BlockJoinEdgeSerializer,
    Q4_EDGE_INCOMING,
    Q4_EDGE_OUTGOING,
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
from workers.scatter_gather.q4_sum.q4_sum_state import Q4SumState


ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
Q4_SUM_EXCHANGE = os.environ["Q4_SUM_EXCHANGE"]
Q4_SUM_ROUTING_PREFIX = os.environ.get("Q4_SUM_ROUTING_PREFIX", "q4_sum")
Q4_FILTER_AMOUNT = int(os.environ.get("Q4_FILTER_AMOUNT", "1"))
Q4_JOINER_EXCHANGE = os.environ["Q4_JOINER_EXCHANGE"]
Q4_JOINER_AMOUNT = int(os.environ["Q4_JOINER_AMOUNT"])
Q4_JOINER_ROUTING_PREFIX = os.environ.get("Q4_JOINER_ROUTING_PREFIX", "q4_joiner")
Q4_SUM_BATCH_MAX_EDGES = int(os.environ.get("Q4_SUM_BATCH_MAX_EDGES", "5000"))
STATE_DIR = os.environ.get("STATE_DIR", "")
SNAPSHOT_INTERVAL = int(os.environ.get("SNAPSHOT_INTERVAL", "1000"))

# Logical name of the downstream edge in the publisher registry / outbox.
Q4_JOINER_EDGE = "q4_joiner"

# Hot-M planning constants. An intermediary whose incoming×outgoing fan-out
# exceeds the threshold is split across HOT_A_BUCKETS × HOT_B_BUCKETS join
# blocks so a hub never lands on a single joiner.
Q4_SUM_HOT_PAIR_THRESHOLD = 1_000_000
Q4_SUM_HOT_A_BUCKETS = 16
Q4_SUM_HOT_B_BUCKETS = 16


class Q4SumWorker:
    """Aggregates counted Q4 edges for the intermediary shard owned by this worker.

    Durable via PersistentStateHandler: DATA batches only accumulate per-client
    counts (no output); the join blocks are planned and scattered to the joiners
    once every upstream Q4Filter shard has sent its EOF (fan-in). A crash at any
    point recovers the accumulated counts and re-publishes any pending flush
    output from the outbox, so the emit-on-EOF happens exactly once.
    """

    def __init__(self):
        self._proto = InternalProtocol()
        self._block_edge_serializer = Q4BlockJoinEdgeSerializer()
        self._control_serializer = ControlMessageSerializer()

        self._lock = threading.Lock()
        self._state = Q4SumState(Q4_FILTER_AMOUNT)
        self._handler = PersistentStateHandler(
            state_dir=STATE_DIR,
            node_id=f"q4_sum_{ID}",
            worker_state=self._state,
            snapshot_every=SNAPSHOT_INTERVAL,
            output_edges={
                Q4_JOINER_EDGE: EdgeSpec(sender_id=ID, shard_count=Q4_JOINER_AMOUNT)
            },
        )

        self._input = MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST,
            Q4_SUM_EXCHANGE,
            [self._input_routing_key()],
            queue_name=self._input_routing_key(),
            exclusive=False,
        )
        self._joiner_output = self._new_joiner_output()
        self._publishers = {Q4_JOINER_EDGE: self._joiner_output}
        self._runner = WorkerRunner(
            handler=self._handler,
            publishers=self._publishers,
            process_payload=self._data_process_payload,
            lock=self._lock,
        )

        self._closed = False
        self._stopped = False
        self._intermediaries_planned = 0

    def _input_routing_key(self) -> str:
        return queue_name_for_worker(Q4_SUM_ROUTING_PREFIX, ID)

    def _new_joiner_output(self) -> ShardedPublisher:
        return ShardedPublisher(
            MOM_HOST,
            Q4_JOINER_EXCHANGE,
            Q4_JOINER_ROUTING_PREFIX,
            Q4_JOINER_AMOUNT,
        )

    def _ensure_output_bindings(self) -> None:
        ensure_exchange_queue_bindings(
            MOM_HOST,
            Q4_JOINER_EXCHANGE,
            {
                queue_name_for_worker(Q4_JOINER_ROUTING_PREFIX, index): (
                    queue_name_for_worker(Q4_JOINER_ROUTING_PREFIX, index)
                )
                for index in range(Q4_JOINER_AMOUNT)
            },
        )

    # ---------- packet helpers ----------

    def _packet(self, msg_type: MessageType, client_id: int, payload: bytes) -> bytes:
        return self._proto.create_packet(
            msg_type=msg_type,
            client_id_bytes=client_id.to_bytes(16, byteorder="big"),
            payload=payload,
        )

    def _control_payload(
        self, sender_id: int, expected_total: int, processed_count: int
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

    # ---------- block planning (pure) ----------

    def _account_parts(self, account: Q4AccountId):
        return (account.bank_id, account.account)

    def _account_bucket(self, account: Q4AccountId, bucket_count: int) -> int:
        return partition_for_parts(self._account_parts(account), bucket_count)

    def _block_partition(
        self, intermediate: Q4AccountId, a_bucket: int, b_bucket: int
    ) -> int:
        return partition_for_parts(
            (*self._account_parts(intermediate), a_bucket, b_bucket),
            Q4_JOINER_AMOUNT,
        )

    def _block_plan(self, incoming_size: int, outgoing_size: int) -> tuple[int, int]:
        """Small fan-out (incoming x outgoing) → one block (1, 1). Above the
        hot-pair threshold → a HOT_A_BUCKETS x HOT_B_BUCKETS grid."""
        if incoming_size * outgoing_size <= Q4_SUM_HOT_PAIR_THRESHOLD:
            return 1, 1
        return max(1, Q4_SUM_HOT_A_BUCKETS), max(1, Q4_SUM_HOT_B_BUCKETS)

    def _build_flush_outputs(self, client_id: int) -> list:
        """Pure: read the accumulated counts and return the logical outputs for
        this client's end — block-join batches per joiner partition plus one EOF
        per joiner partition carrying that partition's record count. No mutation
        and no I/O, so handle()/WAL replay reproduces it exactly."""
        incoming = self._state.incoming_for(client_id)
        outgoing = self._state.outgoing_for(client_id)
        intermediaries = set(incoming) & set(outgoing)

        edges_by_partition: dict[int, list] = defaultdict(list)
        for intermediate in intermediaries:
            incoming_counts = incoming[intermediate]
            outgoing_counts = outgoing[intermediate]
            if not incoming_counts or not outgoing_counts:
                continue
            a_buckets, b_buckets = self._block_plan(
                len(incoming_counts), len(outgoing_counts)
            )
            self._intermediaries_planned += 1
            if should_log_progress(self._intermediaries_planned):
                logging.info(
                    "q4_sum_plan_intermediate | id=%s | client_id=%s | "
                    "intermediate_bank=%s | intermediate_account=%s | "
                    "incoming_endpoints=%s | outgoing_endpoints=%s | "
                    "a_buckets=%s | b_buckets=%s",
                    ID, client_id, intermediate.bank_id, intermediate.account,
                    len(incoming_counts), len(outgoing_counts), a_buckets, b_buckets,
                )
            for endpoint, count in incoming_counts.items():
                a_bucket = self._account_bucket(endpoint, a_buckets)
                for b_bucket in range(b_buckets):
                    block_edge = Q4BlockJoinEdge(
                        role=Q4_EDGE_INCOMING,
                        intermediate=intermediate,
                        endpoint=endpoint,
                        a_bucket=a_bucket,
                        b_bucket=b_bucket,
                        count=count,
                    )
                    partition = self._block_partition(intermediate, a_bucket, b_bucket)
                    edges_by_partition[partition].append(block_edge)
            for endpoint, count in outgoing_counts.items():
                b_bucket = self._account_bucket(endpoint, b_buckets)
                for a_bucket in range(a_buckets):
                    block_edge = Q4BlockJoinEdge(
                        role=Q4_EDGE_OUTGOING,
                        intermediate=intermediate,
                        endpoint=endpoint,
                        a_bucket=a_bucket,
                        b_bucket=b_bucket,
                        count=count,
                    )
                    partition = self._block_partition(intermediate, a_bucket, b_bucket)
                    edges_by_partition[partition].append(block_edge)

        outputs = []
        for partition, block_edges in edges_by_partition.items():
            for chunk in _chunks(block_edges, Q4_SUM_BATCH_MAX_EDGES):
                payload = self._block_edge_serializer.serialize_batch(chunk)
                outputs.append(
                    (Q4_JOINER_EDGE, self._packet(MessageType.DATA, client_id, payload), partition)
                )
        for partition in range(Q4_JOINER_AMOUNT):
            expected_total = len(edges_by_partition.get(partition, ()))
            eof_payload = self._control_payload(ID, expected_total, 0)
            outputs.append(
                (Q4_JOINER_EDGE, self._packet(MessageType.EOF, client_id, eof_payload), partition)
            )
        return outputs

    # ---------- data path (durable) ----------

    def _data_process_payload(self, client_id: int, payload: bytes):
        """business_fn for a DATA batch: accumulate the counted edges into state,
        emit nothing (the join is planned at EOF). A closed client is ignored by
        apply_change, so a late straggler is a no-op."""
        return Q4SumState.data_change(client_id, payload), []

    def _handle_eof(self, raw: bytes, ack, nack) -> None:
        """Upstream EOF from one Q4Filter shard, routed through the handler as a
        durable input. Fan-in: the decide step predicts whether this is the last
        shard; if so business_fn builds the whole flush (block batches + per-
        partition EOFs) into the outbox and bundles close. The UpstreamEofCounter
        is idempotent, so apply_change replays safely."""
        try:
            _msg_type, client_id, sender_id, seq, payload = (
                self._proto.unpack_addressed_packet(raw)
            )
            ctrl = self._control_serializer.deserialize(payload)
            upstream_id = ctrl.sender_id
            msg_id = f"{sender_id}:{seq}"

            with self._lock:
                status = self._handler.inbox.classify(client_id, sender_id, seq)
                if self._state.is_closed(client_id) and status is not InboxStatus.APPLIED:
                    ack()
                    return
                completes = self._state.eof_would_complete(client_id, upstream_id)
                eof_count = self._state.eof_count(client_id)

                def bfn(_pl):
                    eof_change = Q4SumState.eof_change(client_id, upstream_id)
                    if completes:
                        outputs = self._build_flush_outputs(client_id)
                        compound = Q4SumState.compound_change(
                            eof_change, Q4SumState.close_change(client_id)
                        )
                        return compound, outputs
                    return eof_change, []

                instruction = self._handler.handle(
                    msg_id, client_id, sender_id, seq, payload, bfn
                )

            self._publish(instruction.outputs)
            with self._lock:
                self._handler.commit_done(*instruction.ctx)
            ack()
            logging.info(
                "q4_sum_eof_received | id=%s | client_id=%s | upstream_id=%s | "
                "eof_count=%s | expected_eofs=%s | flushed=%s",
                ID, client_id, upstream_id, eof_count + 1, Q4_FILTER_AMOUNT, completes,
            )
        except Exception:
            logging.exception("q4_sum_eof_error | id=%s", ID)
            nack(requeue=True)

    def _on_message(self, raw, ack, nack) -> None:
        if not raw:
            logging.error("q4_sum_empty_message | id=%s", ID)
            nack(requeue=False)
            return
        msg_type = raw[0]
        if msg_type == MessageType.DATA:
            self._runner.process(raw, ack, nack)
        elif msg_type == MessageType.EOF:
            self._handle_eof(raw, ack, nack)
        else:
            logging.error("q4_sum_unknown_message_type | id=%s | type=%s", ID, msg_type)
            nack(requeue=False)

    # ---------- lifecycle ----------

    def start(self) -> None:
        self._ensure_output_bindings()
        self._runner.recover_and_republish()
        logging.info(
            "q4_sum_start | id=%s | input_exchange=%s | input_key=%s | "
            "filter_amount=%s | joiner_exchange=%s | joiner_amount=%s",
            ID, Q4_SUM_EXCHANGE, self._input_routing_key(),
            Q4_FILTER_AMOUNT, Q4_JOINER_EXCHANGE, Q4_JOINER_AMOUNT,
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
        logging.info("q4_sum_shutdown | id=%s", ID)
        self._input.request_stop_consuming()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        for resource in (self._input, self._joiner_output):
            try:
                resource.close()
            except Exception as e:
                logging.warning("q4_sum_close_error | id=%s | error=%s", ID, e)


def _chunks(items: list, size: int):
    for i in range(0, len(items), size):
        yield items[i : i + size]
