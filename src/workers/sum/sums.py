import logging
import os
import threading
import time

from common import middleware
from common.constants import C_Q2, C_Q3
from common.eof_coordinator import EofCoordinator, BroadcastAction, FlushAction, SendAnswerAction
from common.fault_tolerance.handler.action import Action
from common.fault_tolerance.handler.persistent_state_handler import PersistentStateHandler
from common.fault_tolerance.inbox import MsgKind
from common.logging_utils import should_log_progress
from common.message_protocol.internal.common import ControlMessage, MessageType
from common.message_protocol.internal.control_message_serializer import ControlMessageSerializer
from common.message_protocol.internal import InternalProtocol
from common.middleware import LazyQueue, MessageMiddlewareQueueRabbitMQ
from common.routing import queue_name_for_worker, shard_for_key

try:
    from sum_state import SumState
    from processors import create_sum_processor
except ImportError:
    from workers.sum.sum_state import SumState
    from workers.sum.processors import create_sum_processor


ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
CONFIGURATION = os.getenv("CONFIGURATION", C_Q2)
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
if CONFIGURATION in (C_Q2, C_Q3):
    INPUT_EXCHANGE = os.environ["INPUT_EXCHANGE"]
    INPUT_ROUTING_PREFIX = os.environ["INPUT_ROUTING_PREFIX"]
else:
    INPUT_EXCHANGE = os.getenv("INPUT_EXCHANGE")
    INPUT_ROUTING_PREFIX = os.getenv("INPUT_ROUTING_PREFIX")
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_QUEUE_PREFIX = f"{SUM_PREFIX}_control"
SUM_RESPONSE_QUEUE_PREFIX = f"{SUM_PREFIX}_response"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
STATE_DIR = os.environ.get("STATE_DIR", "/tmp/sum_state")
SNAPSHOT_INTERVAL = int(os.environ.get("SNAPSHOT_INTERVAL", "1000"))


class SumWorker:
    """Reduce upstream transactions into per-(partition) partials and forward them
    to the aggregator shards. Durable via PersistentStateHandler: DATA accumulation
    and the EOF/flush protocol are WAL-tracked so a crash replays exactly.

    Coordinator mode is "broadcast": the worker that receives a client's upstream
    EOF becomes the dynamic leader, broadcasts EOF_RECEIVED, collects per-replica
    PROCESSED_ANSWERs, then FLUSH_ORDER / FLUSH_ACK. Each client is co-located on
    one shard (filter routes by client_id), so every queue this worker reads is
    per-replica and RabbitMQ redelivery always returns to the same replica, which
    is what makes the per-replica inbox dedup valid.
    """

    def __init__(self):
        if CONFIGURATION not in (C_Q2, C_Q3):
            raise ValueError(f"Invalid sum configuration: {CONFIGURATION}")

        self._coordinator = EofCoordinator(
            instance_id=ID,
            total_instances=SUM_AMOUNT,
            control_queue_prefix=SUM_CONTROL_QUEUE_PREFIX,
            response_queue_prefix=SUM_RESPONSE_QUEUE_PREFIX,
            mode="broadcast",
        )

        self._state = SumState(
            coordinator=self._coordinator,
            processor_factory=lambda: create_sum_processor(CONFIGURATION),
        )
        self._handler = PersistentStateHandler(
            state_dir=STATE_DIR,
            node_id=f"sum_{ID}",
            worker_state=self._state,
            snapshot_every=SNAPSHOT_INTERVAL,
        )

        self._internal_protocol = InternalProtocol()
        self._control_serializer = ControlMessageSerializer()

        self._lock = threading.Lock()

        # Thread-local lazy senders (pika connections are not thread-safe).
        self._tls = threading.local()
        # Aggregator routing keys identify exchange destinations vs. named queues.
        self._agg_routing_keys = {
            f"{AGGREGATION_PREFIX}_{i}" for i in range(AGGREGATION_AMOUNT)
        }
        self._partials_forwarded = 0
        # Monotonic seq per client for addressed packets sent to the aggregator.
        # In-memory: resets to 0 on restart. Safe because partials are emitted only
        # at flush time (recorded by close_change), so a restarted worker never
        # re-emits for an already-closed client, and the outbox preserves the exact
        # bytes (with their seqs) for any flush interrupted mid-way.
        self._agg_seq_by_client: dict[int, int] = {}

        self._stopped_lock = threading.Lock()
        self._input_queue: MessageMiddlewareQueueRabbitMQ | None = None
        self._control_consumer: MessageMiddlewareQueueRabbitMQ | None = None
        self._response_consumer: MessageMiddlewareQueueRabbitMQ | None = None
        self._control_thread: threading.Thread | None = None
        self._response_thread: threading.Thread | None = None
        self._closed = False
        self._stopped = False

        self._handler.recover()
        self._republish_pending()

    # ---------- connection helpers ----------

    def _tl_sender(self, destination: str):
        """Return the thread-local sender for a destination.

        Aggregator destinations are exchange routing keys (published to the
        AGGREGATION_PREFIX exchange); everything else is a named control/response
        queue served by a LazyQueue.
        """
        if not hasattr(self._tls, "senders"):
            self._tls.senders = {}
        sender = self._tls.senders.get(destination)
        if sender is None:
            if destination in self._agg_routing_keys:
                sender = middleware.MessageMiddlewareExchangeRabbitMQ(
                    MOM_HOST, AGGREGATION_PREFIX, [destination]
                )
            else:
                sender = LazyQueue(MOM_HOST, destination)
            self._tls.senders[destination] = sender
        return sender

    def _republish_pending(self) -> None:
        """Re-send outputs stamped but not committed before the last crash."""
        for entry in self._handler.outbox_to_republish():
            try:
                self._tl_sender(entry.destination).send(entry.body)
            except Exception:
                logging.exception(
                    "sum_republish_error | configuration=%s | id=%s | destination=%s",
                    CONFIGURATION, ID, entry.destination,
                )

    # ---------- packet helpers ----------

    def _aggregation_index(self, partition_key: str) -> int:
        return shard_for_key(partition_key, AGGREGATION_AMOUNT)

    def _input_routing_key(self) -> str:
        if INPUT_ROUTING_PREFIX is None:
            raise RuntimeError("INPUT_ROUTING_PREFIX is required for sharded input")
        return queue_name_for_worker(INPUT_ROUTING_PREFIX, ID)

    def _next_agg_seq(self, client_id: int) -> int:
        seq = self._agg_seq_by_client.get(client_id, 0)
        self._agg_seq_by_client[client_id] = (seq + 1) & 0xFFFFFFFF
        return seq

    def _addressed_packet(
        self, msg_type: MessageType, client_id: int, seq: int, payload: bytes
    ) -> bytes:
        return self._internal_protocol.create_addressed_packet(
            msg_type=msg_type,
            client_id_bytes=client_id.to_bytes(16, byteorder="big"),
            sender_id=ID,
            seq=seq,
            payload=payload,
        )

    def _eof_payload(self, expected_total: int) -> bytes:
        return self._control_serializer.serialize(
            ControlMessage(sender_id=ID, expected_total=expected_total, processed_count=0)
        )

    def _partials_outputs(self, client_id: int, partials) -> list:
        """[(agg_routing_key, addressed DATA bytes)] for each partial, sharded by
        partition key. seqs are assigned here (inside business_fn, NEW path only)."""
        outputs = []
        for partition_key, payload in partials:
            index = self._aggregation_index(partition_key)
            seq = self._next_agg_seq(client_id)
            routing_key = f"{AGGREGATION_PREFIX}_{index}"
            outputs.append(
                (routing_key, self._addressed_packet(MessageType.DATA, client_id, seq, payload))
            )
        return outputs

    def _eof_outputs(self, client_id: int, total_forwarded: int) -> list:
        """[(agg_routing_key, addressed EOF bytes)] for every aggregator shard.
        Each shard receives the global total_forwarded as its expected_total."""
        outputs = []
        payload = self._eof_payload(total_forwarded)
        for index in range(AGGREGATION_AMOUNT):
            seq = self._next_agg_seq(client_id)
            routing_key = f"{AGGREGATION_PREFIX}_{index}"
            outputs.append(
                (routing_key, self._addressed_packet(MessageType.EOF, client_id, seq, payload))
            )
        return outputs

    def _publish_commit_ack(self, instruction, ack) -> bool:
        """Publish the stamped outputs, commit the input, ack. Returns whether work
        was actually done.

        A duplicate or replayed message the inbox already finished comes back as
        Action.ACK with ctx=None — just ack it; never call commit_done(*None).
        This is the crash-recovery path: RabbitMQ redelivers an unacked message,
        the inbox classifies it DONE, and we ack without reprocessing.
        """
        if instruction.action is Action.ACK:
            ack()
            return False
        for entry in instruction.outputs:
            self._tl_sender(entry.destination).send(entry.body)
        with self._lock:
            self._handler.commit_done(*instruction.ctx)
        ack()
        return True

    # ---------- data path ----------

    def _process_message(self, message: bytes, ack, nack) -> None:
        try:
            msg_type, client_id, sender_id, seq, payload = (
                self._internal_protocol.unpack_addressed_packet(message)
            )

            if msg_type == MessageType.DATA:
                self._handle_data(client_id, sender_id, seq, payload, ack)
            elif msg_type == MessageType.EOF:
                self._handle_upstream_eof(client_id, sender_id, seq, payload, ack)
            elif msg_type == MessageType.ABORT:
                self._handle_abort(client_id, sender_id, seq, ack, nack)
            else:
                raise ValueError(f"Unexpected sum data message type: {msg_type}")
        except Exception:
            logging.exception("sum_data_error | configuration=%s | id=%s", CONFIGURATION, ID)
            nack(requeue=True)

    def _handle_data(self, client_id, sender_id, seq, payload, ack) -> None:
        msg_id = f"d:{sender_id}:{client_id}:{seq}"

        def bfn(_pl):
            return SumState.data_change(client_id, _pl), []

        # DATA has no outputs; _publish_commit_ack commits + acks (or just acks
        # a duplicate that the inbox already finished).
        with self._lock:
            instruction = self._handler.handle(msg_id, client_id, sender_id, seq, payload, bfn)
        committed = self._publish_commit_ack(instruction, ack)

        if committed:
            processed = self._state.processed_count(client_id)
            if should_log_progress(processed):
                logging.info(
                    "sum_data | configuration=%s | id=%s | client_id=%s | processed_total=%s",
                    CONFIGURATION, ID, client_id, processed,
                )

    def _handle_upstream_eof(self, client_id, sender_id, seq, payload, ack) -> None:
        ctrl = self._control_serializer.deserialize(payload)
        expected_total = ctrl.expected_total
        msg_id = f"d:{sender_id}:{client_id}:{seq}"

        with self._lock:
            if self._state.is_closed(client_id):
                ack()
                return
            count = self._state.processed_count(client_id)

            def bfn(_pl):
                # Broadcast mode: the dynamic leader sets _leader_expected here.
                # on_upstream_eof is idempotent (N=1 returns Flush without mutating;
                # N>1 dedups via _seen_eof / dict assignment), so the replay call
                # from apply_change is safe.
                action = self._coordinator.on_upstream_eof(
                    client_id, expected_total, count, count
                )
                eof_change = SumState.coordinator_upstream_eof_change(
                    client_id, expected_total, count, count
                )
                if isinstance(action, BroadcastAction):
                    # N>1: fan out EOF_RECEIVED to every replica's control queue.
                    return eof_change, [(qn, action.message) for qn in action.queue_names]
                if isinstance(action, FlushAction):
                    # N=1: no coordination — flush own partials directly.
                    partials = self._state.partials_for(client_id)
                    outputs = (
                        self._partials_outputs(client_id, partials)
                        + self._eof_outputs(client_id, len(partials))
                    )
                    compound = SumState.compound_change(
                        eof_change, SumState.close_change(client_id)
                    )
                    return compound, outputs
                # None: duplicate upstream EOF.
                return eof_change, []

            instruction = self._handler.handle(
                msg_id, client_id, sender_id, seq, payload, bfn
            )

        self._publish_commit_ack(instruction, ack)
        logging.info(
            "sum_upstream_eof | configuration=%s | id=%s | client_id=%s | "
            "local_count=%s | expected_total=%s",
            CONFIGURATION, ID, client_id, count, expected_total,
        )

    def _downstream_abort_outputs(self, client_id: int) -> list:
        outputs = []
        for index in range(AGGREGATION_AMOUNT):
            seq = self._next_agg_seq(client_id)
            routing_key = f"{AGGREGATION_PREFIX}_{index}"
            outputs.append(
                (routing_key, self._addressed_packet(MessageType.ABORT, client_id, seq, b""))
            )
        return outputs

    def _handle_abort(self, client_id: int, sender_id: int, seq: int, ack, nack) -> None:
        msg_id = f"abort:{sender_id}:{client_id}:{seq}"
        try:
            with self._lock:
                if self._state.is_closed(client_id):
                    ack()
                    return

                def bfn(_pl):
                    return (
                        SumState.abort_change(client_id),
                        self._downstream_abort_outputs(client_id),
                    )

                instruction = self._handler.handle(
                    msg_id, client_id, sender_id, seq, b"", bfn,
                    kind=MsgKind.ABORT,
                )
            self._publish_commit_ack(instruction, ack)
            logging.info(
                "sum_abort | configuration=%s | id=%s | client_id=%s",
                CONFIGURATION, ID, client_id,
            )
        except Exception:
            logging.exception(
                "sum_abort_error | configuration=%s | id=%s | client_id=%s",
                CONFIGURATION, ID, client_id,
            )
            nack(requeue=True)

    # ---------- control path ----------

    def _handle_control(self, message: bytes, ack, nack) -> None:
        try:
            msg_type, client_id, ctrl = self._coordinator.parse_message(message)
        except Exception:
            logging.exception(
                "sum_control_parse_error | configuration=%s | id=%s", CONFIGURATION, ID
            )
            nack(requeue=False)
            return

        if msg_type == MessageType.EOF_RECEIVED:
            self._handle_eof_received(message, client_id, ctrl, ack, nack)
        elif msg_type == MessageType.PROCESSED_REQUEST:
            self._handle_processed_request(client_id, ctrl, ack, nack)
        elif msg_type == MessageType.FLUSH_ORDER:
            self._handle_flush_order(client_id, ctrl, ack, nack)
        else:
            logging.warning(
                "sum_unexpected_control_type | configuration=%s | id=%s | msg_type=%s",
                CONFIGURATION, ID, msg_type,
            )
            ack()

    def _handle_eof_received(self, message, client_id, ctrl, ack, nack) -> None:
        # WAL-tracked so _pending_eof is durable: without it a crash would leave a
        # later PROCESSED_REQUEST with nothing to re-report (deadlock).
        # Dedup: one EOF_RECEIVED per (client, dynamic-leader) pair.
        sender_id = ctrl.sender_id
        seq = client_id
        msg_id = f"er:{client_id}:{ctrl.sender_id}"
        try:
            with self._lock:
                count = self._state.processed_count(client_id)

                def bfn(_pl):
                    action = self._coordinator.process_control_message(
                        MessageType.EOF_RECEIVED, client_id, ctrl, count, count
                    )
                    change = SumState.coordinator_msg_change(
                        MessageType.EOF_RECEIVED, client_id, ctrl.sender_id,
                        ctrl.expected_total, ctrl.processed_count, count, count,
                    )
                    if isinstance(action, SendAnswerAction):
                        return change, [(action.queue_name, action.message)]
                    return change, []

                instruction = self._handler.handle(
                    msg_id, client_id, sender_id, seq, message, bfn,
                    kind=MsgKind.CTRL_EOF_RECEIVED,
                )
            self._publish_commit_ack(instruction, ack)
        except Exception:
            logging.exception(
                "sum_eof_received_error | configuration=%s | id=%s | client_id=%s",
                CONFIGURATION, ID, client_id,
            )
            nack(requeue=True)

    def _handle_processed_request(self, client_id, ctrl, ack, nack) -> None:
        # Pure re-report (no state mutation): direct coordinator call. The reported
        # count comes from durable WAL state, so it is correct after recovery too.
        try:
            with self._lock:
                count = self._state.processed_count(client_id)
                action = self._coordinator.process_control_message(
                    MessageType.PROCESSED_REQUEST, client_id, ctrl, count, count
                )
            if isinstance(action, SendAnswerAction):
                self._tl_sender(action.queue_name).send(action.message)
            ack()
        except Exception:
            logging.exception(
                "sum_processed_request_error | configuration=%s | id=%s | client_id=%s",
                CONFIGURATION, ID, client_id,
            )
            nack(requeue=True)

    def _handle_flush_order(self, client_id, ctrl, ack, nack) -> None:
        # The dynamic leader (it set _leader_expected) ignores its own FLUSH_ORDER;
        # it flushes via the FLUSH_ACK path on the response thread instead.
        if self._coordinator.leader_expected(client_id) is not None:
            ack()
            return

        # Non-leader: emit partials + FLUSH_ACK, then clean up and close.
        # _on_flush_order is a pure read, so we determine the leader's response queue
        # straight from ctrl.sender_id and skip the coordinator call here.
        leader_id = ctrl.sender_id
        seq = client_id
        msg_id = f"fo:{client_id}:{ctrl.sender_id}"
        try:
            with self._lock:
                if self._state.is_closed(client_id):
                    ack()
                    return

                def bfn(_pl):
                    partials = self._state.partials_for(client_id)
                    forwarded = len(partials)
                    outputs = self._partials_outputs(client_id, partials)
                    flush_ack_msg = self._coordinator.build_flush_ack(client_id, forwarded)
                    flush_ack_dest = self._coordinator.response_queue_for(leader_id)
                    compound = SumState.compound_change(
                        SumState.coordinator_cleanup_change(client_id),
                        SumState.close_change(client_id),
                    )
                    return compound, outputs + [(flush_ack_dest, flush_ack_msg)]

                instruction = self._handler.handle(
                    msg_id, client_id, leader_id, seq, b"", bfn,
                    kind=MsgKind.CTRL_FLUSH_ORDER,
                )
            if self._publish_commit_ack(instruction, ack):
                logging.info(
                    "sum_flush_ack_sent | configuration=%s | id=%s | client_id=%s",
                    CONFIGURATION, ID, client_id,
                )
        except Exception:
            logging.exception(
                "sum_flush_order_error | configuration=%s | id=%s | client_id=%s",
                CONFIGURATION, ID, client_id,
            )
            nack(requeue=True)

    def _run_control_consumer(self) -> None:
        consumer = MessageMiddlewareQueueRabbitMQ(MOM_HOST, self._coordinator.my_control_queue())
        with self._stopped_lock:
            self._control_consumer = consumer
            already_stopped = self._stopped
        if already_stopped:
            self._safe_close(consumer)
            return
        try:
            consumer.start_consuming(
                lambda msg, ack, nack: self._handle_control(msg, ack, nack)
            )
        except Exception as e:
            if not self._closed:
                logging.error(
                    "sum_control_consumer_stopped | configuration=%s | id=%s | error=%s",
                    CONFIGURATION, ID, e,
                )
        finally:
            self._safe_close(consumer)

    # ---------- response path (leader) ----------

    def _handle_response(self, message: bytes, ack, nack) -> None:
        try:
            msg_type, client_id, ctrl = self._coordinator.parse_message(message)
        except Exception:
            logging.exception(
                "sum_response_parse_error | configuration=%s | id=%s", CONFIGURATION, ID
            )
            nack(requeue=False)
            return

        if msg_type == MessageType.PROCESSED_ANSWER:
            self._handle_processed_answer(client_id, ctrl, ack, nack)
        elif msg_type == MessageType.FLUSH_ACK:
            self._handle_flush_ack(message, client_id, ctrl, ack, nack)
        else:
            logging.warning(
                "sum_unexpected_response_type | configuration=%s | id=%s | msg_type=%s",
                CONFIGURATION, ID, msg_type,
            )
            ack()

    def _handle_processed_answer(self, client_id, ctrl, ack, nack) -> None:
        # Direct coordinator call (not WAL-tracked). Same accepted limitation as the
        # aggregator: if the leader crashes after acking but before snapshotting, the
        # accumulated responder/processed counts are lost. _leader_expected and
        # _pending_eof ARE durable, so the broadcast-specific deadlocks are closed;
        # this residual window is identical to the aggregator's.
        try:
            with self._lock:
                action = self._coordinator.process_control_message(
                    MessageType.PROCESSED_ANSWER, client_id, ctrl
                )
            if isinstance(action, BroadcastAction):
                if action.sleep_before > 0:
                    time.sleep(action.sleep_before)
                for qname in action.queue_names:
                    self._tl_sender(qname).send(action.message)
            ack()
        except Exception:
            logging.exception(
                "sum_processed_answer_error | configuration=%s | id=%s | client_id=%s",
                CONFIGURATION, ID, client_id,
            )
            nack(requeue=True)

    def _handle_flush_ack(self, message, client_id, ctrl, ack, nack) -> None:
        # WAL-tracked so the leader's final flush (partials + downstream EOF) and the
        # close are durable. business_fn predicts the last-ack outcome via read-only
        # coordinator accessors; the single mutation happens inside apply_change.
        sender_id = ctrl.sender_id
        seq = client_id
        msg_id = f"fa:{client_id}:{ctrl.sender_id}"
        try:
            with self._lock:
                if self._state.is_closed(client_id):
                    ack()
                    return

                already = self._coordinator.has_flush_ack(client_id, ctrl.sender_id)
                new_ack_count = self._coordinator.flush_ack_count(client_id) + (
                    0 if already else 1
                )

                def bfn(_pl):
                    ack_change = SumState.coordinator_msg_change(
                        MessageType.FLUSH_ACK, client_id, ctrl.sender_id,
                        ctrl.expected_total, ctrl.processed_count,
                    )
                    if new_ack_count >= SUM_AMOUNT - 1:
                        # Last FLUSH_ACK: leader emits its own partials + the
                        # consolidated downstream EOF, then closes.
                        own_partials = self._state.partials_for(client_id)
                        own_fwd = len(own_partials)
                        acks_fwd = self._coordinator.accumulated_forwarded(client_id) + (
                            0 if already else ctrl.processed_count
                        )
                        total_fwd = acks_fwd + own_fwd
                        outputs = (
                            self._partials_outputs(client_id, own_partials)
                            + self._eof_outputs(client_id, total_fwd)
                        )
                        compound = SumState.compound_change(
                            ack_change, SumState.close_change(client_id)
                        )
                        return compound, outputs
                    return ack_change, []

                instruction = self._handler.handle(
                    msg_id, client_id, sender_id, seq, message, bfn,
                    kind=MsgKind.CTRL_FLUSH_ACK,
                )
            self._publish_commit_ack(instruction, ack)
        except Exception:
            logging.exception(
                "sum_flush_ack_error | configuration=%s | id=%s | client_id=%s",
                CONFIGURATION, ID, client_id,
            )
            nack(requeue=True)

    def _run_response_consumer(self) -> None:
        consumer = MessageMiddlewareQueueRabbitMQ(MOM_HOST, self._coordinator.my_response_queue())
        with self._stopped_lock:
            self._response_consumer = consumer
            already_stopped = self._stopped
        if already_stopped:
            self._safe_close(consumer)
            return
        try:
            consumer.start_consuming(
                lambda msg, ack, nack: self._handle_response(msg, ack, nack)
            )
        except Exception as e:
            if not self._closed:
                logging.error(
                    "sum_response_consumer_stopped | configuration=%s | id=%s | error=%s",
                    CONFIGURATION, ID, e,
                )
        finally:
            self._safe_close(consumer)

    # ---------- lifecycle ----------

    @staticmethod
    def _safe_close(resource) -> None:
        try:
            resource.close()
        except Exception:
            pass

    def start(self) -> None:
        logging.info(
            "sum_start | configuration=%s | id=%s | mom_host=%s | input=%s | "
            "sum_amount=%s | aggregation_amount=%s",
            CONFIGURATION, ID, MOM_HOST, INPUT_QUEUE, SUM_AMOUNT, AGGREGATION_AMOUNT,
        )

        self._control_thread = threading.Thread(target=self._run_control_consumer)
        self._response_thread = threading.Thread(target=self._run_response_consumer)
        self._control_thread.start()
        self._response_thread.start()

        if CONFIGURATION in (C_Q2, C_Q3):
            self._input_queue = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST,
                INPUT_EXCHANGE,
                [self._input_routing_key()],
                queue_name=INPUT_QUEUE,
                exclusive=False,
            )
        else:
            self._input_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, INPUT_QUEUE)
        try:
            if not self._stopped:
                self._input_queue.start_consuming(self._process_message)
        finally:
            self.handle_sigterm()
            self._control_thread.join(timeout=5)
            self._response_thread.join(timeout=5)
            self._close()

    def handle_sigterm(self) -> None:
        with self._stopped_lock:
            if self._stopped:
                return
            self._stopped = True
            control_consumer = self._control_consumer
            response_consumer = self._response_consumer

        logging.info("sum_shutdown | configuration=%s | id=%s", CONFIGURATION, ID)
        for consumer in (self._input_queue, control_consumer, response_consumer):
            if consumer is not None:
                consumer.request_stop_consuming()

    def _close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._input_queue is not None:
            self._safe_close(self._input_queue)
