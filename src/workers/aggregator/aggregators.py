import logging
import os
import threading
import time

from common import middleware
from common.constants import C_Q2, C_Q3, C_Q5
from common.eof_coordinator import EofCoordinator, BroadcastAction, FlushAction, SendAnswerAction
from common.fault_tolerance.handler.persistent_state_handler import PersistentStateHandler
from common.fault_tolerance.inbox import MsgKind
from common.logging_utils import should_log_progress
from common.message_protocol.internal.common import MessageType
from common.message_protocol.internal.common.control_message import ControlMessage
from common.message_protocol.internal.control_message_serializer import ControlMessageSerializer
from common.message_protocol.internal import InternalProtocol
from common.middleware import LazyQueue, MessageMiddlewareQueueRabbitMQ

try:
    from aggregator_state import AggregatorState
    from processors import create_aggregator_processor
except ImportError:
    from workers.aggregator.aggregator_state import AggregatorState
    from workers.aggregator.processors import create_aggregator_processor


ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
CONFIGURATION = os.environ["CONFIGURATION"]

AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
LEADER_ID = 0
AGGREGATION_CONTROL_QUEUE_PREFIX = f"{AGGREGATION_PREFIX}_control"
AGGREGATION_RESPONSE_QUEUE_PREFIX = f"{AGGREGATION_PREFIX}_response"
STATE_DIR = os.environ.get("STATE_DIR", "/tmp/aggregator_state")
SNAPSHOT_INTERVAL = int(os.environ.get("SNAPSHOT_INTERVAL", "1000"))


class AggregatorWorker:
    def __init__(self):
        if CONFIGURATION not in (C_Q2, C_Q3, C_Q5):
            raise ValueError(f"Invalid aggregator configuration: {CONFIGURATION}")

        self._coordinator = EofCoordinator(
            instance_id=ID,
            total_instances=AGGREGATION_AMOUNT,
            control_queue_prefix=AGGREGATION_CONTROL_QUEUE_PREFIX,
            response_queue_prefix=AGGREGATION_RESPONSE_QUEUE_PREFIX,
            mode="flush_order",
            leader_id=LEADER_ID,
        )

        self._state = AggregatorState(
            configuration=CONFIGURATION,
            coordinator=self._coordinator,
            processor_factory=create_aggregator_processor,
        )

        self._handler = PersistentStateHandler(
            state_dir=STATE_DIR,
            node_id=f"agg_{ID}",
            worker_state=self._state,
            snapshot_every=SNAPSHOT_INTERVAL,
        )

        self._internal_protocol = InternalProtocol()
        self._control_serializer = ControlMessageSerializer()
        self._lock = threading.Lock()

        # Thread-local storage for per-thread lazy queue connections.
        # Pika connections are not thread-safe; each thread keeps its own.
        self._tls = threading.local()

        self._input_exchange = None
        self.control_consumer = None
        self.response_consumer = None
        self.control_thread = None
        self.response_thread = None
        self.closed = False
        self._stopped = False

        self._handler.recover()
        self._republish_pending()

    # ---------- connection helpers ----------

    def _tl_sender(self, destination: str) -> LazyQueue:
        """Return the thread-local lazy queue for the given destination."""
        if not hasattr(self._tls, "senders"):
            self._tls.senders = {}
        if destination not in self._tls.senders:
            self._tls.senders[destination] = LazyQueue(MOM_HOST, destination)
        return self._tls.senders[destination]

    def _republish_pending(self) -> None:
        """Re-publish outputs that were stamped but not committed before the last crash."""
        for entry in self._handler.outbox_to_republish():
            try:
                self._tl_sender(entry.destination).send(entry.body)
            except Exception:
                logging.exception(
                    "aggregation_republish_error | id=%s | destination=%s",
                    ID, entry.destination,
                )

    # ---------- packet helpers ----------

    def _packet(self, msg_type, client_id, payload):
        return self._internal_protocol.create_packet(
            msg_type=msg_type,
            client_id_bytes=client_id.to_bytes(16, byteorder="big"),
            payload=payload,
        )

    def _eof_payload(self, expected_total):
        return self._control_serializer.serialize(
            ControlMessage(sender_id=ID, expected_total=expected_total, processed_count=0)
        )

    def _build_result_outputs(self, client_id: int, results: list, data_count: int) -> list:
        """Build [(destination, bytes)] for emitting aggregated results downstream."""
        outputs = [
            (OUTPUT_QUEUE, self._packet(MessageType.DATA, client_id, p))
            for p in results
        ]
        outputs.append((OUTPUT_QUEUE, self._packet(
            MessageType.EOF, client_id, self._eof_payload(len(results))
        )))
        logging.info(
            "aggregation_emit | configuration=%s | id=%s | client_id=%s | "
            "input_data=%s | results=%s",
            CONFIGURATION, ID, client_id, data_count, len(results),
        )
        return outputs

    # ---------- data path ----------

    def _process_data_message(self, message, ack, nack):
        try:
            msg_type, client_id, sender_id, seq, payload = (
                self._internal_protocol.unpack_addressed_packet(message)
            )
            msg_id = f"d:{sender_id}:{client_id}:{seq}"

            if msg_type == MessageType.DATA:
                def bfn(_pl):
                    return AggregatorState.data_change(client_id, _pl), []

                with self._lock:
                    instruction = self._handler.handle(
                        msg_id, client_id, sender_id, seq, payload, bfn
                    )
                # DATA has no outputs; commit immediately.
                with self._lock:
                    self._handler.commit_done(*instruction.ctx)
                ack()

                count = self._state.data_count(client_id)
                if should_log_progress(count):
                    logging.info(
                        "aggregation_data | configuration=%s | id=%s | "
                        "client_id=%s | data_count=%s",
                        CONFIGURATION, ID, client_id, count,
                    )

            elif msg_type == MessageType.EOF:
                ctrl = self._control_serializer.deserialize(payload)

                with self._lock:
                    if self._state.is_closed(client_id):
                        ack()
                        return
                    count = self._state.data_count(client_id)

                    def bfn(_pl):
                        # on_upstream_eof is idempotent for flush_order mode: the second call
                        # (from apply_change on WAL replay) returns None because client_id is
                        # already in _seen_eof, leaving coordinator state unchanged.
                        action = self._coordinator.on_upstream_eof(
                            client_id, ctrl.expected_total, count, 0
                        )
                        eof_change = AggregatorState.coordinator_upstream_eof_change(
                            client_id, ctrl.expected_total, count, 0
                        )
                        if isinstance(action, SendAnswerAction):
                            # N>1: report to leader; leader accumulates and broadcasts FLUSH_ORDER.
                            return eof_change, [(action.queue_name, action.message)]
                        if isinstance(action, FlushAction):
                            # N=1 shortcut: no coordination needed, flush directly.
                            results = self._state.results_for(client_id)
                            outputs = self._build_result_outputs(client_id, results, count)
                            compound = AggregatorState.compound_change(
                                eof_change, AggregatorState.close_change(client_id)
                            )
                            return compound, outputs
                        # None: duplicate (already in _seen_eof after apply_change replay).
                        return eof_change, []

                    instruction = self._handler.handle(
                        msg_id, client_id, sender_id, seq, payload, bfn
                    )

                for entry in instruction.outputs:
                    self._tl_sender(entry.destination).send(entry.body)
                with self._lock:
                    self._handler.commit_done(*instruction.ctx)
                ack()
                logging.info(
                    "aggregation_upstream_eof | configuration=%s | id=%s | "
                    "client_id=%s | data_count=%s | expected_total=%s",
                    CONFIGURATION, ID, client_id, count, ctrl.expected_total,
                )

            else:
                raise ValueError(f"unsupported aggregator message type: {msg_type}")

        except Exception:
            logging.exception(
                "aggregation_data_error | configuration=%s | id=%s", CONFIGURATION, ID
            )
            nack()

    # ---------- control path ----------

    def _handle_control(self, message, ack, nack):
        try:
            msg_type, client_id, ctrl = self._coordinator.parse_message(message)
        except Exception:
            logging.exception(
                "aggregation_control_parse_error | configuration=%s | id=%s",
                CONFIGURATION, ID,
            )
            nack()
            return

        if msg_type != MessageType.FLUSH_ORDER:
            logging.warning(
                "aggregation_unexpected_control_type | id=%s | msg_type=%s | client_id=%s",
                ID, msg_type, client_id,
            )
            ack()
            return

        # In flush_order mode the leader is fixed (LEADER_ID=0) and ignores FLUSH_ORDER.
        if ID == LEADER_ID:
            logging.info(
                "aggregation_flush_order_ignored_by_leader | id=%s | client_id=%s",
                ID, client_id,
            )
            ack()
            return

        # Non-leader: flush results and send FLUSH_ACK to the leader.
        # MsgKind.CTRL_FLUSH_ORDER separates this from DATA messages in the inbox,
        # so real sender_id values are safe even when they equal upstream worker IDs.
        # seq=client_id is unique: at most one FLUSH_ORDER per (client, leader) pair.
        sender_id = ctrl.sender_id
        seq = client_id
        msg_id = f"fo:{client_id}:{ctrl.sender_id}"

        try:
            with self._lock:
                if self._state.is_closed(client_id):
                    ack()
                    return

                def bfn(_pl):
                    # _on_flush_order is pure read (no coordinator state mutation).
                    results = self._state.results_for(client_id)
                    data_count = self._state.data_count(client_id)
                    outputs = self._build_result_outputs(client_id, results, data_count)
                    flush_ack_msg = self._coordinator.build_flush_ack(client_id, 0)
                    flush_ack_dest = self._coordinator.response_queue_for(LEADER_ID)
                    compound = AggregatorState.compound_change(
                        AggregatorState.coordinator_cleanup_change(client_id),
                        AggregatorState.close_change(client_id),
                    )
                    return compound, outputs + [(flush_ack_dest, flush_ack_msg)]

                instruction = self._handler.handle(
                    msg_id, client_id, sender_id, seq, message, bfn,
                    kind=MsgKind.CTRL_FLUSH_ORDER,
                )

            for entry in instruction.outputs:
                self._tl_sender(entry.destination).send(entry.body)
            with self._lock:
                self._handler.commit_done(*instruction.ctx)
            ack()

        except Exception:
            logging.exception(
                "aggregation_control_error | configuration=%s | id=%s | client_id=%s",
                CONFIGURATION, ID, client_id,
            )
            nack()

    def _start_control_consumer(self):
        control_consumer = MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, self._coordinator.my_control_queue()
        )
        self.control_consumer = control_consumer
        try:
            if not self._stopped:
                control_consumer.start_consuming(
                    lambda msg, ack, nack: self._handle_control(msg, ack, nack)
                )
        finally:
            control_consumer.close()

    # ---------- response path (leader only in flush_order mode) ----------

    def _handle_response(self, message, ack, nack):
        try:
            msg_type, client_id, ctrl = self._coordinator.parse_message(message)
        except Exception:
            logging.exception(
                "aggregation_response_parse_error | configuration=%s | id=%s",
                CONFIGURATION, ID,
            )
            nack()
            return

        if msg_type == MessageType.PROCESSED_ANSWER:
            # Direct coordinator call (not via PersistentStateHandler).
            # Limitation: if the leader crashes after acking but before snapshotting,
            # the accumulated responder/processed state is lost.  Non-leaders' messages
            # are redelivered by RabbitMQ and rebuild the state on the next run.
            with self._lock:
                action = self._coordinator.process_control_message(msg_type, client_id, ctrl)
            if action is None:
                ack()
                return
            if isinstance(action, BroadcastAction):
                if action.sleep_before > 0:
                    time.sleep(action.sleep_before)
                for qname in action.queue_names:
                    self._tl_sender(qname).send(action.message)
                ack()
            else:
                logging.warning(
                    "aggregation_unexpected_response_action | id=%s | action=%s", ID, action
                )
                ack()

        elif msg_type == MessageType.FLUSH_ACK:
            # Via PersistentStateHandler so the close_change is WAL-tracked.
            # business_fn uses read-only coordinator accessors to predict whether this is
            # the last FLUSH_ACK without calling the non-idempotent process_control_message.
            # The actual coordinator mutation happens inside apply_change (single call).
            # MsgKind.CTRL_FLUSH_ACK separates from DATA so real sender_id values are safe.
            sender_id = ctrl.sender_id
            seq = client_id  # unique: at most one FLUSH_ACK per (client, non-leader) pair
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
                        ack_change = AggregatorState.coordinator_msg_change(
                            MessageType.FLUSH_ACK, client_id, ctrl.sender_id,
                            ctrl.expected_total, ctrl.processed_count,
                        )
                        if new_ack_count >= AGGREGATION_AMOUNT - 1:
                            # Last FLUSH_ACK received — leader emits results.
                            results = self._state.results_for(client_id)
                            data_count = self._state.data_count(client_id)
                            outputs = self._build_result_outputs(client_id, results, data_count)
                            compound = AggregatorState.compound_change(
                                ack_change, AggregatorState.close_change(client_id)
                            )
                            return compound, outputs
                        return ack_change, []

                    instruction = self._handler.handle(
                        msg_id, client_id, sender_id, seq, message, bfn,
                        kind=MsgKind.CTRL_FLUSH_ACK,
                    )

                for entry in instruction.outputs:
                    self._tl_sender(entry.destination).send(entry.body)
                with self._lock:
                    self._handler.commit_done(*instruction.ctx)
                ack()

            except Exception:
                logging.exception(
                    "aggregation_flush_ack_error | configuration=%s | id=%s | client_id=%s",
                    CONFIGURATION, ID, client_id,
                )
                nack()

        else:
            logging.warning(
                "aggregation_unexpected_response_type | id=%s | msg_type=%s", ID, msg_type
            )
            ack()

    def _start_response_consumer(self):
        response_consumer = MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, self._coordinator.my_response_queue()
        )
        self.response_consumer = response_consumer
        try:
            if not self._stopped:
                response_consumer.start_consuming(
                    lambda msg, ack, nack: self._handle_response(msg, ack, nack)
                )
        finally:
            response_consumer.close()

    # ---------- lifecycle ----------

    def start(self):
        logging.info(
            "aggregation_start | configuration=%s | id=%s | exchange=%s | "
            "output_queue=%s | leader=%s | total=%s",
            CONFIGURATION, ID, f"{AGGREGATION_PREFIX}_{ID}", OUTPUT_QUEUE,
            ID == LEADER_ID, AGGREGATION_AMOUNT,
        )
        self.control_thread = threading.Thread(target=self._start_control_consumer)
        self.control_thread.start()
        if self._coordinator.needs_response_consumer():
            self.response_thread = threading.Thread(target=self._start_response_consumer)
            self.response_thread.start()

        self._input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"]
        )
        try:
            if not self._stopped:
                self._input_exchange.start_consuming(
                    lambda msg, ack, nack: self._process_data_message(msg, ack, nack)
                )
        finally:
            self.handle_sigterm()
            if self.control_thread is not None:
                self.control_thread.join(timeout=5)
            if self.response_thread is not None:
                self.response_thread.join(timeout=5)
            self.close()

    def handle_sigterm(self):
        if self._stopped:
            return
        self._stopped = True
        logging.info(
            "aggregation_shutdown | configuration=%s | id=%s", CONFIGURATION, ID
        )
        self._input_exchange.request_stop_consuming()
        if self.control_consumer is not None:
            self.control_consumer.request_stop_consuming()
        if self.response_consumer is not None:
            self.response_consumer.request_stop_consuming()

    def close(self):
        if self.closed:
            return
        self.closed = True
        logging.info("aggregation_close | configuration=%s | id=%s", CONFIGURATION, ID)
        try:
            self._input_exchange.close()
        except Exception as e:
            logging.warning("aggregation_close_error | id=%s | error=%s", ID, e)
