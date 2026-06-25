import logging
import os
import threading
import time

from common.eof_coordinator import (
    BroadcastAction,
    EofCoordinator,
    FlushAction,
    SendAnswerAction,
)
from common.fault_tolerance.handler import Action, EdgeSpec, PersistentStateHandler
from common.fault_tolerance.inbox import InboxStatus, MsgKind
from common.logging_utils import should_log_progress
from common.message_protocol.internal import (
    InternalProtocol,
    Q4CountedEdgeSerializer,
    Q4AccountId,
    Q4_EDGE_INCOMING,
    Q4_EDGE_OUTGOING,
    TransactionSerializer,
)
from common.message_protocol.internal.common import ControlMessage, MessageType
from common.message_protocol.internal.control_message_serializer import (
    ControlMessageSerializer,
)
from common.middleware import LazyQueue, ShardedPublisher
from common.middleware.middleware_rabbitmq import (
    ensure_exchange_queue_bindings,
    MessageMiddlewareExchangeRabbitMQ,
    MessageMiddlewareQueueRabbitMQ,
)
from common.routing import queue_name_for_worker

try:
    from q4_filter_state import Q4FilterState
except ImportError:
    from workers.scatter_gather.q4_filter.q4_filter_state import Q4FilterState


ID = int(os.environ["ID"])
LEADER_ID = 0
MOM_HOST = os.environ["MOM_HOST"]
INPUT_EXCHANGE = os.environ.get("Q4_FILTER_INPUT_EXCHANGE")
INPUT_ROUTING_PREFIX = os.environ.get(
    "Q4_FILTER_INPUT_ROUTING_PREFIX", "q4_filter"
)
Q4_FILTER_AMOUNT = int(os.environ.get("Q4_FILTER_AMOUNT", "1"))
Q4_FILTER_PREFIX = os.environ.get("Q4_FILTER_PREFIX", "q4_filter")
Q4_SUM_EXCHANGE = os.environ["Q4_SUM_EXCHANGE"]
Q4_SUM_AMOUNT = int(os.environ["Q4_SUM_AMOUNT"])
Q4_SUM_ROUTING_PREFIX = os.environ.get("Q4_SUM_ROUTING_PREFIX", "q4_sum")
Q4_FILTER_BATCH_MAX_EDGES = int(
    os.environ.get("Q4_FILTER_BATCH_MAX_EDGES", "5000")
)
STATE_DIR = os.environ.get("STATE_DIR", "")
SNAPSHOT_INTERVAL = int(os.environ.get("SNAPSHOT_INTERVAL", "1000"))
Q4_FILTER_CONTROL_QUEUE_PREFIX = f"{Q4_FILTER_PREFIX}_control"
Q4_FILTER_RESPONSE_QUEUE_PREFIX = f"{Q4_FILTER_PREFIX}_response"
Q4_SUM_EDGE = "q4_sum"


class Q4FilterWorker:
    def __init__(self):
        self._coordinator = EofCoordinator(
            instance_id=ID,
            total_instances=Q4_FILTER_AMOUNT,
            control_queue_prefix=Q4_FILTER_CONTROL_QUEUE_PREFIX,
            response_queue_prefix=Q4_FILTER_RESPONSE_QUEUE_PREFIX,
            mode="flush_order",
            leader_id=LEADER_ID,
        )
        self._state = Q4FilterState(self._coordinator, Q4_SUM_AMOUNT)
        self._handler = PersistentStateHandler(
            state_dir=STATE_DIR,
            node_id=f"q4_filter_{ID}",
            worker_state=self._state,
            snapshot_every=SNAPSHOT_INTERVAL,
            output_edges={
                Q4_SUM_EDGE: EdgeSpec(sender_id=ID, shard_count=Q4_SUM_AMOUNT)
            },
        )

        self._proto = InternalProtocol()
        self._tx_serializer = TransactionSerializer()
        self._counted_edge_serializer = Q4CountedEdgeSerializer()
        self._control_serializer = ControlMessageSerializer()
        self._lock = threading.Lock()
        self._tls = threading.local()

        self._input = self._new_input()
        self.control_consumer = None
        self.response_consumer = None
        self._control_thread = None
        self._response_thread = None
        self._closed = False
        self._stopped = False

        self._handler.recover()
        self._republish_pending()

    def _new_input(self):
        if INPUT_EXCHANGE:
            return MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST,
                INPUT_EXCHANGE,
                [queue_name_for_worker(INPUT_ROUTING_PREFIX, ID)],
                queue_name=queue_name_for_worker(INPUT_ROUTING_PREFIX, ID),
                exclusive=False,
            )
        return MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, os.environ.get("INPUT_QUEUE", "q4_filter_input")
        )

    def _ensure_output_bindings(self) -> None:
        ensure_exchange_queue_bindings(
            MOM_HOST,
            Q4_SUM_EXCHANGE,
            {
                queue_name_for_worker(Q4_SUM_ROUTING_PREFIX, index): (
                    queue_name_for_worker(Q4_SUM_ROUTING_PREFIX, index)
                )
                for index in range(Q4_SUM_AMOUNT)
            },
        )

    def _tl_sender(self, destination: str):
        if not hasattr(self._tls, "senders"):
            self._tls.senders = {}
        if destination not in self._tls.senders:
            if destination == Q4_SUM_EDGE:
                self._tls.senders[destination] = ShardedPublisher(
                    MOM_HOST,
                    Q4_SUM_EXCHANGE,
                    Q4_SUM_ROUTING_PREFIX,
                    Q4_SUM_AMOUNT,
                )
            else:
                self._tls.senders[destination] = LazyQueue(MOM_HOST, destination)
        return self._tls.senders[destination]

    def _republish_pending(self) -> None:
        for entry in self._handler.outbox_to_republish():
            try:
                self._publish([entry])
            except Exception:
                logging.exception(
                    "q4_filter_republish_error | id=%s | destination=%s",
                    ID,
                    entry.destination,
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

    def _build_data_outputs(self, client_id: int, payload: bytes) -> list:
        outputs = []
        by_partition = self._state.predict_data_outputs(client_id, payload)
        for partition in sorted(by_partition):
            for chunk in _chunks(by_partition[partition], Q4_FILTER_BATCH_MAX_EDGES):
                batch_payload = self._counted_edge_serializer.serialize_batch(chunk)
                outputs.append(
                    (
                        Q4_SUM_EDGE,
                        self._packet(MessageType.DATA, client_id, batch_payload),
                        partition,
                    )
                )
        return outputs

    def _build_eof_outputs(self, client_id: int) -> list:
        outputs = []
        counts = self._state.forwarded_by_partition(client_id)
        for partition in range(Q4_SUM_AMOUNT):
            expected_total = int(counts.get(partition, 0))
            outputs.append(
                (
                    Q4_SUM_EDGE,
                    self._packet(
                        MessageType.EOF,
                        client_id,
                        self._control_payload(ID, expected_total, 0),
                    ),
                    partition,
                )
            )
            logging.info(
                "q4_filter_forward_eof | id=%s | client_id=%s | "
                "edge_store_partition=%s | expected_total=%s",
                ID,
                client_id,
                partition,
                expected_total,
            )
        return outputs

    def _publish(self, entries) -> None:
        for entry in entries:
            sender = self._tl_sender(entry.destination)
            if entry.shard is None:
                sender.send(entry.body)
            else:
                sender.send_to_shard(entry.body, entry.shard)

    def _publish_then_commit(self, instruction) -> None:
        if instruction.action is Action.PUBLISH_THEN_COMMIT:
            self._publish(instruction.outputs)
            with self._lock:
                self._handler.commit_done(*instruction.ctx)

    def _do_broadcast(self, action: BroadcastAction) -> None:
        if action.sleep_before > 0:
            time.sleep(action.sleep_before)
        for qname in action.queue_names:
            self._tl_sender(qname).send(action.message)

    def _process_data_message(self, raw, ack, nack) -> None:
        try:
            msg_type, client_id, sender_id, seq, payload = (
                self._proto.unpack_addressed_packet(raw)
            )
            msg_id = f"d:{sender_id}:{client_id}:{seq}"

            if msg_type == MessageType.DATA:
                def bfn(data):
                    return (
                        Q4FilterState.data_change(client_id, data),
                        self._build_data_outputs(client_id, data),
                    )

                with self._lock:
                    instruction = self._handler.handle(
                        msg_id, client_id, sender_id, seq, payload, bfn
                    )
                self._publish_then_commit(instruction)
                ack()

                processed = self._state.processed_count(client_id)
                if should_log_progress(processed):
                    logging.info(
                        "q4_filter_data_batch | id=%s | client_id=%s | "
                        "processed_total=%s | forwarded_total=%s",
                        ID,
                        client_id,
                        processed,
                        self._state.forwarded_total(client_id),
                    )
                return

            if msg_type == MessageType.EOF:
                self._handle_upstream_eof(
                    msg_id, client_id, sender_id, seq, payload, ack
                )
                return

            if msg_type == MessageType.ABORT:
                self._handle_abort(client_id, sender_id, seq, ack)
                return

            raise ValueError(f"unexpected q4 source prefilter message: {msg_type}")
        except Exception:
            logging.exception("q4_filter_error | id=%s", ID)
            nack(requeue=True)

    def _handle_abort(self, client_id: int, sender_id: int, seq: int, ack) -> None:
        msg_id = f"abort:{sender_id}:{client_id}:{seq}"
        with self._lock:
            if self._state.is_closed(client_id):
                ack()
                return
            instruction = self._handler.handle(
                msg_id, client_id, sender_id, seq, b"",
                lambda _: (
                    Q4FilterState.abort_change(client_id),
                    [
                        (Q4_SUM_EDGE, self._packet(MessageType.ABORT, client_id, b""), partition)
                        for partition in range(Q4_SUM_AMOUNT)
                    ],
                ),
                kind=MsgKind.ABORT,
            )
        self._publish_then_commit(instruction)
        ack()
        logging.info("q4_filter_abort | id=%s | client_id=%s", ID, client_id)

    def _handle_upstream_eof(
        self,
        msg_id: str,
        client_id: int,
        sender_id: int,
        seq: int,
        payload: bytes,
        ack,
    ) -> None:
        ctrl = self._control_serializer.deserialize(payload)
        with self._lock:
            status = self._handler.inbox.classify(client_id, sender_id, seq)
            if self._state.is_closed(client_id) and status is not InboxStatus.APPLIED:
                ack()
                return

            count = self._state.processed_count(client_id)
            forwarded = self._state.forwarded_total(client_id)

            def bfn(_data):
                coordinator_snapshot = self._coordinator.snapshot()
                action = self._coordinator.on_upstream_eof(
                    client_id, ctrl.expected_total, count, forwarded
                )
                self._coordinator.restore(coordinator_snapshot)
                eof_change = Q4FilterState.coordinator_upstream_eof_change(
                    client_id, ctrl.expected_total, count, forwarded
                )
                if isinstance(action, SendAnswerAction):
                    return eof_change, [(action.queue_name, action.message)]
                if isinstance(action, FlushAction):
                    return (
                        Q4FilterState.compound_change(
                            eof_change, Q4FilterState.close_change(client_id)
                        ),
                        self._build_eof_outputs(client_id),
                    )
                return eof_change, []

            instruction = self._handler.handle(
                msg_id, client_id, sender_id, seq, payload, bfn
            )

        self._publish_then_commit(instruction)
        ack()
        logging.info(
            "q4_filter_upstream_eof | id=%s | client_id=%s | "
            "expected_total=%s | processed_snapshot=%s | forwarded_snapshot=%s",
            ID,
            client_id,
            ctrl.expected_total,
            count,
            forwarded,
        )

    def _handle_control(self, message, ack, nack) -> None:
        try:
            msg_type, client_id, ctrl = self._coordinator.parse_message(message)
        except Exception:
            logging.exception("q4_filter_control_parse_error | id=%s", ID)
            nack(requeue=True)
            return

        if msg_type == MessageType.FLUSH_ORDER:
            self._handle_flush_order(message, client_id, ctrl, ack, nack)
            return

        if msg_type == MessageType.PROCESSED_REQUEST:
            try:
                with self._lock:
                    count = self._state.processed_count(client_id)
                    forwarded = self._state.forwarded_total(client_id)
                    action = self._coordinator.process_control_message(
                        msg_type, client_id, ctrl, count, forwarded
                    )
                if isinstance(action, SendAnswerAction):
                    self._tl_sender(action.queue_name).send(action.message)
                ack()
            except Exception:
                logging.exception("q4_filter_control_error | id=%s", ID)
                nack(requeue=True)
            return

        logging.warning(
            "q4_filter_unexpected_control_type | id=%s | msg_type=%s | client_id=%s",
            ID,
            msg_type,
            client_id,
        )
        ack()

    def _handle_flush_order(self, message, client_id: int, ctrl: ControlMessage, ack, nack) -> None:
        if ID == LEADER_ID:
            logging.info(
                "q4_filter_flush_order_ignored_by_leader | id=%s | client_id=%s",
                ID,
                client_id,
            )
            ack()
            return

        sender_id = ctrl.sender_id
        seq = client_id
        msg_id = f"fo:{client_id}:{ctrl.sender_id}"

        try:
            with self._lock:
                status = self._handler.inbox.classify(
                    client_id, sender_id, seq, MsgKind.CTRL_FLUSH_ORDER
                )
                if self._state.is_closed(client_id) and status is not InboxStatus.APPLIED:
                    ack()
                    return

                def bfn(_data):
                    flush_ack = self._coordinator.build_flush_ack(client_id, 0)
                    return (
                        Q4FilterState.compound_change(
                            Q4FilterState.coordinator_cleanup_change(client_id),
                            Q4FilterState.close_change(client_id),
                        ),
                        self._build_eof_outputs(client_id)
                        + [(self._coordinator.response_queue_for(LEADER_ID), flush_ack)],
                    )

                instruction = self._handler.handle(
                    msg_id,
                    client_id,
                    sender_id,
                    seq,
                    message,
                    bfn,
                    kind=MsgKind.CTRL_FLUSH_ORDER,
                )

            self._publish_then_commit(instruction)
            ack()
        except Exception:
            logging.exception(
                "q4_filter_flush_order_error | id=%s | client_id=%s",
                ID,
                client_id,
            )
            nack(requeue=True)

    def _start_control_consumer(self) -> None:
        control_consumer = MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, self._coordinator.my_control_queue()
        )
        self.control_consumer = control_consumer
        try:
            if not self._stopped:
                control_consumer.start_consuming(self._handle_control)
        finally:
            control_consumer.close()

    def _handle_response(self, message, ack, nack) -> None:
        try:
            msg_type, client_id, ctrl = self._coordinator.parse_message(message)
        except Exception:
            logging.exception("q4_filter_response_parse_error | id=%s", ID)
            nack(requeue=True)
            return

        if msg_type == MessageType.PROCESSED_ANSWER:
            self._handle_processed_answer(message, client_id, ctrl, ack, nack)
            return

        if msg_type == MessageType.FLUSH_ACK:
            self._handle_flush_ack(message, client_id, ctrl, ack, nack)
            return

        logging.warning(
            "q4_filter_unexpected_response_type | id=%s | msg_type=%s",
            ID,
            msg_type,
        )
        ack()

    def _handle_processed_answer(
        self,
        message,
        client_id: int,
        ctrl: ControlMessage,
        ack,
        nack,
    ) -> None:
        sender_id = ctrl.sender_id
        seq = client_id
        msg_id = f"pa:{client_id}:{ctrl.sender_id}"

        try:
            with self._lock:
                def bfn(_data):
                    coordinator_snapshot = self._coordinator.snapshot()
                    action = self._coordinator.process_control_message(
                        MessageType.PROCESSED_ANSWER,
                        client_id,
                        ctrl,
                    )
                    self._coordinator.restore(coordinator_snapshot)
                    answer_change = Q4FilterState.coordinator_msg_change(
                        MessageType.PROCESSED_ANSWER,
                        client_id,
                        ctrl.sender_id,
                        ctrl.expected_total,
                        ctrl.processed_count,
                    )
                    if isinstance(action, BroadcastAction):
                        return answer_change, [
                            (qname, action.message) for qname in action.queue_names
                        ]
                    if action is not None:
                        logging.warning(
                            "q4_filter_unexpected_response_action | id=%s | action=%s",
                            ID,
                            action,
                        )
                    return answer_change, []

                instruction = self._handler.handle(
                    msg_id,
                    client_id,
                    sender_id,
                    seq,
                    message,
                    bfn,
                    kind=MsgKind.CTRL_PROCESSED_ANSWER,
                )

            self._publish_then_commit(instruction)
            ack()
        except Exception:
            logging.exception("q4_filter_response_error | id=%s", ID)
            nack(requeue=True)

    def _handle_flush_ack(self, message, client_id: int, ctrl: ControlMessage, ack, nack) -> None:
        sender_id = ctrl.sender_id
        seq = client_id
        msg_id = f"fa:{client_id}:{ctrl.sender_id}"

        try:
            with self._lock:
                status = self._handler.inbox.classify(
                    client_id, sender_id, seq, MsgKind.CTRL_FLUSH_ACK
                )
                if self._state.is_closed(client_id) and status is not InboxStatus.APPLIED:
                    ack()
                    return

                already = self._coordinator.has_flush_ack(client_id, ctrl.sender_id)
                new_ack_count = self._coordinator.flush_ack_count(client_id) + (
                    0 if already else 1
                )

                def bfn(_data):
                    ack_change = Q4FilterState.coordinator_msg_change(
                        MessageType.FLUSH_ACK,
                        client_id,
                        ctrl.sender_id,
                        ctrl.expected_total,
                        ctrl.processed_count,
                    )
                    if new_ack_count >= Q4_FILTER_AMOUNT - 1:
                        return (
                            Q4FilterState.compound_change(
                                ack_change, Q4FilterState.close_change(client_id)
                            ),
                            self._build_eof_outputs(client_id),
                        )
                    return ack_change, []

                instruction = self._handler.handle(
                    msg_id,
                    client_id,
                    sender_id,
                    seq,
                    message,
                    bfn,
                    kind=MsgKind.CTRL_FLUSH_ACK,
                )

            self._publish_then_commit(instruction)
            ack()
        except Exception:
            logging.exception(
                "q4_filter_flush_ack_error | id=%s | client_id=%s",
                ID,
                client_id,
            )
            nack(requeue=True)

    def _start_response_consumer(self) -> None:
        response_consumer = MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, self._coordinator.my_response_queue()
        )
        self.response_consumer = response_consumer
        try:
            if not self._stopped:
                response_consumer.start_consuming(self._handle_response)
        finally:
            response_consumer.close()

    def start(self) -> None:
        self._ensure_output_bindings()
        self._republish_pending()
        logging.info(
            "q4_filter_start | id=%s | amount=%s | input_exchange=%s | "
            "edge_store_exchange=%s | sum_amount=%s | leader=%s",
            ID,
            Q4_FILTER_AMOUNT,
            INPUT_EXCHANGE,
            Q4_SUM_EXCHANGE,
            Q4_SUM_AMOUNT,
            ID == LEADER_ID,
        )
        self._control_thread = threading.Thread(target=self._start_control_consumer)
        self._control_thread.start()
        if self._coordinator.needs_response_consumer():
            self._response_thread = threading.Thread(target=self._start_response_consumer)
            self._response_thread.start()

        try:
            if not self._stopped:
                self._input.start_consuming(self._process_data_message)
        finally:
            self.handle_sigterm()
            if self._control_thread is not None:
                self._control_thread.join(timeout=5)
            if self._response_thread is not None:
                self._response_thread.join(timeout=5)
            self.close()

    def handle_sigterm(self) -> None:
        if self._stopped:
            return
        self._stopped = True
        logging.info("q4_filter_shutdown | id=%s", ID)
        self._input.request_stop_consuming()
        if self.control_consumer is not None:
            self.control_consumer.request_stop_consuming()
        if self.response_consumer is not None:
            self.response_consumer.request_stop_consuming()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        for resource in (self._input,):
            if resource is None:
                continue
            try:
                resource.close()
            except Exception as e:
                logging.warning("q4_filter_close_error | id=%s | error=%s", ID, e)
        if hasattr(self._tls, "senders"):
            for sender in self._tls.senders.values():
                try:
                    sender.close()
                except Exception as e:
                    logging.warning("q4_filter_sender_close_error | id=%s | error=%s", ID, e)


def _chunks(items: list, size: int):
    for i in range(0, len(items), size):
        yield items[i : i + size]
