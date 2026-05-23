import logging
import os
import threading
from collections import defaultdict

from common import message_protocol
from common.middleware.middleware_rabbitmq import (
    MessageMiddlewareExchangeRabbitMQ,
    MessageMiddlewareQueueRabbitMQ,
)
from common.message_protocol.internal.common import ControlMessage, MessageType
from common.message_protocol.internal.control_message_serializer import ControlMessageSerializer
from common.message_protocol.internal.scatter_gather_serializer import (
    ScatterGatherResult,
    ScatterGatherResultSerializer,
    ScatterGatherRelationSerializer,
)

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
SG_DETECTOR_EXCHANGE = os.environ["SG_DETECTOR_EXCHANGE"]
GATEWAY_Q4_QUEUE = os.environ["GATEWAY_Q4_QUEUE"]
MIN_INTERMEDIARIES = int(os.environ.get("MIN_INTERMEDIARIES", "5"))
SG_LINKER_AMOUNT = int(os.environ.get("SG_LINKER_AMOUNT", "1"))
SG_DETECTOR_AMOUNT = int(os.environ.get("SG_DETECTOR_AMOUNT", "1"))
SG_DETECTOR_PREFIX = os.environ.get("SG_DETECTOR_PREFIX", "sg_detector")
SG_DETECTOR_CONTROL_EXCHANGE = os.environ.get(
    "SG_DETECTOR_CONTROL_EXCHANGE", f"{SG_DETECTOR_PREFIX}_control"
)
SG_DETECTOR_RESPONSE_QUEUE_PREFIX = os.environ.get(
    "SG_DETECTOR_RESPONSE_QUEUE_PREFIX", f"{SG_DETECTOR_PREFIX}_response"
)


class ScatterGatherDetector:
    def __init__(self):
        self._input = MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST,
            SG_DETECTOR_EXCHANGE,
            [f"sg_detector_{ID}"],
            queue_name=f"sg_detector_{ID}",
            exclusive=False,
        )
        self._output = MessageMiddlewareQueueRabbitMQ(MOM_HOST, GATEWAY_Q4_QUEUE)
        self._control_sender = None
        if SG_DETECTOR_AMOUNT > 1:
            self._control_sender = MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST,
                SG_DETECTOR_CONTROL_EXCHANGE,
                [SG_DETECTOR_CONTROL_EXCHANGE],
            )
        self._response_queue_name = f"{SG_DETECTOR_RESPONSE_QUEUE_PREFIX}_{ID}"
        self._proto = message_protocol.internal.InternalProtocol()
        self._control_serializer = ControlMessageSerializer()

        # client_id -> (A, B) -> {M}
        self._intermediaries = defaultdict(lambda: defaultdict(set))
        self._emitted = defaultdict(set)
        self._emitted_count_by_client = defaultdict(int)
        self._processed_by_client = defaultdict(int)
        self._eofs_by_client = defaultdict(set)
        self._linker_expected_by_client = defaultdict(dict)
        self._complete_by_client = set()
        self._pending_eof_by_client = {}
        self._reported_initial_by_client = set()
        self._leader_expected_by_client = {}
        self._leader_processed_by_client = defaultdict(int)
        self._leader_emitted_by_client = defaultdict(int)
        self._closed_by_client = set()
        self._lock = threading.Lock()

        self._control_consumer = None
        self._response_consumer = None
        self._control_thread = None
        self._response_thread = None
        self._closed = False

    def _on_message(self, raw, ack, nack):
        try:
            msg_type, client_id, payload = self._proto.unpack_packet(raw)
            with self._lock:
                closed = client_id in self._closed_by_client
            if closed:
                logging.info(
                    "detector_%s message_for_closed_client | client_id=%s",
                    ID,
                    client_id,
                )
                ack()
                return

            if msg_type == MessageType.EOF:
                self._handle_eof(client_id, payload)
                ack()
                return
            if msg_type != MessageType.DATA:
                ack()
                return

            relation = ScatterGatherRelationSerializer.deserialize(payload)
            self._add_relation(
                client_id,
                relation.from_account,
                relation.intermediate_account,
                relation.to_account,
            )
            ack()
        except Exception:
            logging.exception("detector_%s error", ID)
            nack()

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

    def _add_relation(self, client_id: int, a: str, m: str, b: str):
        pair = (a, b)
        should_emit = False

        with self._lock:
            if client_id in self._closed_by_client:
                return

            emitted_delta = 0
            if pair not in self._emitted[client_id]:
                intermediaries = self._intermediaries[client_id][pair]
                intermediaries.add(m)
                should_emit = len(intermediaries) >= MIN_INTERMEDIARIES
                if should_emit:
                    emitted_delta = 1
                    self._emitted[client_id].add(pair)
                    self._emitted_count_by_client[client_id] += 1
                    del self._intermediaries[client_id][pair]

        if should_emit:
            self._emit(client_id, a, b)

        leader_id = None
        with self._lock:
            if client_id in self._closed_by_client:
                return
            self._processed_by_client[client_id] += 1
            pending = self._pending_eof_by_client.get(client_id)
            if pending is not None:
                _, leader_id = pending

        if leader_id is not None:
            self._report_to_leader(
                client_id,
                leader_id,
                processed_count=1,
                emitted_count=emitted_delta,
            )

    def _emit(self, client_id: int, a: str, b: str):
        self._output.send(
            self._packet(
                MessageType.DATA,
                client_id,
                ScatterGatherResultSerializer.serialize(
                    ScatterGatherResult(from_account=a, to_account=b)
                ),
            )
        )
        logging.debug("detector_%s emitted (%s, %s)", ID, a, b)

    def _forward_eof_to_gateway(self, client_id: int, expected_total: int):
        self._output.send(
            self._packet(
                MessageType.EOF,
                client_id,
                self._control_payload(ID, expected_total, 0),
            )
        )
        logging.info(
            "detector_%s forwarded_eof | client_id=%s | expected_total=%s",
            ID,
            client_id,
            expected_total,
        )

    def _broadcast_eof(self, client_id: int, expected_total: int) -> None:
        if self._control_sender is None:
            raise RuntimeError("detector control sender is not configured")
        self._control_sender.send(
            self._packet(
                MessageType.EOF_RECEIVED,
                client_id,
                self._control_payload(ID, expected_total, 0),
            )
        )

    def _report_to_leader(
        self,
        client_id: int,
        leader_id: int,
        processed_count: int,
        emitted_count: int,
    ) -> None:
        response_queue = MessageMiddlewareQueueRabbitMQ(
            MOM_HOST,
            f"{SG_DETECTOR_RESPONSE_QUEUE_PREFIX}_{leader_id}",
        )
        try:
            response_queue.send(
                self._packet(
                    MessageType.PROCESSED_ANSWER,
                    client_id,
                    self._control_payload(
                        sender_id=ID,
                        expected_total=emitted_count,
                        processed_count=processed_count,
                    ),
                )
            )
        finally:
            response_queue.close()

    def _try_initial_report_locked(self, client_id: int):
        if client_id in self._reported_initial_by_client:
            return None
        pending = self._pending_eof_by_client.get(client_id)
        if pending is None:
            return None

        self._reported_initial_by_client.add(client_id)
        _, leader_id = pending
        return (
            leader_id,
            self._processed_by_client.get(client_id, 0),
            self._emitted_count_by_client.get(client_id, 0),
        )

    def _cleanup_client_locked(self, client_id: int):
        self._intermediaries.pop(client_id, None)
        self._emitted.pop(client_id, None)
        self._emitted_count_by_client.pop(client_id, None)
        self._processed_by_client.pop(client_id, None)
        self._eofs_by_client.pop(client_id, None)
        self._linker_expected_by_client.pop(client_id, None)
        self._pending_eof_by_client.pop(client_id, None)
        self._reported_initial_by_client.discard(client_id)
        self._complete_by_client.discard(client_id)
        self._leader_expected_by_client.pop(client_id, None)
        self._leader_processed_by_client.pop(client_id, None)
        self._leader_emitted_by_client.pop(client_id, None)
        self._closed_by_client.add(client_id)

    def _maybe_cleanup_after_report(self, client_id: int):
        with self._lock:
            if (
                ID != 0
                and client_id in self._complete_by_client
                and client_id in self._reported_initial_by_client
            ):
                self._cleanup_client_locked(client_id)

    def _handle_eof(self, client_id: int, payload: bytes):
        control_message = self._control_serializer.deserialize(payload)
        report = None
        should_forward_single_eof = False
        should_broadcast = False
        expected_total = 0
        emitted_total = 0

        with self._lock:
            if control_message.sender_id in self._eofs_by_client[client_id]:
                return

            self._eofs_by_client[client_id].add(control_message.sender_id)
            self._linker_expected_by_client[client_id][
                control_message.sender_id
            ] = control_message.expected_total
            if len(self._eofs_by_client[client_id]) < SG_LINKER_AMOUNT:
                return

            self._complete_by_client.add(client_id)
            expected_total = sum(
                self._linker_expected_by_client[client_id].values()
            )

            if SG_DETECTOR_AMOUNT == 1:
                emitted_total = self._emitted_count_by_client.get(client_id, 0)
                self._cleanup_client_locked(client_id)
                should_forward_single_eof = True
            elif ID == 0:
                self._leader_expected_by_client[client_id] = expected_total
                should_broadcast = True
            else:
                report = self._try_initial_report_locked(client_id)

        if should_forward_single_eof:
            self._forward_eof_to_gateway(client_id, emitted_total)
            return

        if should_broadcast:
            logging.info(
                "detector_%s broadcast_eof | client_id=%s | expected_total=%s",
                ID,
                client_id,
                expected_total,
            )
            self._broadcast_eof(client_id, expected_total)

        if report is not None:
            leader_id, processed_count, emitted_count = report
            self._report_to_leader(
                client_id,
                leader_id,
                processed_count=processed_count,
                emitted_count=emitted_count,
            )

        self._maybe_cleanup_after_report(client_id)

    def _handle_eof_broadcast(self, message: bytes, ack, nack) -> None:
        try:
            msg_type, client_id, payload = self._proto.unpack_packet(message)
            if msg_type != MessageType.EOF_RECEIVED:
                raise ValueError(f"unexpected detector control message type: {msg_type}")

            control_message = self._control_serializer.deserialize(payload)
            leader_id = control_message.sender_id
            expected_total = control_message.expected_total
            report = None

            with self._lock:
                if client_id in self._closed_by_client:
                    ack()
                    return
                if client_id not in self._pending_eof_by_client:
                    self._pending_eof_by_client[client_id] = (
                        expected_total,
                        leader_id,
                    )
                report = self._try_initial_report_locked(client_id)

            if report is not None:
                leader_id, processed_count, emitted_count = report
                self._report_to_leader(
                    client_id,
                    leader_id,
                    processed_count=processed_count,
                    emitted_count=emitted_count,
                )
                self._maybe_cleanup_after_report(client_id)

            ack()
        except Exception:
            logging.exception("detector_%s control_error", ID)
            nack()

    def _handle_leader_report(self, message: bytes, ack, nack) -> None:
        try:
            msg_type, client_id, payload = self._proto.unpack_packet(message)
            if msg_type != MessageType.PROCESSED_ANSWER:
                raise ValueError(f"unexpected detector response message type: {msg_type}")

            control_message = self._control_serializer.deserialize(payload)
            should_forward_eof = False
            emitted_total = 0

            with self._lock:
                if client_id in self._closed_by_client:
                    ack()
                    return

                self._leader_processed_by_client[client_id] += (
                    control_message.processed_count
                )
                self._leader_emitted_by_client[client_id] += (
                    control_message.expected_total
                )
                expected_total = self._leader_expected_by_client.get(client_id)

                if (
                    expected_total is not None
                    and self._leader_processed_by_client[client_id] >= expected_total
                ):
                    should_forward_eof = True
                    emitted_total = self._leader_emitted_by_client[client_id]
                    self._cleanup_client_locked(client_id)

            if should_forward_eof:
                self._forward_eof_to_gateway(client_id, emitted_total)

            ack()
        except Exception:
            logging.exception("detector_%s response_error", ID)
            nack()

    def _start_control_consumer(self) -> None:
        control_consumer = MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST,
            SG_DETECTOR_CONTROL_EXCHANGE,
            [SG_DETECTOR_CONTROL_EXCHANGE],
        )
        self._control_consumer = control_consumer
        try:
            control_consumer.start_consuming(self._handle_eof_broadcast)
        finally:
            control_consumer.close()

    def _start_response_consumer(self) -> None:
        response_consumer = MessageMiddlewareQueueRabbitMQ(
            MOM_HOST,
            self._response_queue_name,
        )
        self._response_consumer = response_consumer
        try:
            response_consumer.start_consuming(self._handle_leader_report)
        finally:
            response_consumer.close()

    def start(self):
        logging.info("detector_%s starting", ID)
        if SG_DETECTOR_AMOUNT > 1:
            self._control_thread = threading.Thread(target=self._start_control_consumer)
            self._response_thread = threading.Thread(target=self._start_response_consumer)
            self._control_thread.start()
            self._response_thread.start()
        try:
            self._input.start_consuming(self._on_message)
        finally:
            self.handle_sigterm()
            if self._control_thread is not None:
                self._control_thread.join(timeout=5)
            if self._response_thread is not None:
                self._response_thread.join(timeout=5)
            self.close()

    def handle_sigterm(self):
        if self._closed:
            return
        logging.info("detector_%s sigterm", ID)
        self._input.stop_consuming()
        if self._control_consumer is not None:
            self._control_consumer.stop_consuming()
        if self._response_consumer is not None:
            self._response_consumer.stop_consuming()

    def close(self):
        if self._closed:
            return
        self._closed = True
        self._input.close()
        self._output.close()
        if self._control_sender is not None:
            self._control_sender.close()
