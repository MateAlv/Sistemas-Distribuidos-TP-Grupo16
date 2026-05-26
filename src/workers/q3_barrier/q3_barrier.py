import logging
import os
import threading

from common import middleware
from common.message_protocol.internal import (
    ControlMessage,
    ControlMessageSerializer,
    InternalProtocol,
    Q3AverageResultSerializer,
    TransactionSerializer,
)
from common.message_protocol.internal.common import MessageType


ID = int(os.environ.get("ID", "0"))
MOM_HOST = os.environ["MOM_HOST"]
AVERAGES_QUEUE = os.environ["Q3_AVERAGES_QUEUE"]
CANDIDATES_QUEUE = os.environ["Q3_CANDIDATES_QUEUE"]
OUTPUT_QUEUE = os.environ["GATEWAY_Q3_QUEUE"]
THRESHOLD_DIVISOR = float(os.getenv("Q3_THRESHOLD_DIVISOR", "100"))


class _ClientState:
    def __init__(self) -> None:
        self.averages: dict[str, float] = {}
        self.candidates = []
        self.avg_eof = False
        self.candidates_eof = False
        self.candidates_expected_total: int | None = None


class Q3BarrierWorker:
    def __init__(self):
        self.averages_input = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, AVERAGES_QUEUE
        )
        self.candidates_input = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, CANDIDATES_QUEUE
        )
        self.averages_output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )

        self.internal_protocol = InternalProtocol()
        self.control_serializer = ControlMessageSerializer()
        self.transaction_serializer = TransactionSerializer()
        self.lock = threading.Lock()
        self.states_by_client: dict[int, _ClientState] = {}
        self.closed_by_client: set[int] = set()
        self.candidates_thread: threading.Thread | None = None
        self.closed = False

    def _state_for_client(self, client_id: int) -> _ClientState:
        return self.states_by_client.setdefault(client_id, _ClientState())

    def _packet(self, msg_type: MessageType, client_id: int, payload: bytes) -> bytes:
        return self.internal_protocol.create_packet(
            msg_type=msg_type,
            client_id_bytes=client_id.to_bytes(16, byteorder="big"),
            payload=payload,
        )

    def _process_average_message(self, message: bytes) -> bool:
        msg_type, client_id, payload = self.internal_protocol.unpack_packet(message)
        with self.lock:
            if client_id in self.closed_by_client:
                return False
            state = self._state_for_client(client_id)
            if msg_type == MessageType.DATA:
                result = Q3AverageResultSerializer.deserialize(payload)
                state.averages[result.payment_format] = result.average
                return False
            if msg_type == MessageType.EOF:
                state.avg_eof = True
                logging.info(
                    "q3_barrier_averages_eof | id=%s | client_id=%s | averages=%s",
                    ID, client_id, len(state.averages),
                )
                return self._ready_locked(client_id)
        raise ValueError(f"unsupported Q3 averages message type: {msg_type}")

    def _process_candidate_message(self, message: bytes) -> bool:
        msg_type, client_id, payload = self.internal_protocol.unpack_packet(message)
        with self.lock:
            if client_id in self.closed_by_client:
                return False
            state = self._state_for_client(client_id)
            if msg_type == MessageType.DATA:
                state.candidates.extend(
                    self.transaction_serializer.deserialize_batch(payload)
                )
                return False
            if msg_type == MessageType.EOF:
                control = self.control_serializer.deserialize(payload)
                state.candidates_eof = True
                state.candidates_expected_total = control.expected_total
                logging.info(
                    "q3_barrier_candidates_eof | id=%s | client_id=%s | "
                    "candidates=%s | expected_total=%s",
                    ID, client_id, len(state.candidates), control.expected_total,
                )
                return self._ready_locked(client_id)
        raise ValueError(f"unsupported Q3 candidates message type: {msg_type}")

    def _ready_locked(self, client_id: int) -> bool:
        state = self.states_by_client.get(client_id)
        if state is None:
            return False
        return state.avg_eof and state.candidates_eof

    def _emit_client(self, client_id: int, output_queue) -> None:
        with self.lock:
            state = self.states_by_client.pop(client_id, None)
            if state is None or client_id in self.closed_by_client:
                return
            self.closed_by_client.add(client_id)

        emitted = 0
        for tx in state.candidates:
            avg = state.averages.get(tx.format)
            if avg is None:
                continue
            if tx.amount < (avg / THRESHOLD_DIVISOR):
                output_queue.send(
                    self._packet(
                        MessageType.DATA,
                        client_id,
                        self.transaction_serializer.serialize(tx),
                    )
                )
                emitted += 1

        control_payload = self.control_serializer.serialize(
            ControlMessage(sender_id=ID, expected_total=emitted, processed_count=0)
        )
        output_queue.send(
            self._packet(MessageType.EOF, client_id, control_payload)
        )
        logging.info(
            "q3_barrier_emit | id=%s | client_id=%s | candidates=%s | "
            "averages=%s | results=%s",
            ID, client_id, len(state.candidates), len(state.averages), emitted,
        )

    def _handle_average(self, message, ack, nack):
        try:
            should_emit = self._process_average_message(message)
            if should_emit:
                _, client_id, _ = self.internal_protocol.unpack_packet(message)
                self._emit_client(client_id, self.averages_output_queue)
            ack()
        except Exception:
            logging.exception("q3_barrier_average_error | id=%s", ID)
            nack()

    def _handle_candidate(self, message, ack, nack, output_queue):
        try:
            should_emit = self._process_candidate_message(message)
            if should_emit:
                _, client_id, _ = self.internal_protocol.unpack_packet(message)
                self._emit_client(client_id, output_queue)
            ack()
        except Exception:
            logging.exception("q3_barrier_candidate_error | id=%s", ID)
            nack()

    def _consume_candidates(self):
        output_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, OUTPUT_QUEUE)
        try:
            self.candidates_input.start_consuming(
                lambda message, ack, nack: self._handle_candidate(
                    message, ack, nack, output_queue
                )
            )
        finally:
            output_queue.close()

    def start(self):
        logging.info(
            "q3_barrier_start | id=%s | averages_queue=%s | candidates_queue=%s | "
            "output_queue=%s",
            ID, AVERAGES_QUEUE, CANDIDATES_QUEUE, OUTPUT_QUEUE,
        )
        self.candidates_thread = threading.Thread(target=self._consume_candidates)
        self.candidates_thread.start()
        try:
            self.averages_input.start_consuming(self._handle_average)
        finally:
            self.handle_sigterm()
            if self.candidates_thread is not None:
                self.candidates_thread.join(timeout=5)
            self.close()

    def handle_sigterm(self):
        try:
            self.averages_input.stop_consuming()
        except Exception:
            pass
        try:
            self.candidates_input.stop_consuming()
        except Exception:
            pass

    def close(self):
        if self.closed:
            return
        self.closed = True
        for resource in (
            self.averages_input,
            self.candidates_input,
            self.averages_output_queue,
        ):
            try:
                resource.close()
            except Exception as e:
                logging.warning("q3_barrier_close_error | id=%s | error=%s", ID, e)
