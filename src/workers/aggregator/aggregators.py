import logging
import os
import threading

from common import middleware
from common.constants import C_Q2, C_Q3, C_Q5
from common.message_protocol.internal.common import MessageType
from common.message_protocol.internal.common.control_message import ControlMessage
from common.message_protocol.internal.control_message_serializer import ControlMessageSerializer
from common.message_protocol.internal import InternalProtocol

try:
    from processors import create_aggregator_processor
except ImportError:
    from workers.aggregator.processors import create_aggregator_processor


ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
CONFIGURATION = os.environ["CONFIGURATION"]

# entrada por un exchange shardeado en {AGGREGATION_PREFIX}_{ID},
# salida por una única cola hacia el Join.
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]

# La reducción concreta por query vive en processors.py (igual que el Sum):
#   - "Q2": máximo monto por banco emisor.
#   - "Q3": promedio (sum / count) por payment_format.
#   - "Q5": conteo de transacciones (sin Sum: cada DATA suma 1).
# El productor upstream envía un único EOF por cliente con la cantidad total
# de DATA que el aggregator debe haber recibido antes de emitir resultados.


class AggregatorWorker:
    def __init__(self):
        if CONFIGURATION not in (C_Q2, C_Q3, C_Q5):
            raise ValueError(f"Invalid aggregator configuration: {CONFIGURATION}")

        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )

        self.internal_protocol = InternalProtocol()
        self.control_serializer = ControlMessageSerializer()

        self.lock = threading.Lock()
        self.closed = False
        self.processors_by_client = {}
        self.closed_by_client = set()
        self.data_count_by_client = {}
        self.expected_total_by_client = {}

    def _processor_for_client(self, client_id: int):
        return self.processors_by_client.setdefault(
            client_id,
            create_aggregator_processor(CONFIGURATION),
        )

    def _packet(self, msg_type: MessageType, client_id: int, payload: bytes) -> bytes:
        return self.internal_protocol.create_packet(
            msg_type=msg_type,
            client_id_bytes=client_id.to_bytes(16, byteorder="big"),
            payload=payload,
        )

    def _emit_results(self, client_id: int) -> None:
        with self.lock:
            processor = self.processors_by_client.pop(client_id, None)
            data_count = self.data_count_by_client.pop(client_id, 0)
            expected_total = self.expected_total_by_client.pop(client_id, None)
            self.closed_by_client.add(client_id)

        payloads = processor.results() if processor is not None else []
        for payload in payloads:
            self.output_queue.send(
                self._packet(MessageType.DATA, client_id, payload)
            )

        control_payload = self.control_serializer.serialize(
            ControlMessage(
                sender_id=ID, expected_total=len(payloads), processed_count=0
            )
        )
        self.output_queue.send(
            self._packet(MessageType.EOF, client_id, control_payload)
        )
        logging.info(
            "aggregation_emit | configuration=%s | id=%s | client_id=%s | "
            "input_data=%s | expected_total=%s | results=%s",
            CONFIGURATION,
            ID,
            client_id,
            data_count,
            expected_total,
            len(payloads),
        )

    def _ready_to_emit(self, client_id: int) -> bool:
        expected_total = self.expected_total_by_client.get(client_id)
        if expected_total is None:
            return False

        data_count = self.data_count_by_client.get(client_id, 0)
        if data_count > expected_total:
            logging.warning(
                "aggregation_data_count_exceeded | configuration=%s | id=%s | "
                "client_id=%s | data_count=%s | expected_total=%s",
                CONFIGURATION,
                ID,
                client_id,
                data_count,
                expected_total,
            )
        return data_count >= expected_total

    def _process_data_message(self, message: bytes) -> None:
        msg_type, client_id, payload = self.internal_protocol.unpack_packet(message)

        with self.lock:
            if client_id in self.closed_by_client:
                logging.info(
                    "aggregation_message_for_closed_client | configuration=%s | "
                    "id=%s | client_id=%s | msg_type=%s",
                    CONFIGURATION,
                    ID,
                    client_id,
                    msg_type,
                )
                return

        should_emit = False

        if msg_type == MessageType.DATA:
            with self.lock:
                self._processor_for_client(client_id).accept(payload)
                self.data_count_by_client[client_id] = (
                    self.data_count_by_client.get(client_id, 0) + 1
                )
                should_emit = self._ready_to_emit(client_id)

            if should_emit:
                self._emit_results(client_id)
            return

        if msg_type == MessageType.EOF:
            control_message = self.control_serializer.deserialize(payload)
            with self.lock:
                self.expected_total_by_client[client_id] = (
                    control_message.expected_total
                )
                data_count = self.data_count_by_client.get(client_id, 0)
                logging.info(
                    "aggregation_eof | configuration=%s | id=%s | "
                    "client_id=%s | sender_id=%s | data_count=%s | "
                    "expected_total=%s",
                    CONFIGURATION,
                    ID,
                    client_id,
                    control_message.sender_id,
                    data_count,
                    control_message.expected_total,
                )
                should_emit = self._ready_to_emit(client_id)

            if should_emit:
                self._emit_results(client_id)
            return

        raise ValueError(f"unsupported message type: {msg_type}")

    def process_data_messages(self, message, ack, nack):
        try:
            self._process_data_message(message)
            ack()
        except Exception as e:
            logging.error(
                "aggregation_callback_error | configuration=%s | id=%s | error=%s",
                CONFIGURATION,
                ID,
                e,
            )
            nack()

    def start(self):
        logging.info(
            "aggregation_start | configuration=%s | id=%s | exchange=%s | output_queue=%s",
            CONFIGURATION,
            ID,
            f"{AGGREGATION_PREFIX}_{ID}",
            OUTPUT_QUEUE,
        )
        try:
            self.input_exchange.start_consuming(self.process_data_messages)
        except Exception as e:
            logging.error(
                "aggregation_start_error | configuration=%s | id=%s | error=%s",
                CONFIGURATION,
                ID,
                e,
            )
        finally:
            self.handle_sigterm()
            self.close()

    def handle_sigterm(self):
        logging.info(
            "aggregation_shutdown | configuration=%s | id=%s", CONFIGURATION, ID
        )
        try:
            self.input_exchange.stop_consuming()
        except Exception as e:
            logging.warning(
                "aggregation_stop_input_error | id=%s | error=%s", ID, e
            )

    def close(self):
        if self.closed:
            return
        self.closed = True

        logging.info("Closing aggregation with id %s", ID)
        for resource in (self.input_exchange, self.output_queue):
            try:
                resource.close()
            except Exception as e:
                logging.warning(
                    "aggregation_close_error | id=%s | error=%s", ID, e
                )
