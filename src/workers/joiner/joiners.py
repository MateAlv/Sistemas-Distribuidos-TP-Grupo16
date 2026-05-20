import logging
import os
import threading
import time

from common import middleware
from common.constants import C_Q2, C_Q3, C_Q5
from common.message_protocol.common import MessageType
from common.message_protocol.common.control_message import ControlMessage
from common.message_protocol.control_message_serializer import ControlMessageSerializer
from common.message_protocol.internal import InternalProtocol

try:
    from processors import create_joiner_processor
except ImportError:
    from workers.joiner.processors import create_joiner_processor


ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
CONFIGURATION = os.environ["CONFIGURATION"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
MAX_CLIENTS = 500  



class JoinerWorker:
    def __init__(self):
        if CONFIGURATION not in (C_Q2, C_Q3, C_Q5):
            raise ValueError(f"Invalid joiner configuration: {CONFIGURATION}")

        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_resource = self._build_output(CONFIGURATION)

        self.internal_protocol = InternalProtocol()
        self.control_serializer = ControlMessageSerializer()

        self.lock = threading.Lock()
        self.closed = False
        self.processors_by_client = {}
        self.eof_count_by_client = {}
        self.closed_by_client = {}  # client_id -> close_timestamp
        self._max_closed_clients = MAX_CLIENTS  # Limpiar cuando exceda este límite

    def _processor_for_client(self, client_id: int):
        return self.processors_by_client.setdefault(
            client_id,
            create_joiner_processor(CONFIGURATION),
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
            self.eof_count_by_client.pop(client_id, None)
            self.closed_by_client[client_id] = time.monotonic()
            
            # Limpiar entries antiguas si excedemos el límite
            if len(self.closed_by_client) > self._max_closed_clients:
                self._cleanup_closed_clients()

        payloads = processor.results() if processor is not None else []
        for payload in payloads:
            self.output_resource.send(
                self._packet(MessageType.DATA, client_id, payload)
            )

        control_payload = self.control_serializer.serialize(
            ControlMessage(
                sender_id=ID, expected_total=len(payloads), processed_count=0
            )
        )
        self.output_resource.send(
            self._packet(MessageType.EOF, client_id, control_payload)
        )
        logging.info(
            "joiner_emit | configuration=%s | id=%s | client_id=%s | results=%s",
            CONFIGURATION, ID, client_id, len(payloads),
        )

    def _cleanup_closed_clients(self) -> None:
        """Limpia clientes cerrados más antiguos cuando excede el límite (debe llamarse dentro del lock)."""
        # Ordenar por timestamp y mantener solo los más recientes
        sorted_clients = sorted(self.closed_by_client.items(), key=lambda x: x[1])
        to_remove = len(sorted_clients) - self._max_closed_clients // 2
        for client_id, _ in sorted_clients[:to_remove]:
            self.closed_by_client.pop(client_id, None)
        logging.debug(
            "joiner_cleanup | configuration=%s | id=%s | removed=%s | remaining=%s",
            CONFIGURATION, ID, to_remove, len(self.closed_by_client),
        )

    def _build_output(self, configuration: str):
        if configuration == C_Q3:
            return middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, OUTPUT_QUEUE, routing_keys=[], exchange_type="fanout"
            )
        return middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, OUTPUT_QUEUE)

    def _process_message(self, message: bytes) -> None:
        msg_type, client_id, payload = self.internal_protocol.unpack_packet(message)

        with self.lock:
            if client_id in self.closed_by_client:
                logging.info(
                    "joiner_message_for_closed_client | configuration=%s | "
                    "id=%s | client_id=%s | msg_type=%s",
                    CONFIGURATION, ID, client_id, msg_type,
                )
                return

            if msg_type == MessageType.DATA:
                self._processor_for_client(client_id).accept(payload)
                return

            if msg_type == MessageType.EOF:
                count = self.eof_count_by_client.get(client_id, 0) + 1
                self.eof_count_by_client[client_id] = count
                should_emit = count == AGGREGATION_AMOUNT
            else:
                raise ValueError(f"unsupported message type: {msg_type}")

        # Emitir fuera del lock para no bloquear otros mensajes
        if msg_type == MessageType.EOF and should_emit:
            self._emit_results(client_id)

    def process_messages(self, message, ack, nack):
        try:
            self._process_message(message)
            ack()
        except Exception as e:
            logging.error(
                "joiner_callback_error | configuration=%s | id=%s | error=%s",
                CONFIGURATION, ID, e,
            )
            nack()

    def start(self):
        logging.info(
            "joiner_start | configuration=%s | id=%s | input=%s | output=%s | "
            "aggregation_amount=%s",
            CONFIGURATION, ID, INPUT_QUEUE, OUTPUT_QUEUE, AGGREGATION_AMOUNT,
        )
        try:
            self.input_queue.start_consuming(self.process_messages)
        except Exception as e:
            logging.error(
                "joiner_start_error | configuration=%s | id=%s | error=%s",
                CONFIGURATION, ID, e,
            )
        finally:
            self.handle_sigterm()
            self.close()

    def handle_sigterm(self):
        logging.info(
            "joiner_shutdown | configuration=%s | id=%s", CONFIGURATION, ID
        )
        try:
            self.input_queue.stop_consuming()
        except Exception as e:
            logging.warning(
                "joiner_stop_input_error | id=%s | error=%s", ID, e
            )

    def close(self):
        if self.closed:
            return
        self.closed = True
        logging.info("joiner_close | configuration=%s | id=%s", CONFIGURATION, ID)
        for resource in (self.input_queue, self.output_resource):
            try:
                resource.close()
            except Exception as e:
                logging.warning(
                    "joiner_close_error | id=%s | error=%s", ID, e
                )
