import logging
import os
import threading

from common import middleware
from common.constants import C_Q2, C_Q3, C_Q5
from common.logging_utils import should_log_progress
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

# Entrada por un exchange shardeado en {AGGREGATION_PREFIX}_{ID}; salida por una
# unica cola hacia el Join.
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]

# EOF entre N replicas (lider-gather, igual que el Sum): el Sum manda el EOF con
# el total global a cada shard, pero cada replica solo recibe ~total/N. Cada una
# reporta su conteo al lider (ID 0); cuando la suma global alcanza expected_total
# el lider ordena el flush por el control exchange y cada replica emite su shard
# + un EOF al Join. No hace falta broadcast EOF_RECEIVED porque el Sum ya entrega
# el EOF a cada shard.
LEADER_ID = 0
AGGREGATION_CONTROL_EXCHANGE = f"{AGGREGATION_PREFIX}_control"
AGGREGATION_RESPONSE_QUEUE_PREFIX = f"{AGGREGATION_PREFIX}_response"


class AggregatorWorker:
    def __init__(self):
        if CONFIGURATION not in (C_Q2, C_Q3, C_Q5):
            raise ValueError(f"Invalid aggregator configuration: {CONFIGURATION}")

        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"]
        )

        self.internal_protocol = InternalProtocol()
        self.control_serializer = ControlMessageSerializer()

        self.is_leader = ID == LEADER_ID
        self.response_queue_name = f"{AGGREGATION_RESPONSE_QUEUE_PREFIX}_{ID}"

        self.lock = threading.Lock()
        self.closed = False
        self._stopped = False

        self.processors_by_client = {}
        self.closed_by_client = set()
        self.data_count_by_client = {}
        self.expected_total_by_client = {}
        # Clientes cuyo EOF de upstream ya llego: DATA posterior se reporta como delta.
        self.pending_eof_by_client = set()

        # Gather del lider (solo ID 0).
        self.leader_processed_by_client = {}
        self.flushed_by_client = set()

        self.control_consumer = None
        self.response_consumer = None
        self.control_thread = None
        self.response_thread = None

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

    def _control_payload(self, expected_total: int, processed_count: int) -> bytes:
        return self.control_serializer.serialize(
            ControlMessage(
                sender_id=ID,
                expected_total=expected_total,
                processed_count=processed_count,
            )
        )

    # ----- thread principal (input): DATA del shard + EOF de upstream --------

    def _process_data_message(self, message: bytes) -> None:
        msg_type, client_id, payload = self.internal_protocol.unpack_packet(message)

        if msg_type == MessageType.DATA:
            self._handle_data(client_id, payload)
        elif msg_type == MessageType.EOF:
            self._handle_upstream_eof(client_id, payload)
        else:
            raise ValueError(f"unsupported message type: {msg_type}")

    def _handle_data(self, client_id: int, payload: bytes) -> None:
        report_delta = 0
        with self.lock:
            if client_id in self.closed_by_client:
                return
            self._processor_for_client(client_id).accept(payload)
            self.data_count_by_client[client_id] = (
                self.data_count_by_client.get(client_id, 0) + 1
            )
            data_count = self.data_count_by_client[client_id]
            if client_id in self.pending_eof_by_client:
                # DATA que llego despues del EOF (carrera entre el EOF del Sum y
                # los partials, que viajan por conexiones distintas): reportar el
                # delta para que el conteo del lider alcance expected_total.
                report_delta = 1

        if should_log_progress(data_count):
            logging.info(
                "aggregation_data | configuration=%s | id=%s | client_id=%s | "
                "data_count=%s | payload_bytes=%s | pending_eof=%s",
                CONFIGURATION,
                ID,
                client_id,
                data_count,
                len(payload),
                report_delta > 0,
            )

        if report_delta:
            self._report_to_leader(client_id, report_delta)

    def _handle_upstream_eof(self, client_id: int, payload: bytes) -> None:
        control_message = self.control_serializer.deserialize(payload)
        expected_total = control_message.expected_total

        snapshot = None
        with self.lock:
            if client_id in self.closed_by_client:
                return
            self.expected_total_by_client[client_id] = expected_total
            if client_id not in self.pending_eof_by_client:
                self.pending_eof_by_client.add(client_id)
                snapshot = self.data_count_by_client.get(client_id, 0)

        if snapshot is None:
            # EOF duplicado: el snapshot ya se tomo y se reporto.
            logging.info(
                "aggregation_duplicate_eof | configuration=%s | id=%s | "
                "client_id=%s | expected_total=%s",
                CONFIGURATION,
                ID,
                client_id,
                expected_total,
            )
            return

        logging.info(
            "aggregation_upstream_eof | configuration=%s | id=%s | client_id=%s | "
            "data_count=%s | expected_total=%s",
            CONFIGURATION,
            ID,
            client_id,
            snapshot,
            expected_total,
        )
        self._report_to_leader(client_id, snapshot)

    def _report_to_leader(self, client_id: int, data_count: int) -> None:
        # Conexion efimera por reporte, igual que el Sum (canales pika no se
        # comparten entre threads).
        response_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST,
            f"{AGGREGATION_RESPONSE_QUEUE_PREFIX}_{LEADER_ID}",
        )
        try:
            response_queue.send(
                self._packet(
                    MessageType.PROCESSED_ANSWER,
                    client_id,
                    self._control_payload(expected_total=0, processed_count=data_count),
                )
            )
        finally:
            response_queue.close()

    def process_data_messages(self, message, ack, nack):
        try:
            self._process_data_message(message)
            ack()
        except Exception:
            logging.exception(
                "aggregation_data_error | configuration=%s | id=%s",
                CONFIGURATION,
                ID,
            )
            nack()

    # ----- thread de response (solo lider): gather de conteos ---------------

    def _handle_leader_report(self, message, ack, nack, control_sender) -> None:
        try:
            msg_type, client_id, payload = self.internal_protocol.unpack_packet(message)
            if msg_type != MessageType.PROCESSED_ANSWER:
                raise ValueError(
                    f"unexpected aggregation response message type: {msg_type}"
                )

            control_message = self.control_serializer.deserialize(payload)
            should_flush = False
            expected_total = None

            with self.lock:
                if client_id in self.flushed_by_client:
                    ack()
                    return

                self.leader_processed_by_client[client_id] = (
                    self.leader_processed_by_client.get(client_id, 0)
                    + control_message.processed_count
                )
                expected_total = self.expected_total_by_client.get(client_id)

                if (
                    expected_total is not None
                    and self.leader_processed_by_client[client_id] >= expected_total
                ):
                    should_flush = True
                    self.flushed_by_client.add(client_id)
                    self.leader_processed_by_client.pop(client_id, None)

            if should_flush:
                logging.info(
                    "aggregation_flush_order | configuration=%s | id=%s | "
                    "client_id=%s | expected_total=%s",
                    CONFIGURATION,
                    ID,
                    client_id,
                    expected_total,
                )
                control_sender.send(
                    self._packet(
                        MessageType.FLUSH_ORDER,
                        client_id,
                        self._control_payload(expected_total=0, processed_count=0),
                    )
                )

            ack()
        except Exception:
            logging.exception(
                "aggregation_response_error | configuration=%s | id=%s",
                CONFIGURATION,
                ID,
            )
            nack()

    # ----- thread de control (todas las replicas): emision por FLUSH_ORDER ---

    def _handle_flush_order(self, message, ack, nack, output_queue) -> None:
        try:
            msg_type, client_id, payload = self.internal_protocol.unpack_packet(message)
            if msg_type != MessageType.FLUSH_ORDER:
                raise ValueError(
                    f"unexpected aggregation control message type: {msg_type}"
                )
            self._emit_results(client_id, output_queue)
            ack()
        except Exception:
            logging.exception(
                "aggregation_control_error | configuration=%s | id=%s",
                CONFIGURATION,
                ID,
            )
            nack()

    def _emit_results(self, client_id: int, output_queue) -> None:
        with self.lock:
            if client_id in self.closed_by_client:
                return
            processor = self.processors_by_client.pop(client_id, None)
            data_count = self.data_count_by_client.pop(client_id, 0)
            expected_total = self.expected_total_by_client.pop(client_id, None)
            self.pending_eof_by_client.discard(client_id)
            self.closed_by_client.add(client_id)

        payloads = processor.results() if processor is not None else []
        for payload in payloads:
            output_queue.send(self._packet(MessageType.DATA, client_id, payload))

        output_queue.send(
            self._packet(
                MessageType.EOF,
                client_id,
                self._control_payload(expected_total=len(payloads), processed_count=0),
            )
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

    # ----- consumidores / ciclo de vida -------------------------------------

    def _start_control_consumer(self) -> None:
        control_consumer = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST,
            AGGREGATION_CONTROL_EXCHANGE,
            [AGGREGATION_CONTROL_EXCHANGE],
        )
        self.control_consumer = control_consumer
        # Canal de salida propio del thread (la emision publica hacia el Join aca).
        output_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, OUTPUT_QUEUE)
        try:
            if not self._stopped:
                control_consumer.start_consuming(
                    lambda message, ack, nack: self._handle_flush_order(
                        message, ack, nack, output_queue
                    )
                )
        finally:
            output_queue.close()
            control_consumer.close()

    def _start_response_consumer(self) -> None:
        response_consumer = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST,
            self.response_queue_name,
        )
        self.response_consumer = response_consumer
        control_sender = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST,
            AGGREGATION_CONTROL_EXCHANGE,
            [AGGREGATION_CONTROL_EXCHANGE],
        )
        try:
            if not self._stopped:
                response_consumer.start_consuming(
                    lambda message, ack, nack: self._handle_leader_report(
                        message, ack, nack, control_sender
                    )
                )
        finally:
            control_sender.close()
            response_consumer.close()

    def start(self):
        logging.info(
            "aggregation_start | configuration=%s | id=%s | exchange=%s | "
            "output_queue=%s | leader=%s",
            CONFIGURATION,
            ID,
            f"{AGGREGATION_PREFIX}_{ID}",
            OUTPUT_QUEUE,
            self.is_leader,
        )

        # Todas escuchan el control exchange (FLUSH_ORDER); solo el lider consume
        # los reportes (todos reportan a {prefix}_response_0).
        self.control_thread = threading.Thread(target=self._start_control_consumer)
        self.control_thread.start()
        if self.is_leader:
            self.response_thread = threading.Thread(
                target=self._start_response_consumer
            )
            self.response_thread.start()

        try:
            if not self._stopped:
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
        self.input_exchange.request_stop_consuming()
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
            self.input_exchange.close()
        except Exception as e:
            logging.warning(
                "aggregation_close_error | id=%s | error=%s", ID, e
            )
