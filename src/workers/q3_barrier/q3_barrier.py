import logging
import os
import tempfile
import threading

from common import middleware
from common.logging_utils import should_log_progress
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

# Sharding: cuando hay >1 barriers, los publishers (joiner_q3 y filter_date)
# enrutan por client_id a un exchange. Cada barrier crea su propia queue
# bindeada al routing key "{prefix}_{ID}" para recibir solo sus clientes.
Q3_BARRIER_AMOUNT = int(os.getenv("Q3_BARRIER_AMOUNT", "1"))
Q3_AVERAGES_EXCHANGE = os.getenv("Q3_AVERAGES_EXCHANGE")
Q3_CANDIDATES_EXCHANGE = os.getenv("Q3_CANDIDATES_EXCHANGE")
Q3_AVERAGES_ROUTING_PREFIX = os.getenv(
    "Q3_AVERAGES_ROUTING_PREFIX", "q3_averages"
)
Q3_CANDIDATES_ROUTING_PREFIX = os.getenv(
    "Q3_CANDIDATES_ROUTING_PREFIX", "q3_candidates"
)
Q3_OUTPUT_BATCH_BYTES = int(os.getenv("Q3_OUTPUT_BATCH_BYTES", str(1024 * 1024)))
Q3_OUTPUT_BATCH_MAX_TX = int(os.getenv("Q3_OUTPUT_BATCH_MAX_TX", "5000"))

_RECORD_LEN_SIZE = 4  # bytes para el prefijo de longitud (big-endian)


class _ClientDiskLog:
    """
    Append-only log en disco para candidatos Q3.
    Formato: [4B length][payload_bytes]* — un registro por mensaje RabbitMQ.

    El log NO desearializa en escritura: guarda el payload crudo (batch de
    transacciones ya serializado por el filter). La desearialización ocurre
    una sola vez al momento del emit, vía iteración secuencial con generador
    (O(1) RAM durante el join, vs O(right_side) si se hiciera list()).
    """

    def __init__(self) -> None:
        # TemporaryFile: el OS elimina el archivo cuando se cierra o cuando
        # el proceso muere. No requiere cleanup explícito en path de crash.
        self._file = tempfile.TemporaryFile()
        self._batch_count = 0
        self._byte_count = 0

    def append(self, raw_batch_payload: bytes) -> None:
        """Escribe un batch de transacciones ya serializado. O(1) amortizado."""
        n = len(raw_batch_payload)
        self._file.write(n.to_bytes(_RECORD_LEN_SIZE, "big"))
        self._file.write(raw_batch_payload)
        self._batch_count += 1
        self._byte_count += n

    def iter_raw_batches(self):
        """
        Iterador secuencial desde el inicio. Yields raw payload bytes por mensaje.
        Mantiene O(1) RAM: el OS hace readahead del archivo, nunca se carga
        todo el contenido a memoria.
        """
        self._file.seek(0)
        while True:
            header = self._file.read(_RECORD_LEN_SIZE)
            if not header:
                return
            n = int.from_bytes(header, "big")
            yield self._file.read(n)

    @property
    def batch_count(self) -> int:
        return self._batch_count

    @property
    def byte_count(self) -> int:
        return self._byte_count

    def close(self) -> None:
        try:
            self._file.close()  # TemporaryFile se elimina del OS automáticamente
        except Exception:
            pass


class _ClientState:
    def __init__(self) -> None:
        self.averages: dict[str, float] = {}
        self.avg_eof = False
        self.candidates_eof = False
        self.candidates_expected_total: int | None = None
        self.disk_log = _ClientDiskLog()

    def close(self) -> None:
        self.disk_log.close()


class Q3BarrierWorker:
    def __init__(self):
        self.averages_input = self._build_input(
            exchange_name=Q3_AVERAGES_EXCHANGE,
            routing_prefix=Q3_AVERAGES_ROUTING_PREFIX,
            fallback_queue=AVERAGES_QUEUE,
            queue_name=f"{Q3_AVERAGES_ROUTING_PREFIX}_{ID}",
        )
        self.candidates_input = self._build_input(
            exchange_name=Q3_CANDIDATES_EXCHANGE,
            routing_prefix=Q3_CANDIDATES_ROUTING_PREFIX,
            fallback_queue=CANDIDATES_QUEUE,
            queue_name=f"{Q3_CANDIDATES_ROUTING_PREFIX}_{ID}",
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
        self._stopped = False

    def _build_input(
        self,
        exchange_name: str | None,
        routing_prefix: str,
        fallback_queue: str,
        queue_name: str,
    ):
        """
        Si hay sharding (exchange seteado y Q3_BARRIER_AMOUNT>1), crea una
        queue bindeada al exchange con routing key "{prefix}_{ID}". Cada barrier
        recibe solo los clientes con client_id % N == ID.

        Sin sharding (Q3_BARRIER_AMOUNT==1 o sin exchange), consume directo de
        la queue legacy compartida — compatibilidad con presets de 1 barrier.
        """
        if exchange_name and Q3_BARRIER_AMOUNT > 1:
            return middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST,
                exchange_name,
                routing_keys=[f"{routing_prefix}_{ID}"],
                queue_name=queue_name,
                exclusive=False,
            )
        return middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, fallback_queue
        )

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
                average_count = len(state.averages)
                if should_log_progress(average_count):
                    logging.info(
                        "q3_barrier_average_data | id=%s | client_id=%s | "
                        "averages=%s | payment_format=%s | payload_bytes=%s",
                        ID,
                        client_id,
                        average_count,
                        result.payment_format,
                        len(payload),
                    )
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
                # Spill al disco sin desearializar: el payload es un batch
                # serializado por el filter; lo guardamos crudo y desearializamos
                # solo al momento del emit.
                state.disk_log.append(payload)
                if should_log_progress(state.disk_log.batch_count):
                    logging.info(
                        "q3_barrier_candidate_data | id=%s | client_id=%s | "
                        "batches_on_disk=%s | disk_bytes=%s | payload_bytes=%s",
                        ID,
                        client_id,
                        state.disk_log.batch_count,
                        state.disk_log.byte_count,
                        len(payload),
                    )
                return False
            if msg_type == MessageType.EOF:
                control = self.control_serializer.deserialize(payload)
                state.candidates_eof = True
                state.candidates_expected_total = control.expected_total
                logging.info(
                    "q3_barrier_candidates_eof | id=%s | client_id=%s | "
                    "batches_on_disk=%s | disk_bytes=%s | expected_total=%s",
                    ID, client_id,
                    state.disk_log.batch_count,
                    state.disk_log.byte_count,
                    control.expected_total,
                )
                return self._ready_locked(client_id)
        raise ValueError(f"unsupported Q3 candidates message type: {msg_type}")

    def _ready_locked(self, client_id: int) -> bool:
        state = self.states_by_client.get(client_id)
        if state is None:
            return False
        return state.avg_eof and state.candidates_eof

    def _send_output_batch(self, client_id: int, output_queue, batch) -> bool:
        if not batch:
            return False
        output_queue.send(
            self._packet(
                MessageType.DATA,
                client_id,
                self.transaction_serializer.serialize_batch(batch),
            )
        )
        return True

    def _emit_client(self, client_id: int, output_queue) -> None:
        with self.lock:
            state = self.states_by_client.pop(client_id, None)
            if state is None or client_id in self.closed_by_client:
                return
            self.closed_by_client.add(client_id)

        emitted = 0
        publishes = 0
        batch = []
        batch_bytes = 0
        disk_batches = state.disk_log.batch_count
        disk_bytes = state.disk_log.byte_count

        try:
            # Lectura secuencial del log en disco vía generator: O(1) RAM.
            # Nunca se hace list() del right side — la única RAM viva es el
            # batch de output que se va acumulando hasta flush.
            for raw_batch in state.disk_log.iter_raw_batches():
                for tx in self.transaction_serializer.deserialize_batch(raw_batch):
                    avg = state.averages.get(tx.format)
                    if avg is None:
                        continue
                    if tx.amount < (avg / THRESHOLD_DIVISOR):
                        batch.append(tx)
                        batch_bytes += TransactionSerializer.SIZE
                        emitted += 1

                        if (
                            len(batch) >= Q3_OUTPUT_BATCH_MAX_TX
                            or batch_bytes >= Q3_OUTPUT_BATCH_BYTES
                        ):
                            publishes += int(
                                self._send_output_batch(
                                    client_id, output_queue, batch
                                )
                            )
                            batch = []
                            batch_bytes = 0

            publishes += int(
                self._send_output_batch(client_id, output_queue, batch)
            )
        finally:
            # Cierra el TemporaryFile → el OS elimina el archivo del disco.
            state.close()

        control_payload = self.control_serializer.serialize(
            ControlMessage(sender_id=ID, expected_total=emitted, processed_count=0)
        )
        output_queue.send(
            self._packet(MessageType.EOF, client_id, control_payload)
        )
        logging.info(
            "q3_barrier_emit | id=%s | client_id=%s | "
            "disk_batches=%s | disk_bytes=%s | averages=%s | results=%s | "
            "publishes=%s",
            ID, client_id, disk_batches, disk_bytes,
            len(state.averages), emitted, publishes,
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
            if not self._stopped:
                self.candidates_input.start_consuming(
                    lambda message, ack, nack: self._handle_candidate(
                        message, ack, nack, output_queue
                    )
                )
        finally:
            # Este thread cierra las conexiones que consume.
            try:
                self.candidates_input.close()
            except Exception:
                pass
            try:
                output_queue.close()
            except Exception:
                pass

    def start(self):
        logging.info(
            "q3_barrier_start | id=%s | averages_queue=%s | candidates_queue=%s | "
            "output_queue=%s",
            ID, AVERAGES_QUEUE, CANDIDATES_QUEUE, OUTPUT_QUEUE,
        )
        self.candidates_thread = threading.Thread(target=self._consume_candidates)
        self.candidates_thread.start()
        try:
            if not self._stopped:
                self.averages_input.start_consuming(self._handle_average)
        finally:
            self.handle_sigterm()
            if self.candidates_thread is not None:
                self.candidates_thread.join(timeout=5)
            self.close()

    def handle_sigterm(self):
        if self._stopped:
            return
        self._stopped = True
        logging.info("q3_barrier_shutdown | id=%s", ID)
        self.averages_input.request_stop_consuming()
        self.candidates_input.request_stop_consuming()

    def close(self):
        if self.closed:
            return
        self.closed = True

        # candidates_input la cierra su propio thread.
        for resource in (self.averages_input, self.averages_output_queue):
            try:
                resource.close()
            except Exception as e:
                logging.warning(
                    "q3_barrier_close_error | id=%s | error=%s", ID, e
                )

        # Cleanup de archivos temporales de clientes que quedaron a medio procesar
        # (caso de SIGTERM antes de emit). TemporaryFile también se limpia solo
        # cuando el proceso muere, pero cerramos explícitamente para liberar fds.
        with self.lock:
            pending = list(self.states_by_client.values())
            self.states_by_client.clear()
        for state in pending:
            try:
                state.close()
            except Exception as e:
                logging.warning(
                    "q3_barrier_state_close_error | id=%s | error=%s", ID, e
                )

        for resource in (
            self.averages_input,
            self.candidates_input,
            self.averages_output_queue,
        ):
            try:
                resource.close()
            except Exception as e:
                logging.warning("q3_barrier_close_error | id=%s | error=%s", ID, e)
