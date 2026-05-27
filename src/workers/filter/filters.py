import os
import json
import logging
import threading

from common import message_protocol
from common.domain.transaction import Transaction
from common import middleware
from common.constants import *

try:
    from output_batcher import OutputBatcher
except ImportError:
    from workers.filter.output_batcher import OutputBatcher

# Id correspondiente a la entidad
ID = int(os.environ["ID"])
# Host del middleware
MOM_HOST = os.environ["MOM_HOST"]
# Corresponde a como esta configurada la entidad, es decir, como filtra las transacciones
# Configuraciones posibles:
#   - "Q1": transaction.amount < 50
#   - "Q5": transaction.format == "Wire" or transaction.format == "ACH"
#   - "USD": transaction.currency == "US Dollar"
#   - "DATE": transaction.is_in_date_range(start_date, end_date)
CONFIGURATION = os.environ["CONFIGURATION"]
# Cola de Entrada
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
TRANSACTION_EXCHANGE = os.getenv("TRANSACTION_EXCHANGE")
# Colas de Salida Posibles
GATEWAY_QUEUE = os.environ["GATEWAY_QUEUE"]
FILTER_DATE_QUEUE = os.environ["FILTER_DATE_QUEUE"]
FILTER_Q1_QUEUE = os.environ["FILTER_Q1_QUEUE"]
SUM_Q2_QUEUE = os.environ["SUM_Q2_QUEUE"]
FILTER_Q3_QUEUE = os.environ["FILTER_Q3_QUEUE"]
SCATTER_GATHER_MAPPER_QUEUE = os.environ["SCATTER_GATHER_MAPPER_QUEUE"]
FILTER_Q5_USD_QUEUE = os.environ["FILTER_Q5_USD_QUEUE"]
Q3_CANDIDATES_QUEUE = os.getenv("Q3_CANDIDATES_QUEUE", FILTER_Q3_QUEUE)
# Sharding opcional de candidatos Q3 por client_id. Si Q3_CANDIDATES_EXCHANGE
# y Q3_BARRIER_AMOUNT > 1 están seteados, el filter publica a un exchange con
# routing key "{Q3_CANDIDATES_ROUTING_PREFIX}_{client_id % Q3_BARRIER_AMOUNT}"
# para distribuir candidatos entre N q3_barrier shards.
Q3_CANDIDATES_EXCHANGE = os.getenv("Q3_CANDIDATES_EXCHANGE")
Q3_CANDIDATES_ROUTING_PREFIX = os.getenv(
    "Q3_CANDIDATES_ROUTING_PREFIX", "q3_candidates"
)
Q3_BARRIER_AMOUNT = int(os.getenv("Q3_BARRIER_AMOUNT", "1"))
# Cola de salida para el sum de Q3. SUM_PREFIX se conserva para compatibilidad
# con configuraciones anteriores y para los nombres de control de Sum.
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_Q3_QUEUE = os.getenv("SUM_Q3_QUEUE", SUM_PREFIX)
# Para control en token ring
FILTER_AMOUNT = int(os.environ["FILTER_AMOUNT"])
FILTER_PREFIX = os.environ["FILTER_PREFIX"] + "_" + CONFIGURATION
CONTROL_EXCHANGE = os.environ["FILTER_PREFIX"] + "_" + "CONTROL_EXCHANGE_" + CONFIGURATION
USD_ENABLE_Q1 = os.getenv("USD_ENABLE_Q1", "1") != "0"
USD_ENABLE_Q2 = os.getenv("USD_ENABLE_Q2", "1") != "0"
USD_ENABLE_DATE = os.getenv("USD_ENABLE_DATE", "1") != "0"
DATE_ENABLE_Q3 = os.getenv("DATE_ENABLE_Q3", "1") != "0"
DATE_ENABLE_Q4 = os.getenv("DATE_ENABLE_Q4", "1") != "0"

FILTER_OUTPUT_BATCH_BYTES = int(os.getenv("FILTER_OUTPUT_BATCH_BYTES", str(1024 * 1024)))
FILTER_OUTPUT_BATCH_MAX_TX = int(os.getenv("FILTER_OUTPUT_BATCH_MAX_TX", "5000"))


class FilterWorker:
    def __init__(self):
    
        # Iniciacion de la cola de entrada
        if TRANSACTION_EXCHANGE:
            self.input_queue = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, TRANSACTION_EXCHANGE, routing_keys=[],
                exchange_type="fanout", queue_name=INPUT_QUEUE, exclusive=False,
            )
        else:
            self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
                MOM_HOST, INPUT_QUEUE
            )

        self.output_queues = self._new_output_queues()
        self.output_exchanges = []

        # Seccion de control
        # Primero Identificamos al Lider
        self.is_leader = ID == 0

        # Definicion de las keys de los exchanges
        self.personal_control_key = f"{FILTER_PREFIX}_{ID}"
        self.output_control_keys = []
        if self.is_leader:
            for i in range(1, FILTER_AMOUNT):
                self.output_control_keys.append(f"{FILTER_PREFIX}_{i}")
        else:
            self.output_control_keys.append(f"{FILTER_PREFIX}_0")

        self.control_input = None

        # Definicion del output del exchange de control
        self.control_output = self._new_control_output()

        # Serializadores para transacciones y mensajes de control
        self.transaction_serializer = message_protocol.internal.TransactionSerializer()
        self.control_serializer = message_protocol.internal.ControlMessageSerializer()
        self.internal_packet_serializer = message_protocol.internal.InternalProtocol()

        self._batcher: OutputBatcher | None = None
        if CONFIGURATION in (C_USD, C_Q5, C_DATE):
            self._batcher = OutputBatcher(
                self.transaction_serializer,
                FILTER_OUTPUT_BATCH_BYTES,
                FILTER_OUTPUT_BATCH_MAX_TX,
            )

        # Estado interno
        self.lock = threading.Lock()
        self.active = True
        self._closed = False
        self.control_thread = None

        # Procesados por cliente
        self.processed_by_client = {}
        self.forwarded_by_client = {}
        self.forwarded_by_output_by_client = {}
        self.closed_by_client = set()
        self.control_responses_by_client = {}
        self.all_processed_by_client = {}
        # Conteos forwardeados por output, agregados por el lider a partir de los
        # FLUSH_ACK de cada filtro (client_id -> {output_queue: count}). El EOF
        # downstream lleva el conteo GLOBAL por output, no el local del lider.
        self.all_forwarded_by_output_by_client = {}
        self.flushed_acks_by_client = {}
        self.first_data_logged_by_client = set()
        self.deserialized_by_client = {}
        # Solo para el caso de un unico filtro (FILTER_AMOUNT == 1): expected_total
        # de un EOF que llego pero todavia no se reenvia porque faltan procesar
        # mensajes. filter_usd es un punto de fan-in (varios file_ingestors
        # producen DATA en conexiones distintas y el EOF llega por otra), asi que
        # no se puede confiar en el orden de llegada del EOF: hay que esperar a
        # processed_by_client >= expected_total antes de cerrar.
        self.pending_eof_by_client = {}

    def _new_output_queues(self):
        output_queues = {}
        if CONFIGURATION == C_Q1:
            # Filtro Q1: la salida es el gateway para devolver datos al cliente
            output_queues[GATEWAY_QUEUE] = middleware.MessageMiddlewareQueueRabbitMQ(
                MOM_HOST, GATEWAY_QUEUE
            )
        if CONFIGURATION == C_Q5:
            # Filtro Q5_PF: la salida es la queue para el filtro Q5_USD
            output_queues[FILTER_Q5_USD_QUEUE] = middleware.MessageMiddlewareQueueRabbitMQ(
                MOM_HOST, FILTER_Q5_USD_QUEUE
            )
        if CONFIGURATION == C_USD:
            # Para el filtro de tipo de moneda, se necesitan tres colas de salida:
            #    - Una para el filtro de Q1
            #    - Una para el sum de Q2
            #    - Una para el filtro de rango de fechas
            if USD_ENABLE_Q1:
                output_queues[FILTER_Q1_QUEUE] = middleware.MessageMiddlewareQueueRabbitMQ(
                    MOM_HOST, FILTER_Q1_QUEUE
                )
            if USD_ENABLE_Q2:
                output_queues[SUM_Q2_QUEUE] = middleware.MessageMiddlewareQueueRabbitMQ(
                    MOM_HOST, SUM_Q2_QUEUE
                )
            if USD_ENABLE_DATE:
                output_queues[FILTER_DATE_QUEUE] = middleware.MessageMiddlewareQueueRabbitMQ(
                    MOM_HOST, FILTER_DATE_QUEUE
                )
        if CONFIGURATION == C_DATE:
            if DATE_ENABLE_Q4:
                output_queues[SCATTER_GATHER_MAPPER_QUEUE] = middleware.MessageMiddlewareQueueRabbitMQ(
                    MOM_HOST, SCATTER_GATHER_MAPPER_QUEUE
                )
            if DATE_ENABLE_Q3:
                output_queues[SUM_Q3_QUEUE] = middleware.MessageMiddlewareQueueRabbitMQ(
                    MOM_HOST, SUM_Q3_QUEUE
                )
                # Q3 candidates: si hay sharding configurado, publish via
                # ShardedByClientPublisher (transparente al resto del código).
                # La key del dict se mantiene en Q3_CANDIDATES_QUEUE por
                # compatibilidad con _publish_to_queue/_forward_eof.
                if Q3_CANDIDATES_EXCHANGE and Q3_BARRIER_AMOUNT > 1:
                    output_queues[Q3_CANDIDATES_QUEUE] = (
                        middleware.ShardedByClientPublisher(
                            MOM_HOST,
                            Q3_CANDIDATES_EXCHANGE,
                            Q3_CANDIDATES_ROUTING_PREFIX,
                            Q3_BARRIER_AMOUNT,
                        )
                    )
                else:
                    output_queues[Q3_CANDIDATES_QUEUE] = (
                        middleware.MessageMiddlewareQueueRabbitMQ(
                            MOM_HOST, Q3_CANDIDATES_QUEUE
                        )
                    )
        return output_queues

    def _new_control_output(self):
        return middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, CONTROL_EXCHANGE, self.output_control_keys
        )

    def _pass_eof_control_message(
            self, client_id, expected_total, control_output=None
    ):
        '''
        Envia un mensaje de control al lider indicando que se recibio un EOF para un cliente, 
        con la cantidad total de mensajes que se esperan para ese cliente
        '''
        message = self.control_serializer.serialize(
            message_protocol.internal.ControlMessage(
                sender_id=ID,
                expected_total=expected_total,
                processed_count=0
            )
        )
        message = self.internal_packet_serializer.create_packet(
            msg_type=message_protocol.internal.MessageType.EOF_RECEIVED,
            client_id_bytes=client_id.to_bytes(16, byteorder='big'),
            payload=message
        )
        (control_output or self.control_output).send(message)

    def _answer_control_message(
            self, client_id, expected_total, processed_count, control_output=None
    ):
        '''
        Responde a una solicitud de control con un mensaje indicando
        '''
        message = self.control_serializer.serialize(
            message_protocol.internal.ControlMessage(
                sender_id=ID,
                expected_total=expected_total,
                processed_count=processed_count
            )
        )
        message = self.internal_packet_serializer.create_packet(
            msg_type=message_protocol.internal.MessageType.PROCESSED_ANSWER,
            client_id_bytes=client_id.to_bytes(16, byteorder='big'),
            payload=message
        )
        (control_output or self.control_output).send(message)

    def _request_control_message(self, client_id, expected_total, control_output=None):
        '''
        Envia un mensaje de control a los workers correspondientes solicitando informacion de cuantos mensajes han procesado
        '''
        message = self.control_serializer.serialize(
            message_protocol.internal.ControlMessage(
                sender_id=ID,
                expected_total=expected_total,
                processed_count=0
            )
        )
        message = self.internal_packet_serializer.create_packet(
            msg_type=message_protocol.internal.MessageType.PROCESSED_REQUEST,
            client_id_bytes=client_id.to_bytes(16, byteorder='big'),
            payload=message
        )
        (control_output or self.control_output).send(message)

    def _flush_control_message(self, client_id, control_output=None):
        '''
        Envia un mensaje de control a los workers correspondientes 
        solicitando que liberen los recursos asociados a un cliente
        '''
        message  = self.control_serializer.serialize(
            message_protocol.internal.ControlMessage(
                sender_id=ID,
                expected_total=0,
                processed_count=0
            )
        )
        message = self.internal_packet_serializer.create_packet(
            msg_type=message_protocol.internal.MessageType.FLUSH_ORDER,
            client_id_bytes=client_id.to_bytes(16, byteorder='big'),
            payload=message
        )
        (control_output or self.control_output).send(message)

    def _ack_flush_control_message(self, client_id, forwarded_by_output, control_output=None):
        '''
        Avisa al lider que se liberaron los recursos del cliente y le reporta los
        conteos forwardeados por output de este filtro. El FLUSH_ACK lleva un
        payload JSON (no ControlMessage) porque necesita el desglose por output,
        que ControlMessage (3 enteros) no puede representar. Es interno al grupo
        de filtros, asi que no afecta el protocolo de ningun otro worker.
        '''
        payload = json.dumps(
            {"sender_id": ID, "forwarded_by_output": forwarded_by_output}
        ).encode("utf-8")
        message = self.internal_packet_serializer.create_packet(
            msg_type=message_protocol.internal.MessageType.FLUSH_ACK,
            client_id_bytes=client_id.to_bytes(16, byteorder='big'),
            payload=payload
        )
        (control_output or self.control_output).send(message)

    def _cleanup_client(self, client_id):
        '''
        Elimina toda la informacion asociada a un cliente, cerrando su procesamiento
        '''
        if self._batcher is not None:
            self._batcher.discard_client(client_id)
        with self.lock:
            if client_id in self.processed_by_client:
                del self.processed_by_client[client_id]
            if client_id in self.forwarded_by_client:
                del self.forwarded_by_client[client_id]
            if client_id in self.forwarded_by_output_by_client:
                del self.forwarded_by_output_by_client[client_id]
            if client_id in self.all_processed_by_client:
                del self.all_processed_by_client[client_id]
            if client_id in self.all_forwarded_by_output_by_client:
                del self.all_forwarded_by_output_by_client[client_id]
            if client_id in self.control_responses_by_client:
                del self.control_responses_by_client[client_id]
            if client_id in self.flushed_acks_by_client:
                del self.flushed_acks_by_client[client_id]
            self.pending_eof_by_client.pop(client_id, None)
            self.closed_by_client.add(client_id)
    
    def _record_forwarded_output(self, client_id: int, output_name: str):
        if client_id not in self.forwarded_by_output_by_client:
            self.forwarded_by_output_by_client[client_id] = {}
        if output_name not in self.forwarded_by_output_by_client[client_id]:
            self.forwarded_by_output_by_client[client_id][output_name] = 0
        self.forwarded_by_output_by_client[client_id][output_name] += 1

    def _data_packet(self, client_id: int, payload: bytes) -> bytes:
        return self.internal_packet_serializer.create_packet(
            msg_type=message_protocol.internal.MessageType.DATA,
            client_id_bytes=client_id.to_bytes(16, byteorder="big"),
            payload=payload,
        )

    def _publish_to_queue(
        self, queue_name: str, client_id: int, transaction: Transaction,
        output_queues=None,
    ) -> None:
        output_queues = output_queues or self.output_queues
        if self._batcher is not None:
            batch_payload = self._batcher.append(queue_name, client_id, transaction)
            if batch_payload is not None:
                output_queues[queue_name].send(
                    self._data_packet(client_id, batch_payload)
                )
            return
        payload = self.transaction_serializer.serialize(transaction)
        output_queues[queue_name].send(self._data_packet(client_id, payload))

    def _flush_batcher_for_client(self, client_id: int, output_queues=None) -> None:
        if self._batcher is None:
            return
        output_queues = output_queues or self.output_queues
        for queue_name, payload in self._batcher.drain_client(client_id).items():
            output_queues[queue_name].send(self._data_packet(client_id, payload))
            logging.info(
                "filter_batcher_flush | filter=%s | id=%s | client_id=%s | "
                "queue=%s | bytes=%s",
                CONFIGURATION, ID, client_id, queue_name, len(payload),
            )

    def _forward_transaction(self, transaction: Transaction, client_id: int, output_queues=None):
        '''
        Envia una transaccion a la(s) cola(s) de salida segun la configuracion.
        Devuelve True si fue forwardeada a al menos una cola.
        '''
        logging.debug(
            f"Transaction {transaction} passed filter in filter_{CONFIGURATION} "
            f"with id {ID}, forwarding to output"
        )
        sent = False
        if CONFIGURATION == C_Q1:
            self._publish_to_queue(GATEWAY_QUEUE, client_id, transaction, output_queues)
            with self.lock:
                self._record_forwarded_output(client_id, GATEWAY_QUEUE)
            sent = True
        if CONFIGURATION == C_Q5:
            self._publish_to_queue(FILTER_Q5_USD_QUEUE, client_id, transaction, output_queues)
            with self.lock:
                self._record_forwarded_output(client_id, FILTER_Q5_USD_QUEUE)
            sent = True
        if CONFIGURATION == C_USD:
            if USD_ENABLE_Q1:
                self._publish_to_queue(FILTER_Q1_QUEUE, client_id, transaction, output_queues)
                with self.lock:
                    self._record_forwarded_output(client_id, FILTER_Q1_QUEUE)
                sent = True
            if USD_ENABLE_Q2:
                self._publish_to_queue(SUM_Q2_QUEUE, client_id, transaction, output_queues)
                with self.lock:
                    self._record_forwarded_output(client_id, SUM_Q2_QUEUE)
                sent = True
            if USD_ENABLE_DATE:
                self._publish_to_queue(FILTER_DATE_QUEUE, client_id, transaction, output_queues)
                with self.lock:
                    self._record_forwarded_output(client_id, FILTER_DATE_QUEUE)
                sent = True
        if CONFIGURATION == C_DATE:
            if DATE_ENABLE_Q3 and self._filter_transaction(transaction, start_date="2022-09-01", end_date="2022-09-05"):
                self._publish_to_queue(SUM_Q3_QUEUE, client_id, transaction, output_queues)
                with self.lock:
                    self._record_forwarded_output(client_id, SUM_Q3_QUEUE)
                sent = True
            if DATE_ENABLE_Q3 and self._filter_transaction(transaction, start_date="2022-09-06", end_date="2022-09-15"):
                self._publish_to_queue(Q3_CANDIDATES_QUEUE, client_id, transaction, output_queues)
                with self.lock:
                    self._record_forwarded_output(client_id, Q3_CANDIDATES_QUEUE)
                sent = True
            if DATE_ENABLE_Q4 and self._filter_transaction(transaction, start_date="2022-09-01", end_date="2022-09-05"):
                self._publish_to_queue(SCATTER_GATHER_MAPPER_QUEUE, client_id, transaction, output_queues)
                with self.lock:
                    self._record_forwarded_output(client_id, SCATTER_GATHER_MAPPER_QUEUE)
                sent = True
        return sent

    def _forward_eof(self, client_id: int, forwarded_by_output: dict, output_queues=None):
        '''
        Envia un mensaje de EOF a la cola de salida correspondiente segun la
        configuracion del worker. Cada EOF lleva el conteo GLOBAL de transacciones
        forwardeadas a ESE output (forwarded_by_output[queue]). Para Q1/Q5/USD ese
        conteo coincide con el total (cada tx que pasa va a todos sus outputs), asi
        que el comportamiento no cambia; para DATE cada output recibe su propio
        conteo, que es lo que el sum/mapper/barrier necesitan para cerrar.
        '''
        def count(queue: str) -> int:
            return int(forwarded_by_output.get(queue, 0))
        def eof_packet(total: int):
            message = self.control_serializer.serialize(
                message_protocol.internal.ControlMessage(
                    sender_id=ID,
                    expected_total=total,
                    processed_count=0
                )
            )
            return self.internal_packet_serializer.create_packet(
                msg_type=message_protocol.internal.MessageType.EOF,
                client_id_bytes=client_id.to_bytes(16, byteorder='big'),
                payload=message
            )

        output_queues = output_queues or self.output_queues
        if CONFIGURATION == C_Q1:
            output_queues[GATEWAY_QUEUE].send(eof_packet(count(GATEWAY_QUEUE)))
        if CONFIGURATION == C_Q5:
            output_queues[FILTER_Q5_USD_QUEUE].send(eof_packet(count(FILTER_Q5_USD_QUEUE)))
        if CONFIGURATION == C_USD:
            if USD_ENABLE_Q1:
                output_queues[FILTER_Q1_QUEUE].send(eof_packet(count(FILTER_Q1_QUEUE)))
            if USD_ENABLE_Q2:
                output_queues[SUM_Q2_QUEUE].send(eof_packet(count(SUM_Q2_QUEUE)))
            if USD_ENABLE_DATE:
                output_queues[FILTER_DATE_QUEUE].send(eof_packet(count(FILTER_DATE_QUEUE)))
        if CONFIGURATION == C_DATE:
            if DATE_ENABLE_Q3:
                output_queues[SUM_Q3_QUEUE].send(eof_packet(count(SUM_Q3_QUEUE)))
                output_queues[Q3_CANDIDATES_QUEUE].send(eof_packet(count(Q3_CANDIDATES_QUEUE)))
            if DATE_ENABLE_Q4:
                output_queues[SCATTER_GATHER_MAPPER_QUEUE].send(
                    eof_packet(count(SCATTER_GATHER_MAPPER_QUEUE))
                )

    
    def _filter_transaction(self, transaction: Transaction, start_date=None, end_date=None):
        '''
        Aplica el filtro correspondiente a la configuracion de este worker a una transaccion,
        devolviendo True si la transaccion pasa el filtro y False en caso contrario
        '''
        if CONFIGURATION == C_Q1:
            return transaction < 50
        if CONFIGURATION == C_Q5:
            return transaction.format == "Wire" or transaction.format == "ACH"
        if CONFIGURATION == C_USD:
            return transaction.currency == "US Dollar"
        if CONFIGURATION == C_DATE:
            if start_date is None or end_date is None:
                return True
            tx_date = transaction.date[:10].replace("/", "-")
            return start_date <= tx_date <= end_date
        raise ValueError(f"Invalid configuration: {CONFIGURATION}")
    
    
    def _process_data_message(self, message):
        '''
        Procesa un mensaje de la cola de entrada, aplicando el filtro correspondiente a la
        configuracion de este worker y reenviando la transaccion a la cola de salida correspondiente 
        si la transaccion pasa el filtro
        '''
        msg_type, client_id, payload = self.internal_packet_serializer.unpack_packet(message)

        with self.lock:
            if client_id in self.closed_by_client:
                # Si el cliente ya fue cerrado, no procesamos mas mensajes de ese cliente
                logging.info(f"Received message for closed client {client_id} in filter_{CONFIGURATION} with id {ID}, ignoring")
                return

        if msg_type == message_protocol.internal.MessageType.DATA:
            transactions = self.transaction_serializer.deserialize_batch(payload)
            if not transactions:
                return

            with self.lock:
                if client_id not in self.first_data_logged_by_client:
                    self.first_data_logged_by_client.add(client_id)
                    logging.info(
                        "filter_first_chunk_received | filter=%s | id=%s | "
                        "client_id=%s | message_bytes=%s | payload_bytes=%s | "
                        "batch_size=%s",
                        CONFIGURATION,
                        ID,
                        client_id,
                        len(message),
                        len(payload),
                        len(transactions),
                    )
                prev_deserialized = self.deserialized_by_client.get(client_id, 0)
                self.deserialized_by_client[client_id] = (
                    prev_deserialized + len(transactions)
                )

            for offset, transaction in enumerate(transactions, start=1):
                tx_number = prev_deserialized + offset
                if tx_number <= 3:
                    logging.info(
                        "filter_transaction_deserialized | filter=%s | id=%s | "
                        "client_id=%s | transaction_number=%s | date=%s | "
                        "from_bank=%s | from_account=%s | to_bank=%s | "
                        "to_account=%s | amount=%s | currency=%s | format=%s",
                        CONFIGURATION,
                        ID,
                        client_id,
                        tx_number,
                        transaction.date,
                        transaction.from_bank,
                        transaction.from_account,
                        transaction.to_bank,
                        transaction.to_account,
                        transaction.amount,
                        transaction.currency,
                        transaction.format,
                    )
                if tx_number == 3:
                    logging.info(
                        "==================== Forward pass successful - Mate | "
                        "filter=%s | id=%s | client_id=%s | "
                        "transactions_deserialized=%s ====================",
                        CONFIGURATION,
                        ID,
                        client_id,
                        tx_number,
                    )

            forwarded_in_batch = 0
            for transaction in transactions:
                if CONFIGURATION == C_DATE or self._filter_transaction(transaction):
                    if self._forward_transaction(transaction, client_id):
                        forwarded_in_batch += 1

            with self.lock:
                self.forwarded_by_client[client_id] = (
                    self.forwarded_by_client.get(client_id, 0) + forwarded_in_batch
                )
                self.processed_by_client[client_id] = (
                    self.processed_by_client.get(client_id, 0) + len(transactions)
                )

            # Si habia un EOF pendiente (caso un unico filtro), intentar cerrarlo
            # ahora que se procesaron mensajes adicionales.
            if FILTER_AMOUNT == 1:
                self._try_forward_single_filter_eof(client_id)

        elif msg_type == message_protocol.internal.MessageType.EOF:
            # Cuando recibimos un EOF:
            # - Si se es el lider, se le avisa a los demas workers que se recibio un EOF 
            #     para este cliente y cuantos mensajes se esperan en total, para que ellos puedan 
            #     responder con su conteo de procesados
            # - Si no se es el lider, se le avisa al lider que se recibio un EOF para este cliente y 
            #     cuantos mensajes se esperan en total, para que el lider pueda solicitar el 
            #     conteo de procesados a los demas workers            
            control_message = self.control_serializer.deserialize(payload)
            expected_total = control_message.expected_total

            logging.info(f"Received EOF for client {client_id} in filter_{CONFIGURATION}. Expected total: {expected_total}.")
            
            if self.is_leader and FILTER_AMOUNT == 1:
                # No cerrar al llegar el EOF: registrar el expected_total y cerrar
                # recien cuando se hayan procesado todos los mensajes esperados
                # (pueden seguir llegando DATA de file_ingestors mas lentos).
                with self.lock:
                    self.pending_eof_by_client[client_id] = expected_total
                self._try_forward_single_filter_eof(client_id)
            elif self.is_leader:
                self._request_control_message(client_id, expected_total)
            else:
                self._pass_eof_control_message(client_id, expected_total)
        
        else:
            logging.warning(f"Received unknown message type: {msg_type} for filter_{CONFIGURATION}")

    def _try_forward_single_filter_eof(self, client_id):
        '''
        Caso FILTER_AMOUNT == 1: reenvia el EOF y limpia el cliente solo cuando ya
        se procesaron todos los mensajes esperados (processed_count >= expected_total).
        Usa >= (no ==) para que un mensaje reentregado no impida nunca el cierre.
        No bloquea: se invoca tras cada DATA y al recibir el EOF.
        '''
        with self.lock:
            expected_total = self.pending_eof_by_client.get(client_id)
            if expected_total is None:
                return
            if self.processed_by_client.get(client_id, 0) < expected_total:
                return
            # Filtro unico: sus conteos por output ya son el total global.
            forwarded_by_output = dict(self.forwarded_by_output_by_client.get(client_id, {}))
            del self.pending_eof_by_client[client_id]

        self._flush_batcher_for_client(client_id)

        logging.info(
            f"All {expected_total} expected messages processed for client {client_id} "
            f"in filter_{CONFIGURATION}, forwarding EOF (forwarded_by_output={forwarded_by_output})"
        )
        self._forward_eof(client_id, forwarded_by_output)
        self._cleanup_client(client_id)

    def _process_control_message(self, message, control_output=None, output_queues=None):
        '''
        Procesa un mensaje de control recibido por el exchange de control, actualizando el estado interno del worker
        y respondiendo a los mensajes de control correspondientes
        '''
        # Desempaquetamos el mensaje
        msg_type, client_id, payload = self.internal_packet_serializer.unpack_packet(message)

        # El FLUSH_ACK lleva payload JSON (desglose por output), no ControlMessage:
        # se maneja antes del deserialize comun para no romperlo.
        if msg_type == message_protocol.internal.MessageType.FLUSH_ACK:
            self._handle_flush_ack(client_id, payload, output_queues)
            return

        control_message = self.control_serializer.deserialize(payload)

        if msg_type == message_protocol.internal.MessageType.EOF_RECEIVED:
            if not self.is_leader:
                logging.warning(f"Received EOF_RECEIVED control message from worker {control_message.sender_id} for client {client_id} in filter_{CONFIGURATION}, but I am not the leader, ignoring")
                return
            # Si se recibe un mensaje indicando que se recibio un EOF para un cliente, se responde con una solicitud de conteo de procesados para ese cliente
            self._request_control_message(
                client_id, control_message.expected_total, control_output
            )

        elif msg_type == message_protocol.internal.MessageType.PROCESSED_REQUEST:
            if self.is_leader:
                logging.warning(f"Received PROCESSED_REQUEST control message from worker {control_message.sender_id} for client {client_id} in filter_{CONFIGURATION}, but I am the leader, ignoring")
                return
            # Si se recibe una solicitud de conteo procesados, se responde con un mensaje indicando
            # cuantos mensajes se han procesado para ese cliente
            with self.lock:
                processed_count = self.processed_by_client.get(client_id, 0)
            self._answer_control_message(
                client_id,
                control_message.expected_total,
                processed_count,
                control_output,
            )
        
        elif msg_type == message_protocol.internal.MessageType.PROCESSED_ANSWER:
            if not self.is_leader:
                logging.warning(f"Received PROCESSED_ANSWER control message from worker {control_message.sender_id} for client {client_id} in filter_{CONFIGURATION}, but I am not the leader, ignoring")
                return
            
            with self.lock:
                procesados = self.processed_by_client.get(client_id, 0)

            if client_id not in self.control_responses_by_client:
                self.control_responses_by_client[client_id] = set()
            self.control_responses_by_client[client_id].add(control_message.sender_id)
            
            if client_id not in self.all_processed_by_client:
                self.all_processed_by_client[client_id] = 0
            self.all_processed_by_client[client_id] += control_message.processed_count
            
            if len(self.control_responses_by_client[client_id]) == FILTER_AMOUNT - 1:
                if self.all_processed_by_client[client_id] + procesados == control_message.expected_total:
                    logging.info(f"Received all PROCESSED_ANSWER control messages for client {client_id} in filter_{CONFIGURATION}, total processed: {procesados + control_message.processed_count}, expected: {control_message.expected_total}, sending FLUSH_ORDER")
                    self._flush_control_message(client_id, control_output)
                else:
                    logging.info(f"Received all PROCESSED_ANSWER control messages for client {client_id} in filter_{CONFIGURATION}, total processed: {procesados + control_message.processed_count}, expected: {control_message.expected_total}, but counts do not match, resending PROCESSED_REQUEST")
                    self._request_control_message(
                        client_id, control_message.expected_total, control_output
                    )
                    self.control_responses_by_client[client_id] = set()
                    self.all_processed_by_client[client_id] = 0
        
        elif msg_type == message_protocol.internal.MessageType.FLUSH_ORDER:
            if self.is_leader:
                logging.warning(f"Received FLUSH_ORDER control message from worker {control_message.sender_id} for client {client_id} in filter_{CONFIGURATION}, but I am the leader, ignoring")
                return
            self._flush_batcher_for_client(client_id, output_queues)
            with self.lock:
                forwarded_by_output = dict(self.forwarded_by_output_by_client.get(client_id, {}))
            self._cleanup_client(client_id)
            self._ack_flush_control_message(client_id, forwarded_by_output, control_output)

        else:
            logging.warning(f"Received unknown control message type: {msg_type} from worker {control_message.sender_id} for client {client_id} in filter_{CONFIGURATION}")

    def _handle_flush_ack(self, client_id, payload, output_queues=None):
        '''
        El lider agrega los conteos por output de cada FLUSH_ACK. Cuando recibio
        el de todos los filtros, combina con los propios y reenvia el EOF
        downstream con el conteo GLOBAL por output.
        '''
        if not self.is_leader:
            logging.warning(f"Received FLUSH_ACK control message for client {client_id} in filter_{CONFIGURATION}, but I am not the leader, ignoring")
            return

        data = json.loads(payload.decode("utf-8"))
        sender_id = data.get("sender_id")
        counts = data.get("forwarded_by_output", {})

        if client_id not in self.flushed_acks_by_client:
            self.flushed_acks_by_client[client_id] = set()
        self.flushed_acks_by_client[client_id].add(sender_id)

        agg = self.all_forwarded_by_output_by_client.setdefault(client_id, {})
        for output, value in counts.items():
            agg[output] = agg.get(output, 0) + int(value)

        if len(self.flushed_acks_by_client[client_id]) == FILTER_AMOUNT - 1:
            logging.info(f"Received all FLUSH_ACK control messages for client {client_id} in filter_{CONFIGURATION}, forwarding EOF")
            self._flush_batcher_for_client(client_id, output_queues)
            global_counts = dict(agg)
            with self.lock:
                for output, value in self.forwarded_by_output_by_client.get(client_id, {}).items():
                    global_counts[output] = global_counts.get(output, 0) + value
            self._forward_eof(client_id, global_counts, output_queues)
            self._cleanup_client(client_id)

    def process_data_messages(self, message, ack, nack):
        '''
        Callback para procesar los mensajes recibidos por la cola de entrada
        '''
        try:
            self._process_data_message(message)
            ack()
        except Exception as e:
            logging.error(f"Error processing data message in filter_{CONFIGURATION} with id {ID}: {e}")
            nack()

    def _start_control_consumer(self):
        control_input = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, CONTROL_EXCHANGE, [self.personal_control_key]
        )
        self.control_input = control_input
        control_output = self._new_control_output()
        output_queues = self._new_output_queues()
        try:
            if self.active:
                control_input.start_consuming(
                    lambda message, ack, nack: self._process_control_message_with_publishers(
                        message, ack, nack, control_output, output_queues
                    )
                )
        finally:
            try:
                control_input.close()
            except Exception as e:
                logging.error(
                    f"Error closing control input in filter_{CONFIGURATION} with id {ID}: {e}"
                )
            self._close_control_thread_publishers(control_output, output_queues)

    def _process_control_message_with_publishers(
        self, message, ack, nack, control_output, output_queues
    ):
        try:
            self._process_control_message(message, control_output, output_queues)
            ack()
        except Exception as e:
            logging.error(
                f"Error processing control message in filter_{CONFIGURATION} "
                f"with id {ID}: {e}"
            )
            nack()

    def _close_control_thread_publishers(self, control_output, output_queues):
        try:
            control_output.close()
        except Exception as e:
            logging.error(f"Error closing control thread output in filter_{CONFIGURATION} with id {ID}: {e}")
        for queue in output_queues.values():
            try:
                queue.close()
            except Exception as e:
                logging.error(f"Error closing control thread queue in filter_{CONFIGURATION} with id {ID}: {e}")

    def start(self):
        '''
        Inicia el procesamiento de mensajes de la cola de entrada y del exchange de control
        '''
        # Se inicia un thread para procesar los mensajes de control
        self.control_thread = threading.Thread(target=self._start_control_consumer)
        self.control_thread.start()

        try:
            if self.active:
                self.input_queue.start_consuming(self.process_data_messages)
        except Exception as e:
            logging.error(f"Error in filter_{CONFIGURATION} with id {ID}: {e}")
        finally:
            self.handle_sigterm()
            if self.control_thread is not None:
                self.control_thread.join(timeout=5)
            self.close()

    def handle_sigterm(self):
        '''
        Maneja la señal de terminacion.
        '''
        if not self.active:
            return
        logging.info(f"Received SIGTERM in filter_{CONFIGURATION} with id {ID}, shutting down")
        self.active = False
        self.input_queue.request_stop_consuming()
        if self.control_input is not None:
            self.control_input.request_stop_consuming()

    def close(self):
        '''
        Cierra los recursos utilizados por este worker.
        '''
        if self._closed:
            return
        self._closed = True
        logging.info(f"Closing filter_{CONFIGURATION} with id {ID}")

        try:
            self.input_queue.close()
        except Exception as e:
            logging.error(f"Error closing input queue in filter_{CONFIGURATION} with id {ID}: {e}")

        try:
            self.control_output.close()
        except Exception as e:
            logging.error(f"Error closing control output in filter_{CONFIGURATION} with id {ID}: {e}")

        for queue in self.output_queues.values():
            try:
                queue.close()
            except Exception as e:
                logging.error(f"Error closing output queue in filter_{CONFIGURATION} with id {ID}: {e}")

        for exchange in self.output_exchanges:
            try:
                exchange.close()
            except Exception as e:
                logging.error(f"Error closing output exchange in filter_{CONFIGURATION} with id {ID}: {e}")
