import os
import logging
import threading

from common import message_protocol
from common.domain.transaction import Transaction
from common import middleware
from common.constants import *

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
# Cola de salida para el sum de Q3. SUM_PREFIX se conserva para compatibilidad
# con configuraciones anteriores y para los nombres de control de Sum.
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_Q3_QUEUE = os.getenv("SUM_Q3_QUEUE", SUM_PREFIX)
# Para control en token ring
FILTER_AMOUNT = int(os.environ["FILTER_AMOUNT"])
FILTER_PREFIX = os.environ["FILTER_PREFIX"] + "_" + CONFIGURATION
CONTROL_EXCHANGE = os.environ["FILTER_PREFIX"] + "_" + "CONTROL_EXCHANGE_" + CONFIGURATION

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

        # Output Queue o Exchange dependiendo de la configuracion
        self.output_queues = {}
        self.output_exchanges = []
        if CONFIGURATION == C_Q1:
            # Filtro Q1: la salida es el gateway para devolver datos al cliente
            self.output_queues[GATEWAY_QUEUE] = middleware.MessageMiddlewareQueueRabbitMQ(
                MOM_HOST, GATEWAY_QUEUE
            )
        if CONFIGURATION == C_Q5:
            # Filtro Q5_PF: la salida es la queue para el filtro Q5_USD
            self.output_queues[FILTER_Q5_USD_QUEUE] = middleware.MessageMiddlewareQueueRabbitMQ(
                MOM_HOST, FILTER_Q5_USD_QUEUE
            )
        if CONFIGURATION == C_USD:
            # Para el filtro de tipo de moneda, se necesitan tres colas de salida:
            #    - Una para el filtro de Q1
            #    - Una para el sum de Q2
            #    - Una para el filtro de rango de fechas
            self.output_queues[FILTER_Q1_QUEUE] = middleware.MessageMiddlewareQueueRabbitMQ(
                MOM_HOST, FILTER_Q1_QUEUE
            )
            self.output_queues[SUM_Q2_QUEUE] = middleware.MessageMiddlewareQueueRabbitMQ(
                MOM_HOST, SUM_Q2_QUEUE
            )
            self.output_queues[FILTER_DATE_QUEUE] = middleware.MessageMiddlewareQueueRabbitMQ(
                MOM_HOST, FILTER_DATE_QUEUE
            )
        if CONFIGURATION == C_DATE:
            # Para el filtro de fecha, se necesitan tres colas de salida:
            #    - Una para el filtro de Q3
            #    - Una para el scatter gather mapper de Q4
            #    - Una para el sum de Q3
            self.output_queues[FILTER_Q3_QUEUE] = middleware.MessageMiddlewareQueueRabbitMQ(
                MOM_HOST, FILTER_Q3_QUEUE
            )
            self.output_queues[SCATTER_GATHER_MAPPER_QUEUE] = middleware.MessageMiddlewareQueueRabbitMQ(
                MOM_HOST, SCATTER_GATHER_MAPPER_QUEUE
            )
            self.output_queues[SUM_Q3_QUEUE] = middleware.MessageMiddlewareQueueRabbitMQ(
                MOM_HOST, SUM_Q3_QUEUE
            )

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

        # Definicion de la entrada del exchange de control
        self.control_input = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, CONTROL_EXCHANGE, [self.personal_control_key]
        )

        # Definicion del output del exchange de control
        self.control_output = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, CONTROL_EXCHANGE, self.output_control_keys
        )

        # Serializadores para transacciones y mensajes de control
        self.transaction_serializer = message_protocol.internal.TransactionSerializer()
        self.control_serializer = message_protocol.internal.ControlMessageSerializer()
        self.internal_packet_serializer = message_protocol.internal.InternalProtocol()

        # Estado interno
        self.lock = threading.Lock()
        self.active = True
        self.control_thread = None

        # Procesados por cliente
        self.processed_by_client = {}
        self.forwarded_by_client = {}
        self.closed_by_client = set()
        self.control_responses_by_client = {}
        self.all_processed_by_client = {}
        self.all_forwarded_by_client = {}
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

    def _pass_eof_control_message(
            self, client_id, expected_total
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
        self.control_output.send(message)

    def _answer_control_message(
            self, client_id, expected_total, processed_count
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
        self.control_output.send(message)

    def _request_control_message(self, client_id, expected_total):
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
        self.control_output.send(message)

    def _flush_control_message(self, client_id):
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
        self.control_output.send(message)

    def _ack_flush_control_message(self, client_id, msgs_sent):
        '''
        Envia un mensaje de control a los workers correspondientes 
        indicando que se han liberado los recursos asociados a un cliente
        '''
        message  = self.control_serializer.serialize(
            message_protocol.internal.ControlMessage(
                sender_id=ID,
                expected_total=0,
                processed_count=msgs_sent
            )
        )
        message = self.internal_packet_serializer.create_packet(
            msg_type=message_protocol.internal.MessageType.FLUSH_ACK,
            client_id_bytes=client_id.to_bytes(16, byteorder='big'),
            payload=message
        )
        self.control_output.send(message)

    def _cleanup_client(self, client_id):
        '''
        Elimina toda la informacion asociada a un cliente, cerrando su procesamiento
        '''
        with self.lock:
            if client_id in self.processed_by_client:
                del self.processed_by_client[client_id]
            if client_id in self.forwarded_by_client:
                del self.forwarded_by_client[client_id]
            if client_id in self.all_processed_by_client:
                del self.all_processed_by_client[client_id]
            if client_id in self.all_forwarded_by_client:
                del self.all_forwarded_by_client[client_id]
            if client_id in self.control_responses_by_client:
                del self.control_responses_by_client[client_id]
            if client_id in self.flushed_acks_by_client:
                del self.flushed_acks_by_client[client_id]
            self.pending_eof_by_client.pop(client_id, None)
            self.closed_by_client.add(client_id)
    
    def _forward_transaction(self, transaction: Transaction, client_id: int):
        '''
        Envia una transaccion a la cola de salida correspondiente segun la configuracion del worker
        '''
        logging.debug(f"Transaction {transaction} passed filter in filter_{CONFIGURATION} with id {ID}, forwarding to output")
        payload = self.transaction_serializer.serialize(transaction)
        message = self.internal_packet_serializer.create_packet(
            msg_type=message_protocol.internal.MessageType.DATA,
            client_id_bytes=client_id.to_bytes(16, byteorder='big'),
            payload=payload
        )
        if CONFIGURATION == C_Q1:
            self.output_queues[GATEWAY_QUEUE].send(message)
        if CONFIGURATION == C_Q5:
            self.output_queues[FILTER_Q5_USD_QUEUE].send(message)
        if CONFIGURATION == C_USD:
            self.output_queues[FILTER_Q1_QUEUE].send(message)
            self.output_queues[SUM_Q2_QUEUE].send(message)
            self.output_queues[FILTER_DATE_QUEUE].send(message)
        if CONFIGURATION == C_DATE:
            # Si la transaccion esta entre las fechas 2022-09-06 y 2022-09-15, va al sum de Q3 por sharding
            # Si la transaccion esta entre las fechas 2022-09-01 y 2022-09-05, va al filtro de Q3
            # Si la transaccion esta entre las fechas 2022-09-01 y 2022-09-05, va al scatter gather mapper de Q4
            if self._filter_transaction(transaction, start_date="2022-09-06", end_date="2022-09-15"):
                self.output_queues[SUM_Q3_QUEUE].send(message)
            if self._filter_transaction(transaction, start_date="2022-09-01", end_date="2022-09-05"):
                self.output_queues[FILTER_Q3_QUEUE].send(message)
                self.output_queues[SCATTER_GATHER_MAPPER_QUEUE].send(message)

    def _forward_eof(self, client_id: int, expected_total: int):
        '''
        Envia un mensaje de EOF a la cola de salida correspondiente segun la configuracion del worker
        '''
        message = self.control_serializer.serialize(
            message_protocol.internal.ControlMessage(
                sender_id=ID,
                expected_total=expected_total,
                processed_count=0
            )
        )
        message = self.internal_packet_serializer.create_packet(
            msg_type=message_protocol.internal.MessageType.EOF,
            client_id_bytes=client_id.to_bytes(16, byteorder='big'),
            payload=message
        )
        if CONFIGURATION == C_Q1:
            self.output_queues[GATEWAY_QUEUE].send(message)
        if CONFIGURATION == C_Q5:
            self.output_queues[FILTER_Q5_USD_QUEUE].send(message)
        if CONFIGURATION == C_USD:
            self.output_queues[FILTER_Q1_QUEUE].send(message)
            self.output_queues[SUM_Q2_QUEUE].send(message)
            self.output_queues[FILTER_DATE_QUEUE].send(message)
        if CONFIGURATION == C_DATE:
            self.output_queues[SUM_Q3_QUEUE].send(message)
            self.output_queues[FILTER_Q3_QUEUE].send(message)
            self.output_queues[SCATTER_GATHER_MAPPER_QUEUE].send(message)

    
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
        # Desempaquetamos el mensaje
        msg_type, client_id, payload = self.internal_packet_serializer.unpack_packet(message)

        with self.lock:
            if client_id in self.closed_by_client:
                # Si el cliente ya fue cerrado, no procesamos mas mensajes de ese cliente
                logging.info(f"Received message for closed client {client_id} in filter_{CONFIGURATION} with id {ID}, ignoring")
                return

        if msg_type == message_protocol.internal.MessageType.DATA:
            with self.lock:
                if client_id not in self.first_data_logged_by_client:
                    self.first_data_logged_by_client.add(client_id)
                    logging.info(
                        "filter_first_chunk_received | filter=%s | id=%s | "
                        "client_id=%s | message_bytes=%s | payload_bytes=%s",
                        CONFIGURATION,
                        ID,
                        client_id,
                        len(message),
                        len(payload),
                    )

            # Deserializamos la transaccion.
            transaction = self.transaction_serializer.deserialize(payload)

            with self.lock:
                if client_id not in self.deserialized_by_client:
                    self.deserialized_by_client[client_id] = 0
                self.deserialized_by_client[client_id] += 1
                deserialized_count = self.deserialized_by_client[client_id]

            if deserialized_count <= 3:
                logging.info(
                    "filter_transaction_deserialized | filter=%s | id=%s | "
                    "client_id=%s | transaction_number=%s | date=%s | "
                    "from_bank=%s | from_account=%s | to_bank=%s | "
                    "to_account=%s | amount=%s | currency=%s | format=%s",
                    CONFIGURATION,
                    ID,
                    client_id,
                    deserialized_count,
                    transaction.date,
                    transaction.from_bank,
                    transaction.from_account,
                    transaction.to_bank,
                    transaction.to_account,
                    transaction.amount,
                    transaction.currency,
                    transaction.format,
                )

            if deserialized_count == 3:
                logging.info(
                    "==================== Forward pass successful - Mate | "
                    "filter=%s | id=%s | client_id=%s | "
                    "transactions_deserialized=%s ====================",
                    CONFIGURATION,
                    ID,
                    client_id,
                    deserialized_count,
                )

            # Aplicamos el filtro y forwrdeamos
            if CONFIGURATION == C_DATE or self._filter_transaction(transaction):
                with self.lock:
                    if client_id not in self.forwarded_by_client:
                        self.forwarded_by_client[client_id] = 0
                    self.forwarded_by_client[client_id] += 1
                self._forward_transaction(transaction, client_id)

            # Actualizamos el conteo de procesados para este cliente
            with self.lock:
                if client_id not in self.processed_by_client:
                    self.processed_by_client[client_id] = 0
                self.processed_by_client[client_id] += 1

            # Si habia un EOF pendiente (caso un unico filtro), intentar cerrarlo
            # ahora que se proceso un mensaje mas.
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
            msgs_sent = self.forwarded_by_client.get(client_id, 0)
            del self.pending_eof_by_client[client_id]

        logging.info(
            f"All {expected_total} expected messages processed for client {client_id} "
            f"in filter_{CONFIGURATION}, forwarding EOF (forwarded={msgs_sent})"
        )
        self._forward_eof(client_id, msgs_sent)
        self._cleanup_client(client_id)

    def _process_control_message(self, message):
        '''
        Procesa un mensaje de control recibido por el exchange de control, actualizando el estado interno del worker
        y respondiendo a los mensajes de control correspondientes
        '''
        # Desempaquetamos el mensaje
        msg_type, client_id, payload = self.internal_packet_serializer.unpack_packet(message)
        control_message = self.control_serializer.deserialize(payload)

        if msg_type == message_protocol.internal.MessageType.EOF_RECEIVED:
            if not self.is_leader:
                logging.warning(f"Received EOF_RECEIVED control message from worker {control_message.sender_id} for client {client_id} in filter_{CONFIGURATION}, but I am not the leader, ignoring")
                return
            # Si se recibe un mensaje indicando que se recibio un EOF para un cliente, se responde con una solicitud de conteo de procesados para ese cliente
            self._request_control_message(client_id, control_message.expected_total)

        elif msg_type == message_protocol.internal.MessageType.PROCESSED_REQUEST:
            if self.is_leader:
                logging.warning(f"Received PROCESSED_REQUEST control message from worker {control_message.sender_id} for client {client_id} in filter_{CONFIGURATION}, but I am the leader, ignoring")
                return
            # Si se recibe una solicitud de conteo procesados, se responde con un mensaje indicando
            # cuantos mensajes se han procesado para ese cliente
            with self.lock:
                processed_count = self.processed_by_client.get(client_id, 0)
            self._answer_control_message(client_id, control_message.expected_total, processed_count)
        
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
                    self._flush_control_message(client_id)
                else:
                    logging.info(f"Received all PROCESSED_ANSWER control messages for client {client_id} in filter_{CONFIGURATION}, total processed: {procesados + control_message.processed_count}, expected: {control_message.expected_total}, but counts do not match, resending PROCESSED_REQUEST")
                    self._request_control_message(client_id, control_message.expected_total)
                    self.control_responses_by_client[client_id] = set()
                    self.all_processed_by_client[client_id] = 0
        
        elif msg_type == message_protocol.internal.MessageType.FLUSH_ORDER:
            if self.is_leader:
                logging.warning(f"Received FLUSH_ORDER control message from worker {control_message.sender_id} for client {client_id} in filter_{CONFIGURATION}, but I am the leader, ignoring")
                return
            # Si se recibe una orden de flush, se limpian los recursos asociados a ese cliente y se responde con un mensaje de ack
            msgs_sent = 0
            with self.lock:
                msgs_sent = self.forwarded_by_client.get(client_id, 0)
            self._cleanup_client(client_id)
            self._ack_flush_control_message(client_id, msgs_sent)
            
        
        elif msg_type == message_protocol.internal.MessageType.FLUSH_ACK:
            if not self.is_leader:
                logging.warning(f"Received FLUSH_ACK control message from worker {control_message.sender_id} for client {client_id} in filter_{CONFIGURATION}, but I am not the leader, ignoring")
                return
            
            if client_id not in self.flushed_acks_by_client:
                self.flushed_acks_by_client[client_id] = set()            
            self.flushed_acks_by_client[client_id].add(control_message.sender_id)
            if client_id not in self.all_forwarded_by_client:
                self.all_forwarded_by_client[client_id] = 0
            self.all_forwarded_by_client[client_id] += control_message.processed_count

            if len(self.flushed_acks_by_client[client_id]) == FILTER_AMOUNT - 1:
                logging.info(f"Received all FLUSH_ACK control messages for client {client_id} in filter_{CONFIGURATION}, cleaning up client")
                with self.lock:
                    msgs_sent = self.all_forwarded_by_client[client_id] + self.forwarded_by_client.get(client_id, 0)
                self._forward_eof(client_id, msgs_sent)
                self._cleanup_client(client_id)

        else:
            logging.warning(f"Received unknown control message type: {msg_type} from worker {control_message.sender_id} for client {client_id} in filter_{CONFIGURATION}")

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

    def process_control_messages(self, message, ack, nack):
        '''
        Callback para procesar los mensajes recibidos por el exchange de control
        '''
        try:
            self._process_control_message(message)
            ack()
        except Exception as e:
            logging.error(f"Error processing control message in filter_{CONFIGURATION} with id {ID}: {e}")
            nack()

    def start(self):
        '''
        Inicia el procesamiento de mensajes de la cola de entrada y del exchange de control
        '''
        # Se inicia un thread para procesar los mensajes de control
        self.control_thread = threading.Thread(
            target=self.control_input.start_consuming,
            args=(self.process_control_messages,)
        )
        self.control_thread.start()

        try:
            # Se procesan los mensajes de la cola de entrada en el thread principal
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
        Maneja la señal de terminacion, cerrando los recursos de manera ordenada
        '''
        logging.info(f"Received SIGTERM in filter_{CONFIGURATION} with id {ID}, shutting down")
        self.active = False
        self.input_queue.stop_consuming()
        self.control_input.stop_consuming()
        self.close()
    
    def close(self):
        '''
        Cierra los recursos utilizados por este worker
        '''
        logging.info(f"Closing filter_{CONFIGURATION} with id {ID}")

        try:
            self.input_queue.close()
        except Exception as e:
            logging.error(f"Error closing input queue in filter_{CONFIGURATION} with id {ID}: {e}")

        try:
            self.control_input.close()
        except Exception as e:
            logging.error(f"Error closing control input in filter_{CONFIGURATION} with id {ID}: {e}")

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
