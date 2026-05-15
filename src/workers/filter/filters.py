import os
import logging
import threading
import time

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
# Colas de Salida Posibles
GATEWAY_QUEUE = os.environ["GATEWAY_QUEUE"]
FILTER_DATE_QUEUE = os.environ["FILTER_DATE_QUEUE"]
FILTER_Q1_QUEUE = os.environ["FILTER_Q1_QUEUE"]
SUM_Q2_QUEUE = os.environ["SUM_Q2_QUEUE"]
FILTER_Q3_QUEUE = os.environ["FILTER_Q3_QUEUE"]
SCATTER_GATHER_MAPPER_QUEUE = os.environ["SCATTER_GATHER_MAPPER_QUEUE"]
FILTER_Q5_USD_QUEUE = os.environ["FILTER_Q5_USD_QUEUE"]
# Exchanges de Salida Posibles (necesario hacer sharding)
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_Q3_AMOUNT = int(os.environ["SUM_Q3_AMOUNT"])
# Para control en token ring
FILTER_AMOUNT = int(os.environ["FILTER_AMOUNT"])
FILTER_PREFIX = os.environ["FILTER_PREFIX"] + "_" + CONFIGURATION
CONTROL_EXCHANGE = os.environ["FILTER_PREFIX"] + "_" + "CONTROL_EXCHANGE_" + CONFIGURATION

class FilterWorker:
    def __init__(self):
    
        # Iniciacion de la cola de entrada
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
            # Para el filtro de tipo de moneda, se necesitan dos colas de salida y un exchange para los Sum:
            #    - Una para el filtro de Q3
            #    - Una para el scatter gather mapper de Q4
            #    - Un exchange para el sum de Q3, con N colas dependiendo del sharding
            self.output_queues[FILTER_Q3_QUEUE] = middleware.MessageMiddlewareQueueRabbitMQ(
                MOM_HOST, FILTER_Q3_QUEUE
            )
            self.output_queues[SCATTER_GATHER_MAPPER_QUEUE] = middleware.MessageMiddlewareQueueRabbitMQ(
                MOM_HOST, SCATTER_GATHER_MAPPER_QUEUE
            )
            for i in range(SUM_Q3_AMOUNT):
                data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                    MOM_HOST, SUM_PREFIX, [f"{SUM_PREFIX}_{i}"]
                )
                self.output_exchanges.append(data_output_exchange)

        # Seccion de control
        # Definicion de las keys de los exchanges 
        # Cada worker puede enviar a todos los demas workers
        self.personal_control_key = f"{FILTER_PREFIX}_{ID}"
        self.output_control_keys = []
        for i in range(FILTER_AMOUNT):
            if i != ID:
                self.output_control_keys.append(f"{FILTER_PREFIX}_{i}")

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
        self.leader_by_client = {}

    def _answer_control_message(
            self, client_id, expected_total, processed_count
    ):
        '''
        Responde a una solicitud de control con un mensaje indicando
        '''
        message = self.control_serializer.serialize(
            message_protocol.common.ControlMessage(
                sender_id=ID,
                expected_total=expected_total,
                processed_count=processed_count
            )
        )
        message = self.internal_packet_serializer.create_packet(
            msg_type=message_protocol.common.MessageType.PROCESSED_ANSWER,
            client_id_bytes=client_id.to_bytes(16, byteorder='big'),
            payload=message
        )
        self.control_output.send(message)

    def _request_control_message(self, client_id, expected_total):
        '''
        Envia un mensaje de control a los workers correspondientes solicitando informacion de cuantos mensajes han procesado
        '''
        message = self.control_serializer.serialize(
            message_protocol.common.ControlMessage(
                sender_id=ID,
                expected_total=expected_total,
                processed_count=0
            )
        )
        message = self.internal_packet_serializer.create_packet(
            msg_type=message_protocol.common.MessageType.PROCESSED_REQUEST,
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
            message_protocol.common.ControlMessage(
                sender_id=ID,
                expected_total=0,
                processed_count=0
            )
        )
        message = self.internal_packet_serializer.create_packet(
            msg_type=message_protocol.common.MessageType.FLUSH_ORDER,
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
            message_protocol.common.ControlMessage(
                sender_id=ID,
                expected_total=0,
                processed_count=msgs_sent
            )
        )
        message = self.internal_packet_serializer.create_packet(
            msg_type=message_protocol.common.MessageType.FLUSH_ACK,
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
            if client_id in self.leader_by_client:
                del self.leader_by_client[client_id]
            if client_id in self.deserialized_by_client:
                del self.deserialized_by_client[client_id]
            self.closed_by_client.add(client_id)
    
    def _forward_transaction(self, transaction: Transaction, client_id: int):
        '''
        Envia una transaccion a la cola de salida correspondiente segun la configuracion del worker
        '''
        try:
            logging.info(f"Transaction {transaction} passed filter in filter_{CONFIGURATION} with id {ID}, forwarding to output")
            payload = self.transaction_serializer.serialize(transaction)
            message = self.internal_packet_serializer.create_packet(
                msg_type=message_protocol.common.MessageType.DATA,
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
                    # Shardeo por el id de la transaccion
                    shard = transaction.hash_by_payment_format(SUM_Q3_AMOUNT)
                    self.output_exchanges[shard].send(message)
                if self._filter_transaction(transaction, start_date="2022-09-01", end_date="2022-09-05"):
                    self.output_queues[FILTER_Q3_QUEUE].send(message)
                    self.output_queues[SCATTER_GATHER_MAPPER_QUEUE].send(message)
        except Exception as e:
            logging.error(f"Error forwarding transaction in filter_{CONFIGURATION} with id {ID} for client {client_id}: {e}")

    def _forward_eof(self, client_id: int, expected_total: int):
        '''
        Envia un mensaje de EOF a la cola de salida correspondiente segun la configuracion del worker
        '''
        try:
            message = self.control_serializer.serialize(
                message_protocol.common.ControlMessage(
                    sender_id=ID,
                    expected_total=expected_total,
                    processed_count=0
                )
            )
            message = self.internal_packet_serializer.create_packet(
                msg_type=message_protocol.common.MessageType.EOF,
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
                for exchange in self.output_exchanges:
                    exchange.send(message)
                self.output_queues[FILTER_Q3_QUEUE].send(message)
                self.output_queues[SCATTER_GATHER_MAPPER_QUEUE].send(message)
        except Exception as e:
            logging.error(f"Error forwarding EOF in filter_{CONFIGURATION} with id {ID} for client {client_id}: {e}")

    
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
            return transaction.is_in_date_range(start_date, end_date)
        raise ValueError(f"Invalid configuration: {CONFIGURATION}")
    
    
    def _process_data_message(self, message):
        '''
        Procesa un mensaje de la cola de entrada, aplicando el filtro correspondiente a la
        configuracion de este worker y reenviando la transaccion a la cola de salida correspondiente 
        si la transaccion pasa el filtro
        '''
        # Desempaquetamos el mensaje
        try:
            msg_type, client_id, payload = self.internal_packet_serializer.unpack_packet(message)
        except Exception as e:
            logging.error(f"Error unpacking packet in filter_{CONFIGURATION} with id {ID}: {e}")
            return

        with self.lock:
            if client_id in self.closed_by_client:
                # Si el cliente ya fue cerrado, no procesamos mas mensajes de ese cliente
                logging.info(f"Received message for closed client {client_id} in filter_{CONFIGURATION} with id {ID}, ignoring")
                return

        if msg_type == message_protocol.common.MessageType.DATA:
            with self.lock:
                if client_id not in self.first_data_logged_by_client:
                    self.first_data_logged_by_client.add(client_id)
                    try:
                        logging.info(
                            "filter_first_chunk_received | filter=%s | id=%s | "
                            "client_id=%s | message_bytes=%s | payload_bytes=%s",
                            CONFIGURATION,
                            ID,
                            client_id,
                            len(message),
                            len(payload),
                        )
                    except Exception as e:
                        logging.error(f"Error logging first chunk received in filter_{CONFIGURATION} with id {ID} for client {client_id}: {e}")

            # Deserializamos la transaccion.
            try:
                transaction = self.transaction_serializer.deserialize(payload)
            except Exception as e:
                logging.error(f"Error deserializing transaction in filter_{CONFIGURATION} with id {ID} for client {client_id}: {e}")
                return

            with self.lock:
                if client_id not in self.deserialized_by_client:
                    self.deserialized_by_client[client_id] = 0
                self.deserialized_by_client[client_id] += 1
                deserialized_count = self.deserialized_by_client[client_id]

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
            
        elif msg_type == message_protocol.common.MessageType.EOF:
            # El worker que recibe el EOF se convierte en el lider para ese cliente
            try:
                control_message = self.control_serializer.deserialize(payload)
            except Exception as e:
                logging.error(f"Error deserializing EOF message in filter_{CONFIGURATION} with id {ID} for client {client_id}: {e}")
                return
            expected_total = control_message.expected_total

            logging.info(f"Received EOF for client {client_id} in filter_{CONFIGURATION}. Expected total: {expected_total}.")
            
            with self.lock:
                self.leader_by_client[client_id] = ID
            
            if FILTER_AMOUNT == 1:
                # Solo un filter, enviar EOF directamente
                with self.lock:
                    msgs_sent = self.forwarded_by_client.get(client_id, 0)
                self._forward_eof(client_id, msgs_sent)
                self._cleanup_client(client_id)
            else:
                # Solicitar conteos de los demas workers
                self._request_control_message(client_id, expected_total)
        
        else:
            logging.warning(f"Received unknown message type: {msg_type} for filter_{CONFIGURATION}")
    
    def _process_control_message(self, message):
        '''
        Procesa un mensaje de control recibido por el exchange de control, actualizando el estado interno del worker
        y respondiendo a los mensajes de control correspondientes
        '''
        # Desempaquetamos el mensaje
        try:
            msg_type, client_id, payload = self.internal_packet_serializer.unpack_packet(message)
        except Exception as e:
            logging.error(f"Error unpacking control packet in filter_{CONFIGURATION} with id {ID}: {e}")
            return
        
        try:
            control_message = self.control_serializer.deserialize(payload)
        except Exception as e:
            logging.error(f"Error deserializing control message in filter_{CONFIGURATION} with id {ID} for client {client_id}: {e}")
            return

        if msg_type == message_protocol.common.MessageType.PROCESSED_REQUEST:
            with self.lock:
                self.leader_by_client[client_id] = control_message.sender_id
                leader_id = self.leader_by_client.get(client_id)
            if leader_id == ID:
                logging.warning(f"Received PROCESSED_REQUEST control message from worker {control_message.sender_id} for client {client_id} in filter_{CONFIGURATION}, but I am the leader, ignoring")
                return
            # Si se recibe una solicitud de conteo procesados, se responde con un mensaje indicando
            # cuantos mensajes se han procesado para ese cliente
            with self.lock:
                processed_count = self.processed_by_client.get(client_id, 0)
            self._answer_control_message(client_id, control_message.expected_total, processed_count)
        
        elif msg_type == message_protocol.common.MessageType.PROCESSED_ANSWER:
            with self.lock:
                leader_id = self.leader_by_client.get(client_id)
            if leader_id != ID:
                logging.warning(f"Received PROCESSED_ANSWER control message from worker {control_message.sender_id} for client {client_id} in filter_{CONFIGURATION}, but I am not the leader, ignoring")
                return
            
            should_resend = False
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
                        should_resend = True
                        logging.info(f"Received all PROCESSED_ANSWER control messages for client {client_id} in filter_{CONFIGURATION}, total processed: {procesados + control_message.processed_count}, expected: {control_message.expected_total}, but counts do not match, resending PROCESSED_REQUEST")
                        self.control_responses_by_client[client_id] = set()
                        self.all_processed_by_client[client_id] = 0
                        
            if should_resend:
                time.sleep(0.05) # Esperamos un poco antes de reenviar para evitar busy wait
                self._request_control_message(client_id, control_message.expected_total)
                        
        
        elif msg_type == message_protocol.common.MessageType.FLUSH_ORDER:
            with self.lock:
                leader_id = self.leader_by_client.get(client_id)
            if leader_id == ID:
                logging.warning(f"Received FLUSH_ORDER control message from worker {control_message.sender_id} for client {client_id} in filter_{CONFIGURATION}, but I am the leader, ignoring")
                return
            # Si se recibe una orden de flush, se limpian los recursos asociados a ese cliente y se responde con un mensaje de ack
            msgs_sent = 0
            with self.lock:
                msgs_sent = self.forwarded_by_client.get(client_id, 0)
            self._cleanup_client(client_id)
            self._ack_flush_control_message(client_id, msgs_sent)
            
        
        elif msg_type == message_protocol.common.MessageType.FLUSH_ACK:
            with self.lock:
                leader_id = self.leader_by_client.get(client_id)
            if leader_id != ID:
                logging.warning(f"Received FLUSH_ACK control message from worker {control_message.sender_id} for client {client_id} in filter_{CONFIGURATION}, but I am not the leader, ignoring")
                return
            
            with self.lock:
                if client_id not in self.flushed_acks_by_client:
                    self.flushed_acks_by_client[client_id] = set()            
                self.flushed_acks_by_client[client_id].add(control_message.sender_id)
                if client_id not in self.all_forwarded_by_client:
                    self.all_forwarded_by_client[client_id] = 0
                self.all_forwarded_by_client[client_id] += control_message.processed_count

                if len(self.flushed_acks_by_client[client_id]) == FILTER_AMOUNT - 1:
                    logging.info(f"Received all FLUSH_ACK control messages for client {client_id} in filter_{CONFIGURATION}, cleaning up client")
                
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
            if self.active:
                logging.error(f"Error closing input queue in filter_{CONFIGURATION} with id {ID}: {e}")

        try:
            self.control_input.close()
        except Exception as e:
            if self.active:
                logging.error(f"Error closing control input in filter_{CONFIGURATION} with id {ID}: {e}")

        try:
            self.control_output.close()
        except Exception as e:
            if self.active:
                logging.error(f"Error closing control output in filter_{CONFIGURATION} with id {ID}: {e}")

        for queue in self.output_queues.values():
            try:
                queue.close()
            except Exception as e:
                if self.active:
                    logging.error(f"Error closing output queue in filter_{CONFIGURATION} with id {ID}: {e}")

        for exchange in self.output_exchanges:
            try:
                exchange.close()
            except Exception as e:
                if self.active:
                    logging.error(f"Error closing output exchange in filter_{CONFIGURATION} with id {ID}: {e}")
