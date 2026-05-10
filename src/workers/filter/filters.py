import os
import logging
import threading

from common import message_protocol
from common.domain.transaction import Transaction
from common.middleware import middleware
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

class filterWorker:
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
        self.closed_by_client = set()

    def _pass_eof_control_message(
            self, client_id, expected_total
    ):
        '''
        Envia un mensaje de control al lider indicando que se recibio un EOF para un cliente, 
        con la cantidad total de mensajes que se esperan para ese cliente
        '''
        message = self.control_serializer.serialize(
            message_protocol.common.ControlMessage(
                sender_id=ID,
                expected_total=expected_total,
                processed_count=0
            )
        )
        message = self.internal_packet_serializer.create_packet(
            msg_type=message_protocol.common.MessageType.EOF_RECEIVED,
            client_id_bytes=client_id.to_bytes(16, byteorder='big'),
            payload=message
        )
        self.control_output.publish(message)

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
        self.control_output.publish(message)

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
        self.control_output.publish(message)

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
            payload=b''
        )
        self.control_output.publish(message)

    def _ack_flush_control_message(self, client_id):
        '''
        Envia un mensaje de control a los workers correspondientes 
        indicando que se han liberado los recursos asociados a un cliente
        '''
        message  = self.control_serializer.serialize(
            message_protocol.common.ControlMessage(
                sender_id=ID,
                expected_total=0,
                processed_count=0
            )
        )
        message = self.internal_packet_serializer.create_packet(
            msg_type=message_protocol.common.MessageType.FLUSH_ACK,
            client_id_bytes=client_id.to_bytes(16, byteorder='big'),
            payload=b''
        )
        self.control_output.publish(message)






    

    
    

    





        
                





