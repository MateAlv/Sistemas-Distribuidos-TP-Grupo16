import logging
import os
import threading

from common import middleware
from common.domain.transaction import Transaction
from common.message_protocol.aggregation_serializer import AggregationSerializer
from common.constants import C_Q2, C_Q3, C_Q5
from common.message_protocol.common import MessageType
from common.message_protocol.internal import InternalProtocol
from common.message_protocol.transaction_serializer import TransactionSerializer
from common.message_protocol.control_message_serializer import ControlMessageSerializer
from common.message_protocol.common.control_message import ControlMessage


ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
CONFIGURATION = os.environ["CONFIGURATION"]

# Q5
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
DATA_QUEUE = os.environ["DATA_QUEUE"]

# Q2
SUM_Q2_EXCHANGE = os.environ["SUM_Q2_EXCHANGE"]
AGG_Q2_OUTPUT_QUEUE = os.environ["AGG_Q2_OUTPUT_QUEUE"]

# Q3
JOIN_Q3_EXCHANGE = os.environ["JOIN_Q3_EXCHANGE"]
AGG_Q3_DATA_QUEUE = os.environ["AGG_Q3_DATA_QUEUE"]
AGG_Q3_OUTPUT_QUEUE = os.environ["AGG_Q3_OUTPUT_QUEUE"]

# Este worker solo implementa agregación para:
#   - "Q2": conserva el máximo monto por banco emisor y, al EOF, emite esos máximos como DATA seguido de un EOF con expected_total.
#   - "Q5": cuenta transacciones por cliente y emite el total final en el EOF.
#   - "Q3": TODO



class AggregatorWorker:
    def __init__(self):
        self.transaction_serializer = TransactionSerializer()
        self.aggregation_serializer = AggregationSerializer()
        self.internal_packet_serializer = InternalProtocol()

        self.input_queues = {}
        self.output_queues = {}
        self.input_exchanges = []
        self.output_exchanges = []
        
        # caso query 2
        if CONFIGURATION == C_Q2:
            self.input_exchanges.append(
                middleware.MessageMiddlewareExchangeRabbitMQ(
                    MOM_HOST, SUM_Q2_EXCHANGE, [f"{SUM_Q2_EXCHANGE}_{ID}"]
                )      
            )
            self.output_queues[AGG_Q2_OUTPUT_QUEUE] = middleware.MessageMiddlewareQueueRabbitMQ(
                MOM_HOST, AGG_Q2_OUTPUT_QUEUE
            )

        # caso query 3
        if CONFIGURATION == C_Q3:
            self.input_exchanges.append(
                middleware.MessageMiddlewareExchangeRabbitMQ(
                    MOM_HOST, JOIN_Q3_EXCHANGE, [f"{JOIN_Q3_EXCHANGE}_{ID}"]
                )
            )
            self.input_queues[INPUT_QUEUE] = middleware.MessageMiddlewareQueueRabbitMQ(
                MOM_HOST, INPUT_QUEUE
            )
            self.output_queues[AGG_Q3_OUTPUT_QUEUE] = middleware.MessageMiddlewareQueueRabbitMQ(
                MOM_HOST, AGG_Q3_OUTPUT_QUEUE
            )

        # caso query 5
        if CONFIGURATION == C_Q5:
            self.input_queues[INPUT_QUEUE] = middleware.MessageMiddlewareQueueRabbitMQ(
                MOM_HOST, INPUT_QUEUE
            )

            self.output_queues[DATA_QUEUE] = middleware.MessageMiddlewareQueueRabbitMQ(
                MOM_HOST, DATA_QUEUE
            )



        self.lock = threading.Lock()
        self.active = True
        self.counts_by_client: dict[int, int] = {}
        self.max_tx_by_client: dict[int, dict[str, Transaction]] = {}
        self.exchange_threads = []

        # TODO: borrar esto
        self.input_queue = next(iter(self.input_queues.values()), None)
        self.output_queue = next(iter(self.output_queues.values()), None)

    def _increment_count(self, client_id: int) -> int:
        with self.lock:
            current = self.counts_by_client.get(client_id, 0) + 1
            self.counts_by_client[client_id] = current
            return current

    def _current_count(self, client_id: int) -> int:
        with self.lock:
            return self.counts_by_client.get(client_id, 0)

    def _send_count(self, client_id: int, msg_type: MessageType, count: int) -> None:
        packet = self.internal_packet_serializer.create_packet(
            msg_type=msg_type,
            client_id_bytes=client_id.to_bytes(16, byteorder="big"),
            payload=self.aggregation_serializer.serialize(count),
        )
        self.output_queue.send(packet)

    def _record_max_transaction(self, client_id: int, transaction: Transaction) -> None:
        with self.lock:
            current_by_bank = self.max_tx_by_client.setdefault(client_id, {})
            current = current_by_bank.get(transaction.from_bank)
            if current is None or transaction.amount > current.amount:
                current_by_bank[transaction.from_bank] = transaction

    def _send_max_transactions(self, client_id: int) -> None:
        with self.lock:
            transactions = list(self.max_tx_by_client.get(client_id, {}).values())

        for transaction in transactions:
            packet = self.internal_packet_serializer.create_packet(
                msg_type=MessageType.DATA,
                client_id_bytes=client_id.to_bytes(16, byteorder="big"),
                payload=self.transaction_serializer.serialize(transaction),
            )
            self.output_queue.send(packet)

    def _process_data_message(self, message: bytes) -> None:
        try:
            msg_type, client_id, payload = self.internal_packet_serializer.unpack_packet(
                message
            )

            if msg_type == MessageType.DATA:
                transaction: Transaction = self.transaction_serializer.deserialize(payload)
                logging.debug(
                    "aggregation_data | id=%s | client_id=%s | date=%s | amount=%s | format=%s | mode=%s",
                    ID,
                    client_id,
                    transaction.date,
                    transaction.amount,
                    transaction.format,
                    CONFIGURATION,
                )
                if CONFIGURATION == C_Q2:
                    self._record_max_transaction(client_id, transaction)
                else:
                    # Q5 (streaming counter): only increment local counter, do not emit per-DATA
                    self._increment_count(client_id)
                return

            if msg_type == MessageType.EOF:
                if CONFIGURATION == C_Q2:
                    # send DATA messages for each max transaction
                    with self.lock:
                        transactions = list(self.max_tx_by_client.get(client_id, {}).values())
                        # number of DATA messages we'll send
                        msgs_sent = len(transactions)

                    for transaction in transactions:
                        packet = self.internal_packet_serializer.create_packet(
                            msg_type=MessageType.DATA,
                            client_id_bytes=client_id.to_bytes(16, byteorder="big"),
                            payload=self.transaction_serializer.serialize(transaction),
                        )
                        self.output_queue.send(packet)

                    # send EOF with expected_total = msgs_sent using control message format
                    control_payload = ControlMessageSerializer().serialize(
                        ControlMessage(sender_id=ID, expected_total=msgs_sent, processed_count=0)
                    )
                    eof_packet = self.internal_packet_serializer.create_packet(
                        msg_type=MessageType.EOF,
                        client_id_bytes=client_id.to_bytes(16, byteorder="big"),
                        payload=control_payload,
                    )
                    self.output_queue.send(eof_packet)

                    # cleanup memory for this client
                    with self.lock:
                        self.max_tx_by_client.pop(client_id, None)
                        self.counts_by_client.pop(client_id, None)
                else:
                    final_count = self._current_count(client_id)
                    # send final aggregated count in EOF payload
                    self._send_count(client_id, MessageType.EOF, final_count)
                    # cleanup memory for this client
                    with self.lock:
                        self.counts_by_client.pop(client_id, None)
                return

            raise ValueError(f"unsupported message type: {msg_type}")
        except Exception as e:
            logging.error("aggregation_message_error | id=%s | error=%s", ID, e)
            # propagate exception to caller which will decide to nack
            raise

    def process_data_messages(self, message, ack, nack):
        try:
            self._process_data_message(message)
            ack()
        except Exception as e:
            logging.error("aggregation_callback_error | id=%s | error=%s", ID, e)
            nack()

    def start(self):
        logging.info(
            "aggregation_start | id=%s | mode=%s | input_queues=%s | input_exchanges=%s | output_queues=%s",
            ID,
            CONFIGURATION,
            len(self.input_queues),
            len(self.input_exchanges),
            len(self.output_queues),
        )

        # Un thread por cada input exchange (start consuming es bloqueante)
        for exchange in self.input_exchanges:
            t = threading.Thread(
                target=exchange.start_consuming,
                args=(self.process_data_messages,),
            )
            t.start()
            self.exchange_threads.append(t)

        try:
            # La input queue principal corre en el thread principal.
            if self.input_queue is not None:
                self.input_queue.start_consuming(self.process_data_messages)
            elif self.exchange_threads:
                # Si no hay cola principal (ej. Q2), bloqueamos en los exchanges activos.
                for t in self.exchange_threads:
                    t.join()
            else:
                logging.warning("aggregation_start_without_inputs | id=%s | mode=%s", ID, CONFIGURATION)
        except Exception as e:
            logging.error("aggregation_start_error | id=%s | mode=%s | error=%s", ID, CONFIGURATION, e)
        finally:
            self.handle_sigterm()
            for t in self.exchange_threads:
                t.join(timeout=5)
            self.close()
        

    def handle_sigterm(self):
        logging.info("Received SIGTERM in aggregation with id %s, shutting down", ID)
        self.active = False
        for queue in self.input_queues.values():
            try:
                queue.stop_consuming()
            except Exception as e:
                logging.warning("aggregation_stop_input_queue_error | id=%s | error=%s", ID, e)

        for exchange in self.input_exchanges:
            try:
                exchange.stop_consuming()
            except Exception as e:
                logging.warning("aggregation_stop_input_exchange_error | id=%s | error=%s", ID, e)

    def close(self):
        logging.info("Closing aggregation with id %s", ID)
        for queue in self.input_queues.values():
            try:
                queue.close()
            except Exception as e:
                logging.warning("aggregation_input_close_error | id=%s | error=%s", ID, e)

        for exchange in self.input_exchanges:
            try:
                exchange.close()
            except Exception as e:
                logging.warning("aggregation_input_exchange_close_error | id=%s | error=%s", ID, e)

        for queue in self.output_queues.values():
            try:
                queue.close()
            except Exception as e:
                logging.warning("aggregation_output_close_error | id=%s | error=%s", ID, e)

        for exchange in self.output_exchanges:
            try:
                exchange.close()
            except Exception as e:
                logging.warning("aggregation_output_exchange_close_error | id=%s | error=%s", ID, e)