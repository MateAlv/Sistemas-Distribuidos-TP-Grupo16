import logging
import os
import struct
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

# Contrato (Sum -> Agg -> Join) usado por Q3
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]

# Contrato (Q5)
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
DATA_QUEUE = os.environ["DATA_QUEUE"]

# Contrato (Q2)
SUM_Q2_EXCHANGE = os.environ["SUM_Q2_EXCHANGE"]
AGG_Q2_OUTPUT_QUEUE = os.environ["AGG_Q2_OUTPUT_QUEUE"]

# TODO: ver si borrar
JOIN_Q3_EXCHANGE = os.environ["JOIN_Q3_EXCHANGE"]
AGG_Q3_DATA_QUEUE = os.environ["AGG_Q3_DATA_QUEUE"]
AGG_Q3_OUTPUT_QUEUE = os.environ["AGG_Q3_OUTPUT_QUEUE"]

# Este worker implementa agregación para:
#   - "Q2": conserva el máximo monto por banco emisor y, al EOF, emite esos máximos como DATA seguido de un EOF con expected_total.
#   - "Q5": cuenta transacciones por cliente y emite el total final en el EOF.
#   - "Q3": acumula parciales (sum, count) por payment_format provenientes de
#           los Sum Q3 y, al recibir un EOF de cada Sum, emite el promedio por
#           payment_format hacia Join Q3.


class Q3PartialSerializer:
    """Wire de los parciales Sum Q3 -> Agg Q3: len(pf) | pf utf-8 | sum(d) | count(Q).
    """

    _TAIL = struct.calcsize("!dQ")

    @staticmethod
    def serialize(payment_format: str, total: float, count: int) -> bytes:
        pf = payment_format.encode("utf-8")
        return struct.pack("!H", len(pf)) + pf + struct.pack("!dQ", float(total), int(count))

    @staticmethod
    def deserialize(data: bytes):
        (pf_len,) = struct.unpack("!H", data[:2])
        pf = data[2:2 + pf_len].decode("utf-8")
        total, count = struct.unpack(
            "!dQ", data[2 + pf_len:2 + pf_len + Q3PartialSerializer._TAIL]
        )
        return pf, total, count


class Q3ResultSerializer:
    """Wire del resultado Agg Q3 -> Join Q3: len(pf) | pf utf-8 | average(d)."""

    @staticmethod
    def serialize(payment_format: str, average: float) -> bytes:
        pf = payment_format.encode("utf-8")
        return struct.pack("!H", len(pf)) + pf + struct.pack("!d", float(average))

    @staticmethod
    def deserialize(data: bytes):
        (pf_len,) = struct.unpack("!H", data[:2])
        pf = data[2:2 + pf_len].decode("utf-8")
        (average,) = struct.unpack("!d", data[2 + pf_len:2 + pf_len + 8])
        return pf, average


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

        # caso query 3 (Sum -> Agg -> Join)
        if CONFIGURATION == C_Q3:
            # Entrada: parciales (sum, count) por payment_format desde los
            # Sum Q3, shardeados por hash(payment_format) % AGGREGATION_AMOUNT.
            self.input_exchanges.append(
                middleware.MessageMiddlewareExchangeRabbitMQ(
                    MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"]
                )
            )
            # Salida: promedio por payment_format hacia Join Q3.
            self.output_queues[OUTPUT_QUEUE] = middleware.MessageMiddlewareQueueRabbitMQ(
                MOM_HOST, OUTPUT_QUEUE
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

        # Q3: acumulación de parciales por (client_id, payment_format)
        # y conteo de EOFs recibidos desde los Sum Q3.
        self.q3_partials: dict[int, dict[str, list]] = {}
        self.q3_sum_eofs: dict[int, int] = {}

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
        self._deliver_output(packet)

    def _record_max_transaction(self, client_id: int, transaction: Transaction) -> None:
        with self.lock:
            current_by_bank = self.max_tx_by_client.setdefault(client_id, {})
            current = current_by_bank.get(transaction.from_bank)
            if current is None or transaction.amount > current.amount:
                current_by_bank[transaction.from_bank] = transaction

    def _deliver_output(self, packet: bytes) -> None:
        try:
            if CONFIGURATION == C_Q2:
                queue = self.output_queues.get(AGG_Q2_OUTPUT_QUEUE)
            elif CONFIGURATION == C_Q5:
                queue = self.output_queues.get(DATA_QUEUE)
            elif CONFIGURATION == C_Q3:
                queue = self.output_queues.get(OUTPUT_QUEUE)
            else:
                # fallback: primera queue disponible
                queue = next(iter(self.output_queues.values()), None)

            if queue is None:
                logging.warning("No output queue configured for mode %s", CONFIGURATION)
                return

            queue.send(packet)
        except Exception as e:
            logging.error("aggregation_output_send_error | id=%s | error=%s", ID, e)

    def _send_max_transactions(self, client_id: int) -> None:
        with self.lock:
            transactions = list(self.max_tx_by_client.get(client_id, {}).values())

        for transaction in transactions:
            packet = self.internal_packet_serializer.create_packet(
                msg_type=MessageType.DATA,
                client_id_bytes=client_id.to_bytes(16, byteorder="big"),
                payload=self.transaction_serializer.serialize(transaction),
            )
            self._deliver_output(packet)

    def _process_data_message(self, message: bytes) -> None:
        try:
            msg_type, client_id, payload = self.internal_packet_serializer.unpack_packet(
                message
            )

            if msg_type == MessageType.DATA:
                if CONFIGURATION == C_Q3:
                    pf, partial_sum, partial_count = Q3PartialSerializer.deserialize(payload)
                    with self.lock:
                        client_acc = self.q3_partials.setdefault(client_id, {})
                        acc = client_acc.setdefault(pf, [0.0, 0])
                        acc[0] += partial_sum
                        acc[1] += partial_count
                    return

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
                if CONFIGURATION == C_Q3:
                    with self.lock:
                        received = self.q3_sum_eofs.get(client_id, 0) + 1
                        self.q3_sum_eofs[client_id] = received
                    # Esperamos un EOF por cada Sum Q3 antes de promediar.
                    if received < SUM_AMOUNT:
                        return
                    with self.lock:
                        partials = self.q3_partials.pop(client_id, {})
                        self.q3_sum_eofs.pop(client_id, None)
                    for pf, (total, count) in partials.items():
                        average = total / count if count else 0.0
                        packet = self.internal_packet_serializer.create_packet(
                            msg_type=MessageType.DATA,
                            client_id_bytes=client_id.to_bytes(16, byteorder="big"),
                            payload=Q3ResultSerializer.serialize(pf, average),
                        )
                        self._deliver_output(packet)
                    control_payload = ControlMessageSerializer().serialize(
                        ControlMessage(sender_id=ID, expected_total=len(partials), processed_count=0)
                    )
                    eof_packet = self.internal_packet_serializer.create_packet(
                        msg_type=MessageType.EOF,
                        client_id_bytes=client_id.to_bytes(16, byteorder="big"),
                        payload=control_payload,
                    )
                    self._deliver_output(eof_packet)
                    return
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
                        self._deliver_output(packet)

                    # send EOF with expected_total = msgs_sent using control message format
                    control_payload = ControlMessageSerializer().serialize(
                        ControlMessage(sender_id=ID, expected_total=msgs_sent, processed_count=0)
                    )
                    eof_packet = self.internal_packet_serializer.create_packet(
                        msg_type=MessageType.EOF,
                        client_id_bytes=client_id.to_bytes(16, byteorder="big"),
                        payload=control_payload,
                    )
                    self._deliver_output(eof_packet)

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
            primary_queue = next(iter(self.input_queues.values()), None)
            # La input queue principal corre en el thread principal.
            if primary_queue is not None:
                primary_queue.start_consuming(self.process_data_messages)
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