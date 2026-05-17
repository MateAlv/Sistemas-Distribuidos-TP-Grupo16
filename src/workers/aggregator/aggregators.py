import logging
import os
import threading

from common import middleware
from common.domain.transaction import Transaction
from common.message_protocol.aggregation_serializer import AggregationSerializer
from common.message_protocol.common import MessageType
from common.message_protocol.internal import InternalProtocol
from common.message_protocol.transaction_serializer import TransactionSerializer


ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]


class AggregatorWorker:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )

        self.transaction_serializer = TransactionSerializer()
        self.aggregation_serializer = AggregationSerializer()
        self.internal_packet_serializer = InternalProtocol()

        self.lock = threading.Lock()
        self.active = True
        self.counts_by_client: dict[int, int] = {}

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

    def _process_data_message(self, message: bytes, ack, nack) -> None:
        try:
            msg_type, client_id, payload = self.internal_packet_serializer.unpack_packet(
                message
            )

            if msg_type == MessageType.DATA:
                transaction: Transaction = self.transaction_serializer.deserialize(payload)
                logging.debug(
                    "aggregation_data | id=%s | client_id=%s | date=%s | amount=%s | format=%s",
                    ID,
                    client_id,
                    transaction.date,
                    transaction.amount,
                    transaction.format,
                )
                current_count = self._increment_count(client_id)
                self._send_count(client_id, MessageType.AGGREGATED_COUNT, current_count)
                ack()
                return

            if msg_type == MessageType.EOF:
                final_count = self._current_count(client_id)
                self._send_count(client_id, MessageType.EOF, final_count)
                ack()
                return

            raise ValueError(f"unsupported message type: {msg_type}")
        except Exception as e:
            logging.error("aggregation_message_error | id=%s | error=%s", ID, e)
            nack()

    def process_data_messages(self, message, ack, nack):
        try:
            self._process_data_message(message, ack, nack)
        except Exception as e:
            logging.error("aggregation_callback_error | id=%s | error=%s", ID, e)
            nack()

    def start(self):
        logging.info(
            "aggregation_start | id=%s | mom_host=%s | input_queue=%s | output_queue=%s",
            ID,
            MOM_HOST,
            INPUT_QUEUE,
            OUTPUT_QUEUE,
        )
        try:
            self.input_queue.start_consuming(self.process_data_messages)
        finally:
            self.close()

    def handle_sigterm(self):
        logging.info("Received SIGTERM in aggregation with id %s, shutting down", ID)
        self.active = False
        self.input_queue.stop_consuming()
        self.close()

    def close(self):
        logging.info("Closing aggregation with id %s", ID)
        try:
            self.input_queue.close()
        except Exception as e:
            logging.warning("aggregation_input_close_error | id=%s | error=%s", ID, e)

        try:
            self.output_queue.close()
        except Exception as e:
            logging.warning("aggregation_output_close_error | id=%s | error=%s", ID, e)