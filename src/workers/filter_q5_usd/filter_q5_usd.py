import logging
import os

from common.middleware import (
    MessageMiddlewareQueueRabbitMQ,
    MessageMiddlewareExchangeRabbitMQ,
    MessageMiddlewareRpcClientRabbitMQ,
)
from common.message_protocol.internal import InternalProtocol, TransactionSerializer
from common.message_protocol.common import MessageType
from common.message_protocol.common.control_message import ControlMessage
from common.message_protocol.control_message_serializer import ControlMessageSerializer
from common.rates.rates_manager import RatesManager

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
RATES_REQUEST_QUEUE = os.environ.get("RATES_REQUEST_QUEUE", "rates_requests")
START_DATE = os.environ.get("Q5_START_DATE", "2022-09-01")
END_DATE = os.environ.get("Q5_END_DATE", "2022-09-05")


class FilterQ5UsdWorker:
    def __init__(self):
        self.input_queue = MessageMiddlewareQueueRabbitMQ(MOM_HOST, INPUT_QUEUE)

        self.output_exchanges: list[MessageMiddlewareExchangeRabbitMQ] = []
        for i in range(AGGREGATION_AMOUNT):
            key = f"{AGGREGATION_PREFIX}_{i}"
            self.output_exchanges.append(
                MessageMiddlewareExchangeRabbitMQ(
                    MOM_HOST, AGGREGATION_PREFIX, [key]
                )
            )

        self.internal_protocol = InternalProtocol()
        self.transaction_serializer = TransactionSerializer()
        self.control_serializer = ControlMessageSerializer()

        self.rates_manager = RatesManager(cache_path="")
        self.rates_loaded = False
        self.forwarded_by_client: dict[int, int] = {}
        self.closed_by_client: set[int] = set()

    def _load_rates(self):
        if self.rates_loaded:
            return
        logging.info("filter_q5_usd_rpc_start | requesting rates")
        with MessageMiddlewareRpcClientRabbitMQ(MOM_HOST, RATES_REQUEST_QUEUE) as client:
            client.connect()
            response = client.call(b"get_rates", timeout=30)
        self.rates_manager.load_from_payload(response)
        self.rates_loaded = True
        logging.info("filter_q5_usd_rpc_done | rates_loaded")

    def _convert_to_usd(self, amount: float, currency: str, date: str) -> float:
        if currency == "US Dollar":
            return amount
        rate = self.rates_manager.get_rate(date, currency)
        return amount * rate

    def _in_date_range(self, date: str) -> bool:
        normalized = date[:10].replace("/", "-")
        return START_DATE <= normalized <= END_DATE

    def _forward_transaction(self, payload: bytes, client_id: int):
        message = self.internal_protocol.create_packet(
            msg_type=MessageType.DATA,
            client_id_bytes=client_id.to_bytes(16, byteorder="big"),
            payload=payload,
        )
        shard = client_id % AGGREGATION_AMOUNT
        self.output_exchanges[shard].send(message)

    def _forward_eof(self, client_id: int, forwarded: int):
        control_payload = self.control_serializer.serialize(
            ControlMessage(sender_id=ID, expected_total=forwarded, processed_count=0)
        )
        message = self.internal_protocol.create_packet(
            msg_type=MessageType.EOF,
            client_id_bytes=client_id.to_bytes(16, byteorder="big"),
            payload=control_payload,
        )
        for exchange in self.output_exchanges:
            exchange.send(message)

    def _process_message(self, message: bytes):
        msg_type, client_id, payload = self.internal_protocol.unpack_packet(message)

        if client_id in self.closed_by_client:
            return

        if msg_type == MessageType.DATA:
            self._load_rates()
            transaction = self.transaction_serializer.deserialize(payload)

            if not self._in_date_range(transaction.date):
                return

            try:
                amount_usd = self._convert_to_usd(
                    transaction.amount, transaction.currency, transaction.date[:10].replace("/", "-")
                )
            except ValueError as e:
                logging.warning("filter_q5_usd_conversion_error | error=%s", e)
                return

            if amount_usd >= 1.0:
                return

            self.forwarded_by_client[client_id] = self.forwarded_by_client.get(client_id, 0) + 1
            self._forward_transaction(payload, client_id)

        elif msg_type == MessageType.EOF:
            forwarded = self.forwarded_by_client.pop(client_id, 0)
            self.closed_by_client.add(client_id)
            self._forward_eof(client_id, forwarded)
            logging.info(
                "filter_q5_usd_eof | client_id=%s | forwarded=%s",
                client_id, forwarded,
            )

    def process_messages(self, message, ack, nack):
        try:
            self._process_message(message)
            ack()
        except Exception as e:
            logging.error("filter_q5_usd_error | id=%s | error=%s", ID, e)
            nack()

    def start(self):
        logging.info(
            "filter_q5_usd_start | id=%s | input=%s | aggregation_prefix=%s | "
            "aggregation_amount=%s | date_range=[%s, %s]",
            ID, INPUT_QUEUE, AGGREGATION_PREFIX, AGGREGATION_AMOUNT,
            START_DATE, END_DATE,
        )
        try:
            self.input_queue.start_consuming(self.process_messages)
        except Exception as e:
            logging.error("filter_q5_usd_start_error | id=%s | error=%s", ID, e)
        finally:
            self.handle_sigterm()
            self.close()

    def handle_sigterm(self):
        logging.info("filter_q5_usd_shutdown | id=%s", ID)
        try:
            self.input_queue.stop_consuming()
        except Exception:
            pass

    def close(self):
        for resource in [self.input_queue] + self.output_exchanges:
            try:
                resource.close()
            except Exception:
                pass
