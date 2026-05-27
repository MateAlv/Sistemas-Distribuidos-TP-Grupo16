import logging
import os
import threading

from common.middleware import (
    MessageMiddlewareQueueRabbitMQ,
    MessageMiddlewareExchangeRabbitMQ,
    MessageMiddlewareRpcClientRabbitMQ,
)
from common.logging_utils import should_log_progress
from common.message_protocol.internal import InternalProtocol, TransactionSerializer
from common.message_protocol.internal.common import ControlMessage, MessageType
from common.message_protocol.internal.control_message_serializer import ControlMessageSerializer
from common.rates.rates_manager import RatesManager

CURRENCY_NAME_TO_ISO = {
    "US Dollar": "USD",
    "Euro": "EUR",
    "UK Pound": "GBP",
    "Yen": "JPY",
    "Swiss Franc": "CHF",
    "Canadian Dollar": "CAD",
    "Australian Dollar": "AUD",
    "Mexican Peso": "MXN",
    "Brazil Real": "BRL",
    "Yuan": "CNY",
    "Rupee": "INR",
    "Ruble": "RUB",
    "Saudi Riyal": "SAR",
    "Shekel": "ILS",
    "Swedish Krona": "SEK",
    "New Zealand Dollar": "NZD",
    "Singapore Dollar": "SGD",
    "Hong Kong Dollar": "HKD",
    "Norwegian Krone": "NOK",
    "South Korean Won": "KRW",
    "Turkish Lira": "TRY",
    "South African Rand": "ZAR",
    "Thai Baht": "THB",
    "Polish Zloty": "PLN",
    "Czech Koruna": "CZK",
    "Philippine Peso": "PHP",
    "Indonesian Rupiah": "IDR",
    "Malaysian Ringgit": "MYR",
    "Hungarian Forint": "HUF",
    "Icelandic Krona": "ISK",
    "Croatian Kuna": "HRK",
    "Romanian Leu": "RON",
    "Danish Krone": "DKK",
    "Bulgarian Lev": "BGN",
    "Bitcoin": "BTC"
}

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
RATES_REQUEST_QUEUE = os.environ.get("RATES_REQUEST_QUEUE", "rates_requests")
START_DATE = os.environ.get("Q5_START_DATE", "2022-09-01")
END_DATE = os.environ.get("Q5_END_DATE", "2022-09-05")
# Tamaño del cluster de filter_q5_usd. Cuando es 1 se atajan los broadcasts
# (cierre directo). Con N>1 se usa el protocolo de líder ocasional: el worker
# que recibe el EOF coordina con sus pares para totalizar processed/forwarded
# antes de emitir el EOF al aggregator.
FILTER_Q5_USD_AMOUNT = int(os.environ.get("FILTER_Q5_USD_AMOUNT", "1"))
FILTER_Q5_USD_PREFIX = os.environ.get("FILTER_Q5_USD_PREFIX", "filter_q5_usd")
CONTROL_EXCHANGE = f"{FILTER_Q5_USD_PREFIX}_control"
RESPONSE_QUEUE_PREFIX = f"{FILTER_Q5_USD_PREFIX}_response"


class FilterQ5UsdWorker:
    def __init__(self):
        self.input_queue = MessageMiddlewareQueueRabbitMQ(MOM_HOST, INPUT_QUEUE)

        self.control_sender = MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, CONTROL_EXCHANGE, [CONTROL_EXCHANGE]
        )
        self.response_queue_name = f"{RESPONSE_QUEUE_PREFIX}_{ID}"

        self.output_exchanges = self._new_output_exchanges()

        self.internal_protocol = InternalProtocol()
        self.transaction_serializer = TransactionSerializer()
        self.control_serializer = ControlMessageSerializer()

        self.rates_manager = RatesManager(cache_path="")
        self.rates_loaded = False
        self.rates_lock = threading.Lock()

        self.lock = threading.Lock()
        self.processed_by_client: dict[int, int] = {}
        self.forwarded_by_client: dict[int, int] = {}
        # (expected_total, leader_id): EOF visto para ese cliente. Mientras esté
        # presente, todo DATA que llegue se reporta como delta al líder.
        self.pending_eof_by_client: dict[int, tuple[int, int]] = {}
        # Estado del líder: agregados de processed/forwarded reportados por
        # los demás workers + el expected_total guardado al recibir el EOF
        # de upstream.
        self.leader_processed_by_client: dict[int, int] = {}
        self.leader_forwarded_by_client: dict[int, int] = {}
        self.leader_expected_by_client: dict[int, int] = {}

        self.control_consumer: MessageMiddlewareExchangeRabbitMQ | None = None
        self.response_consumer: MessageMiddlewareQueueRabbitMQ | None = None
        self.control_thread: threading.Thread | None = None
        self.response_thread: threading.Thread | None = None
        self.closed = False
        # Aborts an in-flight rates RPC on SIGTERM.
        self._shutdown = threading.Event()

    # ---------- helpers ----------

    def _new_output_exchanges(self):
        return [
            MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            for i in range(AGGREGATION_AMOUNT)
        ]

    def _packet(self, msg_type: MessageType, client_id: int, payload: bytes) -> bytes:
        return self.internal_protocol.create_packet(
            msg_type=msg_type,
            client_id_bytes=client_id.to_bytes(16, byteorder="big"),
            payload=payload,
        )

    def _control_payload(
        self, sender_id: int, expected_total: int, processed_count: int
    ) -> bytes:
        return self.control_serializer.serialize(
            ControlMessage(
                sender_id=sender_id,
                expected_total=expected_total,
                processed_count=processed_count,
            )
        )

    def _load_rates(self):
        with self.rates_lock:
            if self.rates_loaded:
                return
            logging.info("filter_q5_usd_rpc_start | id=%s | requesting rates", ID)
            with MessageMiddlewareRpcClientRabbitMQ(MOM_HOST, RATES_REQUEST_QUEUE) as client:
                client.connect()
                response = client.call(
                    b"get_rates", timeout=120, cancel_event=self._shutdown
                )
            self.rates_manager.load_from_payload(response)
            self.rates_loaded = True
            logging.info("filter_q5_usd_rpc_done | id=%s | rates_loaded", ID)

    def _convert_to_usd(self, amount: float, currency: str, date: str) -> float:
        if currency == "US Dollar":
            return amount
        iso = CURRENCY_NAME_TO_ISO.get(currency)
        if iso is None:
            raise ValueError(f"Unknown currency: {currency}")
        rate = self.rates_manager.get_rate(date, iso)
        return amount * rate

    def _in_date_range(self, date: str) -> bool:
        normalized = date[:10].replace("/", "-")
        return START_DATE <= normalized <= END_DATE

    def _forward_transaction(self, payload: bytes, client_id: int):
        shard = client_id % AGGREGATION_AMOUNT
        self.output_exchanges[shard].send(
            self._packet(MessageType.DATA, client_id, payload)
        )

    def _forward_eof_to_aggregators(
        self, client_id: int, expected_total: int, exchanges
    ):
        payload = self._control_payload(
            sender_id=ID, expected_total=expected_total, processed_count=0
        )
        for index, exchange in enumerate(exchanges):
            exchange.send(self._packet(MessageType.EOF, client_id, payload))
            logging.info(
                "filter_q5_usd_forward_eof_to_aggregator | id=%s | client_id=%s | "
                "aggregation_index=%s | expected_total=%s",
                ID, client_id, index, expected_total,
            )

    def _broadcast_eof(self, client_id: int, expected_total: int):
        self.control_sender.send(
            self._packet(
                MessageType.EOF_RECEIVED,
                client_id,
                self._control_payload(ID, expected_total, 0),
            )
        )

    def _report_to_leader(
        self,
        client_id: int,
        leader_id: int,
        processed_count: int,
        forwarded_count: int,
    ):
        response_queue = MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, f"{RESPONSE_QUEUE_PREFIX}_{leader_id}"
        )
        try:
            response_queue.send(
                self._packet(
                    MessageType.PROCESSED_ANSWER,
                    client_id,
                    self._control_payload(
                        sender_id=ID,
                        expected_total=forwarded_count,
                        processed_count=processed_count,
                    ),
                )
            )
        finally:
            response_queue.close()

    def _cleanup_client(self, client_id: int):
        self.processed_by_client.pop(client_id, None)
        self.forwarded_by_client.pop(client_id, None)
        self.pending_eof_by_client.pop(client_id, None)
        self.leader_processed_by_client.pop(client_id, None)
        self.leader_forwarded_by_client.pop(client_id, None)
        self.leader_expected_by_client.pop(client_id, None)

    # ---------- data path ----------

    def _process_data(self, client_id: int, payload: bytes):
        self._load_rates()
        # El payload puede traer 1 o N transactions concatenadas.
        transactions = self.transaction_serializer.deserialize_batch(payload)
        if not transactions:
            return

        forwarded_in_batch = 0
        for transaction in transactions:
            if not self._in_date_range(transaction.date):
                continue
            try:
                amount_usd = self._convert_to_usd(
                    transaction.amount,
                    transaction.currency,
                    transaction.date[:10].replace("/", "-"),
                )
            except ValueError as e:
                logging.warning("filter_q5_usd_conversion_error | error=%s", e)
                continue
            if amount_usd >= 1.0:
                continue

            # Forward individual: el aggregator espera 1 transaction por DATA.
            self._forward_transaction(
                self.transaction_serializer.serialize(transaction), client_id
            )
            forwarded_in_batch += 1

        processed_delta = len(transactions)
        with self.lock:
            self.processed_by_client[client_id] = (
                self.processed_by_client.get(client_id, 0) + processed_delta
            )
            if forwarded_in_batch:
                self.forwarded_by_client[client_id] = (
                    self.forwarded_by_client.get(client_id, 0) + forwarded_in_batch
                )
            processed_total = self.processed_by_client[client_id]
            forwarded_total = self.forwarded_by_client.get(client_id, 0)
            pending = self.pending_eof_by_client.get(client_id)

        if should_log_progress(processed_total):
            logging.info(
                "filter_q5_usd_data_batch | id=%s | client_id=%s | batch_size=%s | "
                "forwarded_in_batch=%s | processed_total=%s | forwarded_total=%s | "
                "pending_eof=%s",
                ID,
                client_id,
                processed_delta,
                forwarded_in_batch,
                processed_total,
                forwarded_total,
                pending is not None,
            )

        if pending is None:
            return

        # Late DATA: ya vimos el EOF, reportamos el delta agregado al líder.
        _, leader_id = pending
        self._report_to_leader(
            client_id,
            leader_id,
            processed_count=processed_delta,
            forwarded_count=forwarded_in_batch,
        )

    def _handle_upstream_eof(self, client_id: int, payload: bytes):
        control = self.control_serializer.deserialize(payload)
        expected_total = control.expected_total

        with self.lock:
            self.leader_expected_by_client[client_id] = expected_total

        logging.info(
            "filter_q5_usd_upstream_eof | id=%s | client_id=%s | expected_total=%s",
            ID, client_id, expected_total,
        )
        self._broadcast_eof(client_id, expected_total)

    def _process_message(self, message: bytes):
        msg_type, client_id, payload = self.internal_protocol.unpack_packet(message)

        if msg_type == MessageType.DATA:
            self._process_data(client_id, payload)
        elif msg_type == MessageType.EOF:
            self._handle_upstream_eof(client_id, payload)
        else:
            raise ValueError(f"Unexpected filter_q5_usd message type: {msg_type}")

    def process_messages(self, message, ack, nack):
        try:
            self._process_message(message)
            ack()
        except Exception as e:
            logging.error("filter_q5_usd_error | id=%s | error=%s", ID, e)
            nack()

    # ---------- control path ----------

    def _handle_eof_broadcast(self, message: bytes, ack, nack, output_exchanges):
        try:
            msg_type, client_id, payload = self.internal_protocol.unpack_packet(message)
            if msg_type != MessageType.EOF_RECEIVED:
                raise ValueError(
                    f"unexpected filter_q5_usd control message type: {msg_type}"
                )

            control = self.control_serializer.deserialize(payload)
            leader_id = control.sender_id
            expected_total = control.expected_total

            duplicate = False
            with self.lock:
                if client_id in self.pending_eof_by_client:
                    duplicate = True
                else:
                    self.pending_eof_by_client[client_id] = (
                        expected_total,
                        leader_id,
                    )
                    processed_snapshot = self.processed_by_client.get(client_id, 0)
                    forwarded_snapshot = self.forwarded_by_client.get(client_id, 0)

            if duplicate:
                logging.info(
                    "filter_q5_usd_duplicate_eof_control | id=%s | client_id=%s | "
                    "leader_id=%s",
                    ID, client_id, leader_id,
                )
                ack()
                return

            logging.info(
                "filter_q5_usd_eof_control_snapshot | id=%s | client_id=%s | "
                "leader_id=%s | processed_count=%s | forwarded_count=%s | "
                "expected_total=%s",
                ID,
                client_id,
                leader_id,
                processed_snapshot,
                forwarded_snapshot,
                expected_total,
            )
            self._report_to_leader(
                client_id,
                leader_id,
                processed_count=processed_snapshot,
                forwarded_count=forwarded_snapshot,
            )

            # Caso especial: si yo soy el único worker, mi snapshot ya es total.
            # El protocolo igual se cierra al recibir mi propia PROCESSED_ANSWER.
            ack()
        except Exception:
            logging.exception("filter_q5_usd_control_error | id=%s", ID)
            nack()

    def _handle_leader_report(self, message: bytes, ack, nack, output_exchanges):
        try:
            msg_type, client_id, payload = self.internal_protocol.unpack_packet(message)
            if msg_type != MessageType.PROCESSED_ANSWER:
                raise ValueError(
                    f"unexpected filter_q5_usd response message type: {msg_type}"
                )

            control = self.control_serializer.deserialize(payload)
            should_close = False
            forwarded_total = 0

            with self.lock:
                self.leader_processed_by_client[client_id] = (
                    self.leader_processed_by_client.get(client_id, 0)
                    + control.processed_count
                )
                self.leader_forwarded_by_client[client_id] = (
                    self.leader_forwarded_by_client.get(client_id, 0)
                    + control.expected_total
                )
                expected_total = self.leader_expected_by_client.get(client_id)

                if (
                    expected_total is not None
                    and self.leader_processed_by_client[client_id] >= expected_total
                ):
                    should_close = True
                    forwarded_total = self.leader_forwarded_by_client[client_id]
                    self._cleanup_client(client_id)

            if should_close:
                logging.info(
                    "filter_q5_usd_eof_ready | id=%s | client_id=%s | "
                    "expected_total=%s | forwarded_total=%s",
                    ID,
                    client_id,
                    expected_total,
                    forwarded_total,
                )
                self._forward_eof_to_aggregators(
                    client_id, forwarded_total, output_exchanges
                )

            ack()
        except Exception:
            logging.exception("filter_q5_usd_response_error | id=%s", ID)
            nack()

    def _start_control_consumer(self):
        consumer = MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, CONTROL_EXCHANGE, [CONTROL_EXCHANGE]
        )
        self.control_consumer = consumer
        output_exchanges = self._new_output_exchanges()
        try:
            if not self._shutdown.is_set():
                consumer.start_consuming(
                    lambda message, ack, nack: self._handle_eof_broadcast(
                        message, ack, nack, output_exchanges
                    )
                )
        finally:
            for exchange in output_exchanges:
                try:
                    exchange.close()
                except Exception:
                    pass
            try:
                consumer.close()
            except Exception:
                pass

    def _start_response_consumer(self):
        consumer = MessageMiddlewareQueueRabbitMQ(MOM_HOST, self.response_queue_name)
        self.response_consumer = consumer
        output_exchanges = self._new_output_exchanges()
        try:
            if not self._shutdown.is_set():
                consumer.start_consuming(
                    lambda message, ack, nack: self._handle_leader_report(
                        message, ack, nack, output_exchanges
                    )
                )
        finally:
            for exchange in output_exchanges:
                try:
                    exchange.close()
                except Exception:
                    pass
            try:
                consumer.close()
            except Exception:
                pass

    # ---------- lifecycle ----------

    def start(self):
        logging.info(
            "filter_q5_usd_start | id=%s | input=%s | aggregation_prefix=%s | "
            "aggregation_amount=%s | cluster_size=%s | date_range=[%s, %s]",
            ID, INPUT_QUEUE, AGGREGATION_PREFIX, AGGREGATION_AMOUNT,
            FILTER_Q5_USD_AMOUNT, START_DATE, END_DATE,
        )

        self.control_thread = threading.Thread(target=self._start_control_consumer)
        self.response_thread = threading.Thread(target=self._start_response_consumer)
        self.control_thread.start()
        self.response_thread.start()

        try:
            if not self._shutdown.is_set():
                self.input_queue.start_consuming(self.process_messages)
        except Exception as e:
            logging.error("filter_q5_usd_start_error | id=%s | error=%s", ID, e)
        finally:
            self.handle_sigterm()
            if self.control_thread is not None:
                self.control_thread.join(timeout=5)
            if self.response_thread is not None:
                self.response_thread.join(timeout=5)
            self.close()

    def handle_sigterm(self):
        if self.closed or self._shutdown.is_set():
            return
        logging.info("filter_q5_usd_shutdown | id=%s", ID)

        self._shutdown.set()
        self.input_queue.request_stop_consuming()
        if self.control_consumer is not None:
            self.control_consumer.request_stop_consuming()
        if self.response_consumer is not None:
            self.response_consumer.request_stop_consuming()

    def close(self):
        if self.closed:
            return
        self.closed = True
        for resource in (
            [self.input_queue, self.control_sender]
            + list(self.output_exchanges)
        ):
            try:
                resource.close()
            except Exception:
                pass
