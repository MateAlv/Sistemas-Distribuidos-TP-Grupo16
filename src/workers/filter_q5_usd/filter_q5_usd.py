import logging
import os
import threading
import time

from common.eof_coordinator import EofCoordinator, BroadcastAction, FlushAction, SendAnswerAction
from common.middleware import (
    LazyQueue,
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
FILTER_Q5_USD_AMOUNT = int(os.environ.get("FILTER_Q5_USD_AMOUNT", "1"))
FILTER_Q5_USD_PREFIX = os.environ.get("FILTER_Q5_USD_PREFIX", "filter_q5_usd")
MODE = "broadcast"  # Only broadcast mode is supported for the coordinator in this worker.


class FilterQ5UsdWorker:
    def __init__(self):
        self.coordinator = EofCoordinator(
            instance_id=ID,
            total_instances=FILTER_Q5_USD_AMOUNT,
            control_queue_prefix=f"{FILTER_Q5_USD_PREFIX}_control",
            response_queue_prefix=f"{FILTER_Q5_USD_PREFIX}_response",
            mode=MODE,
        )

        self.input_queue = MessageMiddlewareQueueRabbitMQ(MOM_HOST, INPUT_QUEUE)
        self.output_exchanges = self._new_output_exchanges()

        # Named control queue senders for the main thread (EOF_RECEIVED broadcast).
        # Pika connections are not thread-safe; each thread creates its own senders.
        self._main_control_senders = self._new_control_senders()

        self._ctrl_ser = ControlMessageSerializer()
        self._proto = InternalProtocol()
        self._tx_ser = TransactionSerializer()

        self.rates_manager = RatesManager(cache_path="")
        self.rates_loaded = False
        self.rates_lock = threading.Lock()

        self.lock = threading.Lock()
        self.processed_by_client: dict[int, int] = {}
        self.forwarded_by_client: dict[int, int] = {}
        self._agg_seq_by_client: dict[int, int] = {}

        self.control_consumer: MessageMiddlewareQueueRabbitMQ | None = None
        self.response_consumer: MessageMiddlewareQueueRabbitMQ | None = None
        self.control_thread: threading.Thread | None = None
        self.response_thread: threading.Thread | None = None
        self.closed = False
        self._shutdown = threading.Event()

    # ---------- connection factories ----------

    def _new_output_exchanges(self):
        return [
            MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            for i in range(AGGREGATION_AMOUNT)
        ]

    def _new_control_senders(self) -> dict:
        return {
            self.coordinator.control_queue_for(i): LazyQueue(
                MOM_HOST, self.coordinator.control_queue_for(i)
            )
            for i in range(FILTER_Q5_USD_AMOUNT)
        }

    def _new_response_senders(self) -> dict:
        return {
            self.coordinator.response_queue_for(i): LazyQueue(
                MOM_HOST, self.coordinator.response_queue_for(i)
            )
            for i in range(FILTER_Q5_USD_AMOUNT)
        }

    # ---------- helpers ----------

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

    def _next_agg_seq(self, client_id: int) -> int:
        seq = self._agg_seq_by_client.get(client_id, 0)
        self._agg_seq_by_client[client_id] = (seq + 1) & 0xFFFFFFFF
        return seq

    def _addressed_packet(
        self, msg_type: MessageType, client_id: int, seq: int, payload: bytes
    ) -> bytes:
        return self._proto.create_addressed_packet(
            msg_type=msg_type,
            client_id_bytes=client_id.to_bytes(16, byteorder="big"),
            sender_id=ID,
            seq=seq,
            payload=payload,
        )

    def _eof_payload(self, expected_total: int) -> bytes:
        return self._ctrl_ser.serialize(
            ControlMessage(sender_id=ID, expected_total=expected_total, processed_count=0)
        )

    def _forward_transaction(self, payload: bytes, client_id: int):
        shard = client_id % AGGREGATION_AMOUNT
        seq = self._next_agg_seq(client_id)
        self.output_exchanges[shard].send(
            self._addressed_packet(MessageType.DATA, client_id, seq, payload)
        )

    def _forward_eof_to_aggregators(
        self, client_id: int, total_forwarded: int, exchanges
    ):
        eof_payload = self._eof_payload(total_forwarded)
        for index, exchange in enumerate(exchanges):
            seq = self._next_agg_seq(client_id)
            exchange.send(self._addressed_packet(MessageType.EOF, client_id, seq, eof_payload))
            logging.info(
                "filter_q5_usd_forward_eof | id=%s | client_id=%s | "
                "agg_index=%s | total_forwarded=%s",
                ID, client_id, index, total_forwarded,
            )

    def _do_broadcast(self, action: BroadcastAction, control_senders: dict):
        if action.sleep_before > 0:
            time.sleep(action.sleep_before)
        for qname in action.queue_names:
            control_senders[qname].send(action.message)

    # ---------- data path ----------

    def _process_data(self, client_id: int, payload: bytes):
        self._load_rates()
        transactions = self._tx_ser.deserialize_batch(payload)
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

            self._forward_transaction(
                self._tx_ser.serialize(transaction), client_id
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

        if should_log_progress(processed_total):
            logging.info(
                "filter_q5_usd_data_batch | id=%s | client_id=%s | "
                "batch_size=%s | forwarded_in_batch=%s | "
                "processed_total=%s | forwarded_total=%s",
                ID, client_id, processed_delta, forwarded_in_batch,
                processed_total, forwarded_total,
            )

    def _handle_upstream_eof(self, client_id: int, payload: bytes):
        ctrl = self._ctrl_ser.deserialize(payload)
        expected_total = ctrl.expected_total

        with self.lock:
            count = self.processed_by_client.get(client_id, 0)
            fwd = self.forwarded_by_client.get(client_id, 0)
            action = self.coordinator.on_upstream_eof(
                client_id, expected_total, count, fwd
            )

        logging.info(
            "filter_q5_usd_upstream_eof | id=%s | client_id=%s | expected_total=%s",
            ID, client_id, expected_total,
        )

        if isinstance(action, BroadcastAction):
            self._do_broadcast(action, self._main_control_senders)

        elif isinstance(action, FlushAction):
            # N==1: coordinator returns total_forwarded=current_forwarded directly
            with self.lock:
                self.forwarded_by_client.pop(client_id, None)
                self.processed_by_client.pop(client_id, None)
            logging.info(
                "filter_q5_usd_eof_ready | id=%s | client_id=%s | total_fwd=%s",
                ID, client_id, action.total_forwarded,
            )
            self._forward_eof_to_aggregators(
                client_id, action.total_forwarded, self.output_exchanges
            )

    def _process_message(self, message: bytes):
        msg_type, client_id, payload = self._proto.unpack_packet(message)
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

    def _handle_control(self, message, ack, nack, response_senders: dict):
        try:
            msg_type, client_id, ctrl = self.coordinator.parse_message(message)
        except Exception:
            logging.exception("filter_q5_usd_control_parse_error | id=%s", ID)
            nack()
            return

        with self.lock:
            count = self.processed_by_client.get(client_id, 0)
            fwd = self.forwarded_by_client.get(client_id, 0)
            action = self.coordinator.process_control_message(
                msg_type, client_id, ctrl, count, fwd
            )

        if action is None:
            ack()
            return

        if isinstance(action, SendAnswerAction):
            try:
                response_senders[action.queue_name].send(action.message)
                ack()
            except Exception:
                logging.exception(
                    "filter_q5_usd_send_answer_error | id=%s | client_id=%s", ID, client_id
                )
                with self.lock:
                    self.coordinator.clear_pending_eof(client_id)
                nack()

        elif isinstance(action, FlushAction):
            # Non-leader: no buffered output to flush; send FLUSH_ACK and cleanup
            with self.lock:
                fwd_final = self.forwarded_by_client.get(client_id, 0)
            ack_msg = self.coordinator.build_flush_ack(client_id, fwd_final)
            try:
                response_senders[action.ack_queue].send(ack_msg)
            except Exception:
                logging.exception(
                    "filter_q5_usd_flush_ack_error | id=%s | client_id=%s", ID, client_id
                )
                nack()
                return
            with self.lock:
                self.forwarded_by_client.pop(client_id, None)
                self.processed_by_client.pop(client_id, None)
                self.coordinator.cleanup_client(client_id)
            ack()

        else:
            logging.warning(
                "filter_q5_usd_unexpected_control_action | id=%s | action=%s", ID, action
            )
            ack()

    def _start_control_consumer(self):
        consumer = MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, self.coordinator.my_control_queue()
        )
        self.control_consumer = consumer
        response_senders = self._new_response_senders()
        try:
            if not self._shutdown.is_set():
                consumer.start_consuming(
                    lambda msg, ack, nack: self._handle_control(
                        msg, ack, nack, response_senders
                    )
                )
        finally:
            for q in response_senders.values():
                try:
                    q.close()
                except Exception:
                    pass
            try:
                consumer.close()
            except Exception:
                pass

    # ---------- response path (líder) ----------

    def _handle_response(self, message, ack, nack, control_senders: dict, output_exchanges):
        try:
            msg_type, client_id, ctrl = self.coordinator.parse_message(message)
        except Exception:
            logging.exception("filter_q5_usd_response_parse_error | id=%s", ID)
            nack()
            return

        own_fwd = 0
        with self.lock:
            action = self.coordinator.process_control_message(msg_type, client_id, ctrl)
            if isinstance(action, FlushAction) and action.is_leader:
                # Grab own forwarded under the same lock before any other thread modifies it.
                # Coordinator cleans its own state in _on_flush_ack; no cleanup_client needed.
                own_fwd = self.forwarded_by_client.pop(client_id, 0)
                self.processed_by_client.pop(client_id, None)

        if action is None:
            ack()
            return

        if isinstance(action, BroadcastAction):
            try:
                self._do_broadcast(action, control_senders)
                ack()
            except Exception:
                logging.exception(
                    "filter_q5_usd_broadcast_error | id=%s | client_id=%s", ID, client_id
                )
                nack()

        elif isinstance(action, FlushAction) and action.is_leader:
            # total_forwarded = sum of N-1 FLUSH_ACKs; add leader's own contribution
            total_fwd = action.total_forwarded + own_fwd
            logging.info(
                "filter_q5_usd_eof_ready | id=%s | client_id=%s | "
                "acks_fwd=%s | own_fwd=%s | total_fwd=%s",
                ID, client_id, action.total_forwarded, own_fwd, total_fwd,
            )
            try:
                self._forward_eof_to_aggregators(client_id, total_fwd, output_exchanges)
                ack()
            except Exception:
                logging.exception(
                    "filter_q5_usd_forward_eof_error | id=%s | client_id=%s", ID, client_id
                )
                nack()

        else:
            logging.warning(
                "filter_q5_usd_unexpected_response_action | id=%s | action=%s", ID, action
            )
            ack()

    def _start_response_consumer(self):
        consumer = MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, self.coordinator.my_response_queue()
        )
        self.response_consumer = consumer
        control_senders = self._new_control_senders()
        output_exchanges = self._new_output_exchanges()
        try:
            if not self._shutdown.is_set():
                consumer.start_consuming(
                    lambda msg, ack, nack: self._handle_response(
                        msg, ack, nack, control_senders, output_exchanges
                    )
                )
        finally:
            for q in control_senders.values():
                try:
                    q.close()
                except Exception:
                    pass
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
        resources = (
            [self.input_queue]
            + list(self.output_exchanges)
            + list(self._main_control_senders.values())
        )
        for resource in resources:
            try:
                resource.close()
            except Exception:
                pass
