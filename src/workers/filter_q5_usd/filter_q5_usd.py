import hashlib
import logging
import os
import threading
import time

from common.eof_coordinator import EofCoordinator, BroadcastAction, FlushAction, SendAnswerAction
from common.fault_tolerance.handler.action import Action
from common.fault_tolerance.handler.persistent_state_handler import PersistentStateHandler
from common.fault_tolerance.inbox import MsgKind
from common.middleware import (
    LazyQueue,
    MessageMiddlewareQueueRabbitMQ,
    MessageMiddlewareExchangeRabbitMQ,
    MessageMiddlewareRpcClientRabbitMQ,
)
from common.message_protocol.internal import InternalProtocol, TransactionSerializer
from common.message_protocol.internal.common import ControlMessage, MessageType
from common.message_protocol.internal.control_message_serializer import ControlMessageSerializer
from common.rates.rates_manager import RatesManager
from common.routing import queue_name_for_worker

try:
    from filter_q5_usd_state import FilterQ5UsdState
except ImportError:
    from workers.filter_q5_usd.filter_q5_usd_state import FilterQ5UsdState

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
INPUT_EXCHANGE = os.environ["INPUT_EXCHANGE"]
INPUT_ROUTING_PREFIX = os.environ["INPUT_ROUTING_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
RATES_REQUEST_QUEUE = os.environ.get("RATES_REQUEST_QUEUE", "rates_requests")
START_DATE = os.environ.get("Q5_START_DATE", "2022-09-01")
END_DATE = os.environ.get("Q5_END_DATE", "2022-09-05")
FILTER_Q5_USD_AMOUNT = int(os.environ.get("FILTER_Q5_USD_AMOUNT", "1"))
FILTER_Q5_USD_PREFIX = os.environ.get("FILTER_Q5_USD_PREFIX", "filter_q5_usd")
STATE_DIR = os.environ.get("STATE_DIR", "/tmp/filter_q5_usd_state")
SNAPSHOT_INTERVAL = int(os.environ.get("SNAPSHOT_INTERVAL", "1000"))
MODE = "broadcast"


class FilterQ5UsdWorker:
    def __init__(self):
        self._coordinator = EofCoordinator(
            instance_id=ID,
            total_instances=FILTER_Q5_USD_AMOUNT,
            control_queue_prefix=f"{FILTER_Q5_USD_PREFIX}_control",
            response_queue_prefix=f"{FILTER_Q5_USD_PREFIX}_response",
            mode=MODE,
        )

        self._state = FilterQ5UsdState(self._coordinator)

        self._handler = PersistentStateHandler(
            state_dir=STATE_DIR,
            node_id=f"filter_q5_usd_{ID}",
            worker_state=self._state,
            snapshot_every=SNAPSHOT_INTERVAL,
        )

        self.input_queue = MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST,
            INPUT_EXCHANGE,
            [self._input_routing_key()],
            queue_name=INPUT_QUEUE,
            exclusive=False,
        )

        self._main_control_senders = self._new_control_senders()

        self._ctrl_ser = ControlMessageSerializer()
        self._proto = InternalProtocol()
        self._tx_ser = TransactionSerializer()

        self.rates_manager = RatesManager(cache_path="")
        self.rates_loaded = False
        self.rates_lock = threading.Lock()

        self._lock = threading.Lock()
        self._tls = threading.local()

        self.control_consumer: MessageMiddlewareQueueRabbitMQ | None = None
        self.response_consumer: MessageMiddlewareQueueRabbitMQ | None = None
        self.control_thread: threading.Thread | None = None
        self.response_thread: threading.Thread | None = None
        self.closed = False
        self._shutdown = threading.Event()

        self._handler.recover()
        self._republish_pending()

    # ---------- connection helpers ----------

    def _input_routing_key(self) -> str:
        return queue_name_for_worker(INPUT_ROUTING_PREFIX, ID)

    def _tl_sender(self, destination: str) -> LazyQueue:
        if not hasattr(self._tls, "senders"):
            self._tls.senders = {}
        if destination not in self._tls.senders:
            self._tls.senders[destination] = LazyQueue(MOM_HOST, destination)
        return self._tls.senders[destination]

    def _new_control_senders(self) -> dict:
        return {
            self._coordinator.control_queue_for(i): LazyQueue(
                MOM_HOST, self._coordinator.control_queue_for(i)
            )
            for i in range(FILTER_Q5_USD_AMOUNT)
        }

    def _new_response_senders(self) -> dict:
        return {
            self._coordinator.response_queue_for(i): LazyQueue(
                MOM_HOST, self._coordinator.response_queue_for(i)
            )
            for i in range(FILTER_Q5_USD_AMOUNT)
        }

    def _republish_pending(self) -> None:
        for entry in self._handler.outbox_to_republish():
            try:
                self._tl_sender(entry.destination).send(entry.body)
            except Exception:
                logging.exception(
                    "filter_q5_usd_republish_error | id=%s | destination=%s",
                    ID, entry.destination,
                )
                raise

    def _publish_commit_ack(self, instruction, ack) -> bool:
        if instruction.action is Action.ACK:
            ack()
            return False
        for entry in instruction.outputs:
            self._tl_sender(entry.destination).send(entry.body)
        with self._lock:
            self._handler.commit_done(*instruction.ctx)
        ack()
        return True

    # ---------- packet helpers ----------

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

    def _build_eof_outputs(self, client_id: int, total_forwarded: int) -> list:
        eof_payload = self._eof_payload(total_forwarded)
        current_seq = self._state.agg_seq(client_id)
        return [
            (f"{AGGREGATION_PREFIX}_{i}",
             self._addressed_packet(MessageType.EOF, client_id, current_seq + i, eof_payload))
            for i in range(AGGREGATION_AMOUNT)
        ]

    # ---------- data path ----------

    def _process_data_message(self, message, ack, nack):
        try:
            msg_type, client_id, payload = self._proto.unpack_packet(message)

            if msg_type == MessageType.DATA:
                # TEMPORARY: upstream filter (C_Q5 config) still sends plain packets
                # [msg_type|client_id|payload] via create_packet, not addressed packets.
                # Once filter migrates to PersistentStateHandler it will use
                # create_addressed_packet and we can read sender_id/seq directly.
                # Until then: synthesize a stable dedup key from sha256(message).
                # Protects against RabbitMQ redelivery of the same bytes on crash.
                # Does NOT protect against two distinct batches with identical content
                # (astronomically unlikely in practice for financial transaction data).
                h = hashlib.sha256(message).digest()
                sender_id = int.from_bytes(h[:4], "big")
                seq = int.from_bytes(h[4:8], "big")
                msg_id = f"d:{sender_id}:{seq}"

                with self._lock:
                    if self._state.is_closed(client_id):
                        ack()
                        return

                self._load_rates()  # RPC outside the handler lock

                batch_stats = {"processed": 0, "forwarded": 0}

                def bfn(pl):
                    transactions = self._tx_ser.deserialize_batch(pl)
                    batch_stats["processed"] = len(transactions)
                    if not transactions:
                        return FilterQ5UsdState.data_change(client_id, 0, 0), []

                    current_seq = self._state.agg_seq(client_id)
                    outputs = []
                    forwarded = 0
                    for tx in transactions:
                        if not self._in_date_range(tx.date):
                            continue
                        try:
                            amount_usd = self._convert_to_usd(
                                tx.amount,
                                tx.currency,
                                tx.date[:10].replace("/", "-"),
                            )
                        except ValueError as e:
                            logging.warning("filter_q5_usd_conversion_error | error=%s", e)
                            continue
                        if amount_usd >= 1.0:
                            continue
                        shard = client_id % AGGREGATION_AMOUNT
                        dest = f"{AGGREGATION_PREFIX}_{shard}"
                        pkt = self._addressed_packet(
                            MessageType.DATA, client_id,
                            current_seq + forwarded,
                            self._tx_ser.serialize(tx),
                        )
                        outputs.append((dest, pkt))
                        forwarded += 1

                    batch_stats["forwarded"] = forwarded
                    return FilterQ5UsdState.data_change(
                        client_id, len(transactions), forwarded, seq_advance=forwarded
                    ), outputs

                with self._lock:
                    instruction = self._handler.handle(
                        msg_id, client_id, sender_id, seq, payload, bfn
                    )
                committed = self._publish_commit_ack(instruction, ack)

                processed = self._state.processed_count(client_id)
                if committed:
                    logging.info(
                        "filter_q5_usd_data_batch | id=%s | client_id=%s | "
                        "batch_size=%s | forwarded_in_batch=%s | outputs=%s | "
                        "processed_total=%s | forwarded_total=%s",
                        ID, client_id, batch_stats["processed"],
                        batch_stats["forwarded"], len(instruction.outputs),
                        processed, self._state.forwarded_count(client_id),
                    )

            elif msg_type == MessageType.EOF:
                self._handle_upstream_eof(client_id, payload, ack, nack)

            else:
                raise ValueError(f"Unexpected filter_q5_usd message type: {msg_type}")

        except Exception:
            logging.exception("filter_q5_usd_data_error | id=%s", ID)
            nack(requeue=True)

    def _handle_upstream_eof(self, client_id: int, payload: bytes, ack, nack):
        ctrl = self._ctrl_ser.deserialize(payload)
        sender_id = ctrl.sender_id
        seq = client_id  # at most one upstream EOF per (sender, client) pair
        msg_id = f"ue:{client_id}:{sender_id}"

        try:
            with self._lock:
                if self._state.is_closed(client_id):
                    ack()
                    return

                count = self._state.processed_count(client_id)
                fwd = self._state.forwarded_count(client_id)

                def bfn(_pl):
                    action = self._coordinator.on_upstream_eof(
                        client_id, ctrl.expected_total, count, fwd
                    )
                    eof_change = FilterQ5UsdState.coordinator_upstream_eof_change(
                        client_id, ctrl.expected_total, count, fwd
                    )
                    if isinstance(action, BroadcastAction):
                        outputs = [(qname, action.message) for qname in action.queue_names]
                        return eof_change, outputs
                    if isinstance(action, FlushAction):
                        # N=1: flush directly without coordination.
                        outputs = self._build_eof_outputs(client_id, fwd)
                        compound = FilterQ5UsdState.compound_change(
                            eof_change,
                            FilterQ5UsdState.data_change(client_id, 0, 0, AGGREGATION_AMOUNT),
                            FilterQ5UsdState.close_change(client_id),
                        )
                        return compound, outputs
                    # None: duplicate upstream EOF
                    return eof_change, []

                instruction = self._handler.handle(
                    msg_id, client_id, sender_id, seq, payload, bfn,
                    kind=MsgKind.CTRL_UPSTREAM_EOF,
                )

            committed = self._publish_commit_ack(instruction, ack)
            if not committed:
                return
            logging.info(
                "filter_q5_usd_upstream_eof | id=%s | client_id=%s | expected_total=%s",
                ID, client_id, ctrl.expected_total,
            )

        except Exception:
            logging.exception(
                "filter_q5_usd_upstream_eof_error | id=%s | client_id=%s", ID, client_id
            )
            nack(requeue=True)

    # ---------- control path ----------

    def _handle_control(self, message, ack, nack, response_senders: dict):
        try:
            msg_type, client_id, ctrl = self._coordinator.parse_message(message)
        except Exception:
            logging.exception("filter_q5_usd_control_parse_error | id=%s", ID)
            nack(requeue=False)
            return

        sender_id = ctrl.sender_id
        seq = client_id
        msg_id = f"ctrl:{msg_type.value}:{client_id}:{sender_id}"
        kind_by_type = {
            MessageType.EOF_RECEIVED: MsgKind.CTRL_EOF_RECEIVED,
            MessageType.FLUSH_ORDER: MsgKind.CTRL_FLUSH_ORDER,
        }
        kind = kind_by_type.get(msg_type)

        try:
            with self._lock:
                if self._state.is_closed(client_id):
                    ack()
                    return

                count = self._state.processed_count(client_id)
                fwd = self._state.forwarded_count(client_id)

                if msg_type == MessageType.EOF_RECEIVED:
                    def bfn(_pl):
                        action = self._coordinator.process_control_message(
                            msg_type, client_id, ctrl, count, fwd
                        )
                        change = FilterQ5UsdState.coordinator_msg_change(
                            msg_type, client_id, sender_id,
                            ctrl.expected_total, ctrl.processed_count, count, fwd,
                        )
                        if isinstance(action, SendAnswerAction):
                            return change, [(action.queue_name, action.message)]
                        return change, []

                    instruction = self._handler.handle(
                        msg_id, client_id, sender_id, seq, message, bfn,
                        kind=kind,
                    )

                elif msg_type == MessageType.FLUSH_ORDER:
                    leader_id = ctrl.sender_id

                    def bfn(_pl):
                        # process_control_message for FLUSH_ORDER is read-only:
                        # it only checks _leader_expected to decide leader vs non-leader.
                        # The actual coordinator cleanup happens via apply_change below.
                        action = self._coordinator.process_control_message(msg_type, client_id, ctrl)
                        if action is None:
                            # Leader receives its own broadcast but must not flush.
                            return FilterQ5UsdState.data_change(client_id, 0, 0, 0), []
                        outputs = self._build_eof_outputs(client_id, fwd)
                        flush_ack_msg = self._coordinator.build_flush_ack(client_id, fwd)
                        flush_ack_dest = self._coordinator.response_queue_for(leader_id)
                        compound = FilterQ5UsdState.compound_change(
                            FilterQ5UsdState.coordinator_cleanup_change(client_id),
                            FilterQ5UsdState.data_change(client_id, 0, 0, AGGREGATION_AMOUNT),
                            FilterQ5UsdState.close_change(client_id),
                        )
                        return compound, outputs + [(flush_ack_dest, flush_ack_msg)]

                    instruction = self._handler.handle(
                        msg_id, client_id, sender_id, seq, message, bfn,
                        kind=kind,
                    )

                else:
                    logging.warning(
                        "filter_q5_usd_unexpected_control_type | id=%s | msg_type=%s",
                        ID, msg_type,
                    )
                    ack()
                    return

            self._publish_commit_ack(instruction, ack)

        except Exception:
            logging.exception(
                "filter_q5_usd_control_error | id=%s | client_id=%s", ID, client_id
            )
            nack(requeue=True)

    def _start_control_consumer(self):
        consumer = MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, self._coordinator.my_control_queue()
        )
        self.control_consumer = consumer
        response_senders = self._new_response_senders()
        try:
            if not self._shutdown.is_set():
                consumer.start_consuming(
                    lambda msg, ack, nack: self._handle_control(msg, ack, nack, response_senders)
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

    # ---------- response path (leader) ----------

    def _handle_response(self, message, ack, nack):
        try:
            msg_type, client_id, ctrl = self._coordinator.parse_message(message)
        except Exception:
            logging.exception("filter_q5_usd_response_parse_error | id=%s", ID)
            nack(requeue=False)
            return

        try:
            if msg_type == MessageType.PROCESSED_ANSWER:
                # Direct coordinator call — same known limitation as the aggregator:
                # if leader crashes after acking some PROCESSED_ANSWERs but before
                # snapshotting, non-leaders redeliver and rebuild the count.
                with self._lock:
                    action = self._coordinator.process_control_message(msg_type, client_id, ctrl)
                if action is None:
                    ack()
                    return
                if isinstance(action, BroadcastAction):
                    if action.sleep_before > 0:
                        time.sleep(action.sleep_before)
                    for qname in action.queue_names:
                        self._tl_sender(qname).send(action.message)
                    ack()
                else:
                    logging.warning(
                        "filter_q5_usd_unexpected_processed_answer_action | id=%s | action=%s",
                        ID, action,
                    )
                    ack()

            elif msg_type == MessageType.FLUSH_ACK:
                sender_id = ctrl.sender_id
                seq = client_id
                msg_id = f"fa:{client_id}:{sender_id}"

                with self._lock:
                    if self._state.is_closed(client_id):
                        ack()
                        return

                    already = self._coordinator.has_flush_ack(client_id, ctrl.sender_id)
                    new_ack_count = self._coordinator.flush_ack_count(client_id) + (
                        0 if already else 1
                    )
                    own_fwd = self._state.forwarded_count(client_id)
                    accumulated = self._coordinator.accumulated_forwarded(client_id)
                    new_total_fwd = accumulated + ctrl.processed_count + own_fwd

                    def bfn(_pl):
                        ack_change = FilterQ5UsdState.coordinator_msg_change(
                            MessageType.FLUSH_ACK, client_id, sender_id,
                            ctrl.expected_total, ctrl.processed_count,
                        )
                        if new_ack_count >= FILTER_Q5_USD_AMOUNT - 1:
                            outputs = self._build_eof_outputs(client_id, new_total_fwd)
                            compound = FilterQ5UsdState.compound_change(
                                ack_change,
                                FilterQ5UsdState.data_change(client_id, 0, 0, AGGREGATION_AMOUNT),
                                FilterQ5UsdState.close_change(client_id),
                            )
                            logging.info(
                                "filter_q5_usd_eof_ready | id=%s | client_id=%s | total_fwd=%s",
                                ID, client_id, new_total_fwd,
                            )
                            return compound, outputs
                        return ack_change, []

                    instruction = self._handler.handle(
                        msg_id, client_id, sender_id, seq, message, bfn,
                        kind=MsgKind.CTRL_FLUSH_ACK,
                    )

                self._publish_commit_ack(instruction, ack)

            else:
                logging.warning(
                    "filter_q5_usd_unexpected_response_type | id=%s | msg_type=%s", ID, msg_type
                )
                ack()

        except Exception:
            logging.exception(
                "filter_q5_usd_response_error | id=%s | client_id=%s", ID, client_id
            )
            nack(requeue=True)

    def _start_response_consumer(self):
        consumer = MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, self._coordinator.my_response_queue()
        )
        self.response_consumer = consumer
        try:
            if not self._shutdown.is_set():
                consumer.start_consuming(
                    lambda msg, ack, nack: self._handle_response(msg, ack, nack)
                )
        finally:
            try:
                consumer.close()
            except Exception:
                pass

    # ---------- lifecycle ----------

    def start(self):
        logging.info(
            "filter_q5_usd_start | id=%s | input=%s | exchange=%s | routing_key=%s | "
            "aggregation_prefix=%s | aggregation_amount=%s | cluster_size=%s | "
            "date_range=[%s, %s]",
            ID, INPUT_QUEUE, INPUT_EXCHANGE, self._input_routing_key(),
            AGGREGATION_PREFIX, AGGREGATION_AMOUNT,
            FILTER_Q5_USD_AMOUNT, START_DATE, END_DATE,
        )

        self.control_thread = threading.Thread(target=self._start_control_consumer)
        self.response_thread = threading.Thread(target=self._start_response_consumer)
        self.control_thread.start()
        self.response_thread.start()

        try:
            if not self._shutdown.is_set():
                self.input_queue.start_consuming(self._process_data_message)
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
            + list(self._main_control_senders.values())
        )
        for resource in resources:
            try:
                resource.close()
            except Exception:
                pass
