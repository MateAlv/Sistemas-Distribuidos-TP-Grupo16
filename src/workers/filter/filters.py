import json
import logging
import os
import threading
import time

from common import message_protocol
from common.bank_ids import notebook_bank_id
from common.domain.transaction import Transaction
from common import middleware
from common.constants import *
from common.eof_coordinator import (
    BroadcastAction,
    EofCoordinator,
    FlushAction,
    SendAnswerAction,
)
from common.logging_utils import should_log_progress
from common.message_protocol.internal import partition_for_parts
from common.message_protocol.internal.common import MessageType
from common.middleware.middleware_rabbitmq import ensure_exchange_queue_bindings
from common.routing import queue_name_for_worker

try:
    from output_batcher import OutputBatcher
except ImportError:
    from workers.filter.output_batcher import OutputBatcher

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
# Personal-queue input: cuando está seteado, el filter consume su propia cola
# (INPUT_QUEUE) ligada a un exchange direct por routing key. Reemplaza a la cola
# compartida / fanout en las etapas convertidas a colas personales.
INPUT_EXCHANGE = os.getenv("INPUT_EXCHANGE")
INPUT_ROUTING_PREFIX = os.getenv("INPUT_ROUTING_PREFIX")
# Colas de Salida Posibles
GATEWAY_QUEUE = os.environ["GATEWAY_QUEUE"]
FILTER_DATE_QUEUE = os.environ["FILTER_DATE_QUEUE"]
FILTER_Q1_QUEUE = os.environ["FILTER_Q1_QUEUE"]
FILTER_Q3_QUEUE = os.environ["FILTER_Q3_QUEUE"]
SCATTER_GATHER_MAPPER_QUEUE = os.environ["SCATTER_GATHER_MAPPER_QUEUE"]
FILTER_Q5_USD_QUEUE = os.environ["FILTER_Q5_USD_QUEUE"]
Q4_FILTER_INPUT_EXCHANGE = os.getenv("Q4_FILTER_INPUT_EXCHANGE")
Q4_FILTER_INPUT_ROUTING_PREFIX = os.getenv(
    "Q4_FILTER_INPUT_ROUTING_PREFIX",
    "q4_filter",
)
Q4_FILTER_AMOUNT = int(os.getenv("Q4_FILTER_AMOUNT", "1"))
Q4_FILTER_OUTPUT = "q4_filter"
Q3_CANDIDATES_QUEUE = os.getenv("Q3_CANDIDATES_QUEUE", FILTER_Q3_QUEUE)
# Sharding opcional de candidatos Q3 por client_id. Si Q3_CANDIDATES_EXCHANGE
# y Q3_BARRIER_AMOUNT > 1 están seteados, el filter publica a un exchange con
# routing key "{Q3_CANDIDATES_ROUTING_PREFIX}_{client_id % Q3_BARRIER_AMOUNT}"
# para distribuir candidatos entre N q3_barrier shards.
Q3_CANDIDATES_EXCHANGE = os.getenv("Q3_CANDIDATES_EXCHANGE")
Q3_CANDIDATES_ROUTING_PREFIX = os.getenv(
    "Q3_CANDIDATES_ROUTING_PREFIX", "q3_candidates"
)
Q3_BARRIER_AMOUNT = int(os.getenv("Q3_BARRIER_AMOUNT", "1"))
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_Q3_QUEUE = os.getenv("SUM_Q3_QUEUE", SUM_PREFIX)
SUM_Q3_EXCHANGE = os.getenv("SUM_Q3_EXCHANGE", "")
SUM_Q3_ROUTING_PREFIX = os.getenv("SUM_Q3_ROUTING_PREFIX", SUM_PREFIX)
SUM_Q3_AMOUNT = int(os.getenv("SUM_Q3_AMOUNT", "0"))

FILTER_AMOUNT = int(os.environ["FILTER_AMOUNT"])
FILTER_PREFIX = os.environ["FILTER_PREFIX"] + "_" + CONFIGURATION
FILTER_CONTROL_QUEUE_PREFIX = FILTER_PREFIX + "_control"
FILTER_RESPONSE_QUEUE_PREFIX = FILTER_PREFIX + "_response"
USD_ENABLE_Q1 = os.getenv("USD_ENABLE_Q1", "1") != "0"
USD_ENABLE_Q2 = os.getenv("USD_ENABLE_Q2", "1") != "0"
USD_ENABLE_DATE = os.getenv("USD_ENABLE_DATE", "1") != "0"
DATE_ENABLE_Q3 = os.getenv("DATE_ENABLE_Q3", "1") != "0"
DATE_ENABLE_Q4 = os.getenv("DATE_ENABLE_Q4", "1") != "0"
SUM_Q2_OUTPUT = "sum_q2"
if CONFIGURATION == C_USD and USD_ENABLE_Q1:
    FILTER_Q1_EXCHANGE = os.environ["FILTER_Q1_EXCHANGE"]
    FILTER_Q1_ROUTING_PREFIX = os.environ["FILTER_Q1_ROUTING_PREFIX"]
    FILTER_Q1_AMOUNT = int(os.environ["FILTER_Q1_AMOUNT"])
else:
    FILTER_Q1_EXCHANGE = ""
    FILTER_Q1_ROUTING_PREFIX = ""
    FILTER_Q1_AMOUNT = 0

if CONFIGURATION == C_Q5:
    FILTER_Q5_USD_EXCHANGE = os.environ["FILTER_Q5_USD_EXCHANGE"]
    FILTER_Q5_USD_ROUTING_PREFIX = os.environ["FILTER_Q5_USD_ROUTING_PREFIX"]
    FILTER_Q5_USD_AMOUNT = int(os.environ["FILTER_Q5_USD_AMOUNT"])
else:
    FILTER_Q5_USD_EXCHANGE = ""
    FILTER_Q5_USD_ROUTING_PREFIX = ""
    FILTER_Q5_USD_AMOUNT = 0

if CONFIGURATION == C_USD and USD_ENABLE_Q2:
    SUM_Q2_EXCHANGE = os.environ["SUM_Q2_EXCHANGE"]
    SUM_Q2_ROUTING_PREFIX = os.environ["SUM_Q2_ROUTING_PREFIX"]
    SUM_Q2_AMOUNT = int(os.environ["SUM_Q2_AMOUNT"])
else:
    SUM_Q2_EXCHANGE = ""
    SUM_Q2_ROUTING_PREFIX = ""
    SUM_Q2_AMOUNT = 0

if CONFIGURATION == C_USD and USD_ENABLE_DATE:
    FILTER_DATE_EXCHANGE = os.environ["FILTER_DATE_EXCHANGE"]
    FILTER_DATE_ROUTING_PREFIX = os.environ["FILTER_DATE_ROUTING_PREFIX"]
    FILTER_DATE_AMOUNT = int(os.environ["FILTER_DATE_AMOUNT"])
else:
    FILTER_DATE_EXCHANGE = ""
    FILTER_DATE_ROUTING_PREFIX = ""
    FILTER_DATE_AMOUNT = 0

FILTER_OUTPUT_BATCH_BYTES = int(os.getenv("FILTER_OUTPUT_BATCH_BYTES", str(1024 * 1024)))
FILTER_OUTPUT_BATCH_MAX_TX = int(os.getenv("FILTER_OUTPUT_BATCH_MAX_TX", "5000"))


class FilterWorker:
    def __init__(self):
        # Iniciacion de la cola de entrada
        if INPUT_EXCHANGE:
            self.input_queue = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST,
                INPUT_EXCHANGE,
                routing_keys=[self._input_routing_key()],
                queue_name=INPUT_QUEUE,
                exclusive=False,
            )
        else:
            self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
                MOM_HOST, INPUT_QUEUE
            )

        self.output_queues = self._new_output_queues()
        self.output_exchanges = []

        # Coordinador EOF
        self.coordinator = EofCoordinator(
            instance_id=ID,
            total_instances=FILTER_AMOUNT,
            control_queue_prefix=FILTER_CONTROL_QUEUE_PREFIX,
            response_queue_prefix=FILTER_RESPONSE_QUEUE_PREFIX,
            mode="broadcast",
        )

        # Senders del hilo principal (data thread) para broadcast de EOF_RECEIVED
        self._main_control_senders = self._new_control_senders()

        # Serializadores para transacciones y mensajes de control
        self.transaction_serializer = message_protocol.internal.TransactionSerializer()
        self.control_serializer = message_protocol.internal.ControlMessageSerializer()
        self.internal_packet_serializer = message_protocol.internal.InternalProtocol()

        self._batcher: OutputBatcher | None = None
        if CONFIGURATION in (C_USD, C_Q5, C_DATE, C_Q1):
            self._batcher = OutputBatcher(
                self.transaction_serializer,
                FILTER_OUTPUT_BATCH_BYTES,
                FILTER_OUTPUT_BATCH_MAX_TX,
            )

        # Locks, flags y threads
        self.lock = threading.Lock()
        self._stopped_lock = threading.Lock()
        self.active = True
        self._closed = False
        self.control_thread = None
        self.response_thread = None
        self._control_consumer = None
        self._response_consumer = None

        # Procesados por cliente
        self.processed_by_client = {}
        self.forwarded_by_client = {}
        self.forwarded_by_output_by_client = {}
        self.closed_by_client = set()
        self.all_forwarded_by_output_by_client = {}
        self.flushed_acks_by_client = {}
        self.first_data_logged_by_client = set()
        self.deserialized_by_client = {}

    # ─── connection factories ────────────────────────────────────────────────

    def _new_control_senders(self) -> dict:
        return {
            self.coordinator.control_queue_for(i): middleware.LazyQueue(
                MOM_HOST, self.coordinator.control_queue_for(i)
            )
            for i in range(FILTER_AMOUNT)
        }

    def _new_response_senders(self) -> dict:
        return {
            self.coordinator.response_queue_for(i): middleware.LazyQueue(
                MOM_HOST, self.coordinator.response_queue_for(i)
            )
            for i in range(FILTER_AMOUNT)
        }

    def _new_output_queues(self):
        output_queues = {}
        if CONFIGURATION == C_Q1:
            output_queues[GATEWAY_QUEUE] = middleware.MessageMiddlewareQueueRabbitMQ(
                MOM_HOST, GATEWAY_QUEUE
            )
        if CONFIGURATION == C_Q5:
            output_queues[FILTER_Q5_USD_QUEUE] = middleware.ShardedPublisher(
                MOM_HOST,
                FILTER_Q5_USD_EXCHANGE,
                FILTER_Q5_USD_ROUTING_PREFIX,
                FILTER_Q5_USD_AMOUNT,
                key_fn=middleware.body_digest_key,
            )
        if CONFIGURATION == C_USD:
            if USD_ENABLE_Q1:
                output_queues[FILTER_Q1_QUEUE] = middleware.ShardedPublisher(
                    MOM_HOST,
                    FILTER_Q1_EXCHANGE,
                    FILTER_Q1_ROUTING_PREFIX,
                    FILTER_Q1_AMOUNT,
                    key_fn=middleware.body_digest_key,
                )
            if USD_ENABLE_Q2:
                output_queues[SUM_Q2_OUTPUT] = middleware.ShardedPublisher(
                    MOM_HOST,
                    SUM_Q2_EXCHANGE,
                    SUM_Q2_ROUTING_PREFIX,
                    SUM_Q2_AMOUNT,
                    key_fn=middleware.body_digest_key,
                )
            if USD_ENABLE_DATE:
                output_queues[FILTER_DATE_QUEUE] = middleware.ShardedPublisher(
                    MOM_HOST,
                    FILTER_DATE_EXCHANGE,
                    FILTER_DATE_ROUTING_PREFIX,
                    FILTER_DATE_AMOUNT,
                    key_fn=middleware.body_digest_key,
                )
        if CONFIGURATION == C_DATE:
            if DATE_ENABLE_Q4:
                if Q4_FILTER_INPUT_EXCHANGE:
                    for partition in range(Q4_FILTER_AMOUNT):
                        routing_key = self._q4_filter_routing_key(partition)
                        output_queues[routing_key] = (
                            middleware.MessageMiddlewareExchangeRabbitMQ(
                                MOM_HOST,
                                Q4_FILTER_INPUT_EXCHANGE,
                                [routing_key],
                            )
                        )
                else:
                    output_queues[SCATTER_GATHER_MAPPER_QUEUE] = (
                        middleware.MessageMiddlewareQueueRabbitMQ(
                            MOM_HOST, SCATTER_GATHER_MAPPER_QUEUE
                        )
                    )
            if DATE_ENABLE_Q3:
                output_queues[SUM_Q3_QUEUE] = middleware.ShardedPublisher(
                    MOM_HOST,
                    SUM_Q3_EXCHANGE,
                    SUM_Q3_ROUTING_PREFIX,
                    SUM_Q3_AMOUNT,
                    key_fn=middleware.body_digest_key,
                )
                if Q3_CANDIDATES_EXCHANGE and Q3_BARRIER_AMOUNT > 1:
                    output_queues[Q3_CANDIDATES_QUEUE] = (
                        middleware.ShardedByClientPublisher(
                            MOM_HOST,
                            Q3_CANDIDATES_EXCHANGE,
                            Q3_CANDIDATES_ROUTING_PREFIX,
                            Q3_BARRIER_AMOUNT,
                        )
                    )
                else:
                    output_queues[Q3_CANDIDATES_QUEUE] = (
                        middleware.MessageMiddlewareQueueRabbitMQ(
                            MOM_HOST, Q3_CANDIDATES_QUEUE
                        )
                    )
        return output_queues

    # ─── helpers ─────────────────────────────────────────────────────────────

    @staticmethod
    def _input_routing_key() -> str:
        return queue_name_for_worker(INPUT_ROUTING_PREFIX, ID)

    @staticmethod
    def _ensure_sharded_output_bindings(
        exchange: str, routing_prefix: str, shard_count: int
    ) -> None:
        ensure_exchange_queue_bindings(
            MOM_HOST,
            exchange,
            {
                queue_name_for_worker(routing_prefix, index): queue_name_for_worker(
                    routing_prefix, index
                )
                for index in range(shard_count)
            },
        )

    def _ensure_output_bindings(self) -> None:
        if CONFIGURATION == C_Q5:
            self._ensure_sharded_output_bindings(
                FILTER_Q5_USD_EXCHANGE,
                FILTER_Q5_USD_ROUTING_PREFIX,
                FILTER_Q5_USD_AMOUNT,
            )
            return
        if CONFIGURATION != C_USD:
            return
        if USD_ENABLE_Q1:
            self._ensure_sharded_output_bindings(
                FILTER_Q1_EXCHANGE, FILTER_Q1_ROUTING_PREFIX, FILTER_Q1_AMOUNT
            )
        if USD_ENABLE_DATE:
            self._ensure_sharded_output_bindings(
                FILTER_DATE_EXCHANGE,
                FILTER_DATE_ROUTING_PREFIX,
                FILTER_DATE_AMOUNT,
            )

    def _cleanup_client(self, client_id):
        if self._batcher is not None:
            self._batcher.discard_client(client_id)
        with self.lock:
            self._cleanup_client_locked(client_id)

    def _cleanup_client_locked(self, client_id):
        """Limpia estado por cliente. Debe llamarse bajo self.lock."""
        self.processed_by_client.pop(client_id, None)
        self.forwarded_by_client.pop(client_id, None)
        self.forwarded_by_output_by_client.pop(client_id, None)
        self.all_forwarded_by_output_by_client.pop(client_id, None)
        self.flushed_acks_by_client.pop(client_id, None)
        self.closed_by_client.add(client_id)

    def _record_forwarded_output(self, client_id: int, output_name: str):
        if client_id not in self.forwarded_by_output_by_client:
            self.forwarded_by_output_by_client[client_id] = {}
        if output_name not in self.forwarded_by_output_by_client[client_id]:
            self.forwarded_by_output_by_client[client_id][output_name] = 0
        self.forwarded_by_output_by_client[client_id][output_name] += 1

    def _q4_filter_routing_key(self, partition: int) -> str:
        return queue_name_for_worker(Q4_FILTER_INPUT_ROUTING_PREFIX, partition)

    def _q4_filter_output_names(self) -> list[str]:
        if not Q4_FILTER_INPUT_EXCHANGE:
            return [SCATTER_GATHER_MAPPER_QUEUE]
        return [
            self._q4_filter_routing_key(partition)
            for partition in range(Q4_FILTER_AMOUNT)
        ]

    def _q4_filter_output_for_transaction(self, transaction: Transaction) -> str:
        if not Q4_FILTER_INPUT_EXCHANGE:
            return SCATTER_GATHER_MAPPER_QUEUE
        partition = partition_for_parts(
            (
                notebook_bank_id(transaction.from_bank),
                (transaction.from_account or "").strip(),
            ),
            Q4_FILTER_AMOUNT,
        )
        return self._q4_filter_routing_key(partition)

    def _data_packet(self, client_id: int, payload: bytes) -> bytes:
        return self.internal_packet_serializer.create_packet(
            msg_type=message_protocol.internal.MessageType.DATA,
            client_id_bytes=client_id.to_bytes(16, byteorder="big"),
            payload=payload,
        )

    def _publish_to_queue(
        self, queue_name: str, client_id: int, transaction: Transaction,
        output_queues=None,
    ) -> None:
        output_queues = output_queues or self.output_queues
        if self._batcher is not None:
            batch_payload = self._batcher.append(queue_name, client_id, transaction)
            if batch_payload is not None:
                output_queues[queue_name].send(
                    self._data_packet(client_id, batch_payload)
                )
            return
        payload = self.transaction_serializer.serialize(transaction)
        output_queues[queue_name].send(self._data_packet(client_id, payload))

    def _flush_batcher_for_client(self, client_id: int, output_queues=None) -> None:
        if self._batcher is None:
            return
        output_queues = output_queues or self.output_queues
        for queue_name, payload in self._batcher.drain_client(client_id).items():
            output_queues[queue_name].send(self._data_packet(client_id, payload))
            logging.info(
                "filter_batcher_flush | filter=%s | id=%s | client_id=%s | "
                "queue=%s | bytes=%s",
                CONFIGURATION, ID, client_id, queue_name, len(payload),
            )

    def _forward_transaction(
        self, transaction: Transaction, client_id: int, output_queues=None
    ):
        logging.debug(
            f"Transaction {transaction} passed filter in filter_{CONFIGURATION} "
            f"with id {ID}, forwarding to output"
        )
        sent = False
        if CONFIGURATION == C_Q1:
            self._publish_to_queue(GATEWAY_QUEUE, client_id, transaction, output_queues)
            with self.lock:
                self._record_forwarded_output(client_id, GATEWAY_QUEUE)
            sent = True
        if CONFIGURATION == C_Q5:
            self._publish_to_queue(FILTER_Q5_USD_QUEUE, client_id, transaction, output_queues)
            with self.lock:
                self._record_forwarded_output(client_id, FILTER_Q5_USD_QUEUE)
            sent = True
        if CONFIGURATION == C_USD:
            if USD_ENABLE_Q1:
                self._publish_to_queue(FILTER_Q1_QUEUE, client_id, transaction, output_queues)
                with self.lock:
                    self._record_forwarded_output(client_id, FILTER_Q1_QUEUE)
                sent = True
            if USD_ENABLE_Q2:
                self._publish_to_queue(SUM_Q2_OUTPUT, client_id, transaction, output_queues)
                with self.lock:
                    self._record_forwarded_output(client_id, SUM_Q2_OUTPUT)
                sent = True
            if USD_ENABLE_DATE:
                self._publish_to_queue(FILTER_DATE_QUEUE, client_id, transaction, output_queues)
                with self.lock:
                    self._record_forwarded_output(client_id, FILTER_DATE_QUEUE)
                sent = True
        if CONFIGURATION == C_DATE:
            if DATE_ENABLE_Q3 and self._filter_transaction_by_raw_timestamp(
                transaction,
                start_timestamp="2022/09/01",
                end_timestamp="2022/09/06",
            ):
                self._publish_to_queue(SUM_Q3_QUEUE, client_id, transaction, output_queues)
                with self.lock:
                    self._record_forwarded_output(client_id, SUM_Q3_QUEUE)
                sent = True
            if DATE_ENABLE_Q3 and self._filter_transaction_by_raw_timestamp(
                transaction,
                start_timestamp="2022/09/06",
                end_timestamp="2022/09/15",
            ):
                self._publish_to_queue(Q3_CANDIDATES_QUEUE, client_id, transaction, output_queues)
                with self.lock:
                    self._record_forwarded_output(client_id, Q3_CANDIDATES_QUEUE)
                sent = True
            if DATE_ENABLE_Q4 and self._filter_transaction_by_raw_timestamp(
                transaction,
                start_timestamp="2022/09/01",
                end_timestamp="2022/09/06",
            ):
                q4_output = self._q4_filter_output_for_transaction(transaction)
                self._publish_to_queue(q4_output, client_id, transaction, output_queues)
                with self.lock:
                    self._record_forwarded_output(
                        client_id,
                        (
                            Q4_FILTER_OUTPUT
                            if Q4_FILTER_INPUT_EXCHANGE
                            else SCATTER_GATHER_MAPPER_QUEUE
                        ),
                    )
                sent = True
        return sent

    def _forward_eof(self, client_id: int, forwarded_by_output: dict, output_queues=None):
        def count(queue: str) -> int:
            return int(forwarded_by_output.get(queue, 0))

        def eof_packet(total: int):
            message = self.control_serializer.serialize(
                message_protocol.internal.ControlMessage(
                    sender_id=ID,
                    expected_total=total,
                    processed_count=0
                )
            )
            return self.internal_packet_serializer.create_packet(
                msg_type=message_protocol.internal.MessageType.EOF,
                client_id_bytes=client_id.to_bytes(16, byteorder='big'),
                payload=message
            )

        output_queues = output_queues or self.output_queues
        if CONFIGURATION == C_Q1:
            expected_total = count(GATEWAY_QUEUE)
            output_queues[GATEWAY_QUEUE].send(eof_packet(expected_total))
            self._log_forwarded_eof(client_id, GATEWAY_QUEUE, expected_total)
        if CONFIGURATION == C_Q5:
            expected_total = count(FILTER_Q5_USD_QUEUE)
            output_queues[FILTER_Q5_USD_QUEUE].send(eof_packet(expected_total))
            self._log_forwarded_eof(client_id, FILTER_Q5_USD_QUEUE, expected_total)
        if CONFIGURATION == C_USD:
            if USD_ENABLE_Q1:
                expected_total = count(FILTER_Q1_QUEUE)
                output_queues[FILTER_Q1_QUEUE].send(eof_packet(expected_total))
                self._log_forwarded_eof(client_id, FILTER_Q1_QUEUE, expected_total)
            if USD_ENABLE_Q2:
                expected_total = count(SUM_Q2_OUTPUT)
                output_queues[SUM_Q2_OUTPUT].send(eof_packet(expected_total))
                self._log_forwarded_eof(client_id, SUM_Q2_OUTPUT, expected_total)
            if USD_ENABLE_DATE:
                expected_total = count(FILTER_DATE_QUEUE)
                output_queues[FILTER_DATE_QUEUE].send(eof_packet(expected_total))
                self._log_forwarded_eof(client_id, FILTER_DATE_QUEUE, expected_total)
        if CONFIGURATION == C_DATE:
            if DATE_ENABLE_Q3:
                expected_total = count(SUM_Q3_QUEUE)
                output_queues[SUM_Q3_QUEUE].send(eof_packet(expected_total))
                self._log_forwarded_eof(client_id, SUM_Q3_QUEUE, expected_total)
                expected_total = count(Q3_CANDIDATES_QUEUE)
                output_queues[Q3_CANDIDATES_QUEUE].send(eof_packet(expected_total))
                self._log_forwarded_eof(client_id, Q3_CANDIDATES_QUEUE, expected_total)
            if DATE_ENABLE_Q4:
                if Q4_FILTER_INPUT_EXCHANGE:
                    expected_total = count(Q4_FILTER_OUTPUT)
                    for output_name in self._q4_filter_output_names():
                        output_queues[output_name].send(eof_packet(expected_total))
                        self._log_forwarded_eof(client_id, output_name, expected_total)
                else:
                    expected_total = count(SCATTER_GATHER_MAPPER_QUEUE)
                    output_queues[SCATTER_GATHER_MAPPER_QUEUE].send(
                        eof_packet(expected_total)
                    )
                    self._log_forwarded_eof(
                        client_id, SCATTER_GATHER_MAPPER_QUEUE, expected_total
                    )

    def _log_forwarded_eof(self, client_id: int, output_name: str, expected_total: int):
        logging.info(
            "filter_forward_eof | filter=%s | id=%s | client_id=%s | "
            "output=%s | expected_total=%s",
            CONFIGURATION, ID, client_id, output_name, expected_total,
        )

    def _filter_transaction(self, transaction: Transaction, start_date=None, end_date=None):
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

    @staticmethod
    def _filter_transaction_by_raw_timestamp(
        transaction: Transaction, start_timestamp: str, end_timestamp: str
    ) -> bool:
        # Notebook query windows compare the raw Timestamp string directly.
        timestamp = transaction.date.strip()
        return start_timestamp <= timestamp <= end_timestamp

    def _do_broadcast(self, action: BroadcastAction, control_senders: dict) -> None:
        if action.sleep_before > 0:
            time.sleep(action.sleep_before)
        for qname in action.queue_names:
            control_senders[qname].send(action.message)

    @staticmethod
    def _close_queue_dict(queues: dict) -> None:
        for q in queues.values():
            try:
                q.close()
            except Exception:
                pass

    # ─── data path ───────────────────────────────────────────────────────────

    def _process_data_message(self, message):
        msg_type, client_id, payload = self.internal_packet_serializer.unpack_packet(message)

        with self.lock:
            if client_id in self.closed_by_client:
                logging.info(
                    f"Received message for closed client {client_id} "
                    f"in filter_{CONFIGURATION} with id {ID}, ignoring"
                )
                return

        if msg_type == message_protocol.internal.MessageType.DATA:
            transactions = self.transaction_serializer.deserialize_batch(payload)
            if not transactions:
                return

            with self.lock:
                if client_id not in self.first_data_logged_by_client:
                    self.first_data_logged_by_client.add(client_id)
                    logging.info(
                        "filter_first_chunk_received | filter=%s | id=%s | "
                        "client_id=%s | message_bytes=%s | payload_bytes=%s | "
                        "batch_size=%s",
                        CONFIGURATION, ID, client_id,
                        len(message), len(payload), len(transactions),
                    )
                prev_deserialized = self.deserialized_by_client.get(client_id, 0)
                self.deserialized_by_client[client_id] = (
                    prev_deserialized + len(transactions)
                )

            for offset, transaction in enumerate(transactions, start=1):
                tx_number = prev_deserialized + offset
                if tx_number <= 3:
                    logging.info(
                        "filter_transaction_deserialized | filter=%s | id=%s | "
                        "client_id=%s | transaction_number=%s | date=%s | "
                        "from_bank=%s | from_account=%s | to_bank=%s | "
                        "to_account=%s | amount=%s | currency=%s | format=%s",
                        CONFIGURATION, ID, client_id, tx_number,
                        transaction.date, transaction.from_bank,
                        transaction.from_account, transaction.to_bank,
                        transaction.to_account, transaction.amount,
                        transaction.currency, transaction.format,
                    )
                if tx_number == 3:
                    logging.info(
                        "==================== Forward pass successful - Mate | "
                        "filter=%s | id=%s | client_id=%s | "
                        "transactions_deserialized=%s ====================",
                        CONFIGURATION, ID, client_id, tx_number,
                    )

            forwarded_in_batch = 0
            for transaction in transactions:
                if CONFIGURATION == C_DATE or self._filter_transaction(transaction):
                    if self._forward_transaction(transaction, client_id):
                        forwarded_in_batch += 1

            with self.lock:
                self.forwarded_by_client[client_id] = (
                    self.forwarded_by_client.get(client_id, 0) + forwarded_in_batch
                )
                self.processed_by_client[client_id] = (
                    self.processed_by_client.get(client_id, 0) + len(transactions)
                )
                processed_total = self.processed_by_client[client_id]
                forwarded_total = self.forwarded_by_client[client_id]

            if should_log_progress(processed_total):
                logging.info(
                    "filter_data_batch | filter=%s | id=%s | client_id=%s | "
                    "batch_size=%s | forwarded_in_batch=%s | processed_total=%s | "
                    "forwarded_total=%s",
                    CONFIGURATION, ID, client_id,
                    len(transactions), forwarded_in_batch,
                    processed_total, forwarded_total,
                )

        elif msg_type == message_protocol.internal.MessageType.EOF:
            control_message = self.control_serializer.deserialize(payload)
            expected_total = control_message.expected_total

            logging.info(
                "filter_upstream_eof | filter=%s | id=%s | client_id=%s | "
                "expected_total=%s | filter_amount=%s",
                CONFIGURATION, ID, client_id, expected_total, FILTER_AMOUNT,
            )

            with self.lock:
                count = self.processed_by_client.get(client_id, 0)
                fwd = sum(self.forwarded_by_output_by_client.get(client_id, {}).values())
                action = self.coordinator.on_upstream_eof(
                    client_id, expected_total, count, fwd
                )

            if action is None:
                return

            if isinstance(action, FlushAction):
                # N=1: todos los datos ya fueron procesados (orden AMQP garantizado)
                self._flush_batcher_for_client(client_id)
                with self.lock:
                    forwarded_by_output = dict(
                        self.forwarded_by_output_by_client.get(client_id, {})
                    )
                logging.info(
                    "filter_single_instance_flush | filter=%s | id=%s | "
                    "client_id=%s | expected_total=%s | forwarded_by_output=%s",
                    CONFIGURATION, ID, client_id, expected_total, forwarded_by_output,
                )
                self._forward_eof(client_id, forwarded_by_output)
                self._cleanup_client(client_id)

            elif isinstance(action, BroadcastAction):
                self._do_broadcast(action, self._main_control_senders)

        else:
            logging.warning(
                f"Received unknown message type: {msg_type} "
                f"for filter_{CONFIGURATION}"
            )

    def process_data_messages(self, message, ack, nack):
        try:
            self._process_data_message(message)
            ack()
        except Exception as e:
            logging.error(
                f"Error processing data message in filter_{CONFIGURATION} "
                f"with id {ID}: {e}"
            )
            nack()

    # ─── control path ────────────────────────────────────────────────────────

    def _handle_control(self, message, ack, nack, response_senders, output_queues):
        try:
            msg_type, client_id, payload = self.internal_packet_serializer.unpack_packet(
                message
            )

            if msg_type == MessageType.FLUSH_ORDER:
                ctrl = self.control_serializer.deserialize(payload)
                with self.lock:
                    action = self.coordinator.process_control_message(
                        msg_type, client_id, ctrl
                    )
                # None → líder dinámico ignora su propio FLUSH_ORDER
                if isinstance(action, FlushAction):
                    # No-líder: flush + enviar JSON FLUSH_ACK al líder dinámico
                    self._flush_batcher_for_client(client_id, output_queues)
                    with self.lock:
                        forwarded_by_output = dict(
                            self.forwarded_by_output_by_client.get(client_id, {})
                        )
                        self.coordinator.cleanup_client(client_id)
                        self._cleanup_client_locked(client_id)
                    if self._batcher is not None:
                        self._batcher.discard_client(client_id)
                    ack_payload = json.dumps(
                        {"sender_id": ID, "forwarded_by_output": forwarded_by_output}
                    ).encode("utf-8")
                    ack_message = self.internal_packet_serializer.create_packet(
                        msg_type=MessageType.FLUSH_ACK,
                        client_id_bytes=client_id.to_bytes(16, byteorder="big"),
                        payload=ack_payload,
                    )
                    response_senders[action.ack_queue].send(ack_message)
                    logging.info(
                        "filter_flush_ack_sent | filter=%s | id=%s | "
                        "client_id=%s | forwarded_by_output=%s",
                        CONFIGURATION, ID, client_id, forwarded_by_output,
                    )
                ack()

            else:
                ctrl = self.control_serializer.deserialize(payload)
                with self.lock:
                    count = self.processed_by_client.get(client_id, 0)
                    fwd = sum(
                        self.forwarded_by_output_by_client.get(client_id, {}).values()
                    )
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
                            "filter_send_answer_error | filter=%s | id=%s | "
                            "client_id=%s",
                            CONFIGURATION, ID, client_id,
                        )
                        with self.lock:
                            self.coordinator.clear_pending_eof(client_id)
                        nack()
                else:
                    logging.warning(
                        "filter_unexpected_control_action | filter=%s | id=%s | "
                        "action=%s",
                        CONFIGURATION, ID, action,
                    )
                    ack()

        except Exception:
            logging.exception(
                "filter_control_error | filter=%s | id=%s", CONFIGURATION, ID
            )
            nack()

    def _run_control_consumer(self) -> None:
        consumer = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, self.coordinator.my_control_queue()
        )
        response_senders = self._new_response_senders()
        output_queues = self._new_output_queues()

        with self._stopped_lock:
            self._control_consumer = consumer
            already_stopped = not self.active

        if already_stopped:
            try:
                consumer.close()
            except Exception:
                pass
            self._close_queue_dict(response_senders)
            self._close_queue_dict(output_queues)
            return

        try:
            consumer.start_consuming(
                lambda msg, ack, nack: self._handle_control(
                    msg, ack, nack, response_senders, output_queues
                )
            )
        except Exception as e:
            if not self._closed:
                logging.error(
                    "filter_control_consumer_stopped | filter=%s | id=%s | error=%s",
                    CONFIGURATION, ID, e,
                )
        finally:
            try:
                consumer.close()
            except Exception:
                pass
            self._close_queue_dict(response_senders)
            self._close_queue_dict(output_queues)

    # ─── response path ────────────────────────────────────────────────────────

    def _handle_response(self, message, ack, nack, control_senders, output_queues):
        try:
            msg_type, client_id, payload = self.internal_packet_serializer.unpack_packet(
                message
            )

            if msg_type == MessageType.FLUSH_ACK:
                # Payload JSON con desglose por output — manejo manual
                self._handle_flush_ack(client_id, payload, output_queues)
                ack()
                return

            ctrl = self.control_serializer.deserialize(payload)
            with self.lock:
                action = self.coordinator.process_control_message(
                    msg_type, client_id, ctrl
                )

            if action is None:
                ack()
                return

            if isinstance(action, BroadcastAction):
                try:
                    self._do_broadcast(action, control_senders)
                    ack()
                except Exception:
                    logging.exception(
                        "filter_response_broadcast_error | filter=%s | id=%s | "
                        "client_id=%s",
                        CONFIGURATION, ID, client_id,
                    )
                    nack()
            else:
                logging.warning(
                    "filter_unexpected_response_action | filter=%s | id=%s | "
                    "action=%s",
                    CONFIGURATION, ID, action,
                )
                ack()

        except Exception:
            logging.exception(
                "filter_response_error | filter=%s | id=%s", CONFIGURATION, ID
            )
            nack()

    def _handle_flush_ack(self, client_id, payload, output_queues=None):
        with self.lock:
            if client_id in self.closed_by_client:
                logging.info(
                    "filter_flush_ack_for_closed_client | filter=%s | id=%s | "
                    "client_id=%s",
                    CONFIGURATION, ID, client_id,
                )
                return

        sender_id, counts = self._decode_flush_ack_payload(payload, output_queues)

        if client_id not in self.flushed_acks_by_client:
            self.flushed_acks_by_client[client_id] = set()
        self.flushed_acks_by_client[client_id].add(sender_id)

        agg = self.all_forwarded_by_output_by_client.setdefault(client_id, {})
        for output, value in counts.items():
            agg[output] = agg.get(output, 0) + int(value)

        if len(self.flushed_acks_by_client[client_id]) == FILTER_AMOUNT - 1:
            logging.info(
                "filter_flush_ack_complete | filter=%s | id=%s | client_id=%s | "
                "acks=%s | forwarding_eof=true",
                CONFIGURATION, ID, client_id,
                len(self.flushed_acks_by_client[client_id]),
            )
            self._flush_batcher_for_client(client_id, output_queues)
            global_counts = dict(agg)
            with self.lock:
                local_counts = self.forwarded_by_output_by_client.get(client_id, {})
                if local_counts:
                    for output, value in local_counts.items():
                        global_counts[output] = global_counts.get(output, 0) + value
                elif client_id in self.forwarded_by_client:
                    outputs = output_queues or self.output_queues
                    if len(outputs) == 1:
                        output = next(iter(outputs))
                        global_counts[output] = (
                            global_counts.get(output, 0)
                            + self.forwarded_by_client.get(client_id, 0)
                        )
            self._forward_eof(client_id, global_counts, output_queues)
            if self._batcher is not None:
                self._batcher.discard_client(client_id)
            with self.lock:
                self.coordinator.cleanup_leader_state(client_id)
                self._cleanup_client_locked(client_id)

    def _decode_flush_ack_payload(self, payload, output_queues=None):
        try:
            data = json.loads(payload.decode("utf-8"))
            return data.get("sender_id"), data.get("forwarded_by_output", {})
        except (UnicodeDecodeError, json.JSONDecodeError):
            control = self.control_serializer.deserialize(payload)
            outputs = output_queues or self.output_queues
            if len(outputs) != 1:
                raise ValueError(
                    "legacy FLUSH_ACK payload can only be mapped with one output queue"
                )
            output = next(iter(outputs))
            return control.sender_id, {output: control.processed_count}

    def _run_response_consumer(self) -> None:
        consumer = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, self.coordinator.my_response_queue()
        )
        control_senders = self._new_control_senders()
        output_queues = self._new_output_queues()

        with self._stopped_lock:
            self._response_consumer = consumer
            already_stopped = not self.active

        if already_stopped:
            try:
                consumer.close()
            except Exception:
                pass
            self._close_queue_dict(control_senders)
            self._close_queue_dict(output_queues)
            return

        try:
            consumer.start_consuming(
                lambda msg, ack, nack: self._handle_response(
                    msg, ack, nack, control_senders, output_queues
                )
            )
        except Exception as e:
            if not self._closed:
                logging.error(
                    "filter_response_consumer_stopped | filter=%s | id=%s | error=%s",
                    CONFIGURATION, ID, e,
                )
        finally:
            try:
                consumer.close()
            except Exception:
                pass
            self._close_queue_dict(control_senders)
            self._close_queue_dict(output_queues)

    # ─── lifecycle ────────────────────────────────────────────────────────────

    def start(self):
        self._ensure_output_bindings()
        self.control_thread = threading.Thread(target=self._run_control_consumer)
        self.response_thread = threading.Thread(target=self._run_response_consumer)
        self.control_thread.start()
        self.response_thread.start()

        try:
            if self.active:
                self.input_queue.start_consuming(self.process_data_messages)
        except Exception as e:
            logging.error(f"Error in filter_{CONFIGURATION} with id {ID}: {e}")
        finally:
            self.handle_sigterm()
            if self.control_thread is not None:
                self.control_thread.join(timeout=5)
            if self.response_thread is not None:
                self.response_thread.join(timeout=5)
            self.close()

    def handle_sigterm(self):
        with self._stopped_lock:
            if not self.active:
                return
            self.active = False
            control_consumer = self._control_consumer
            response_consumer = self._response_consumer

        logging.info(
            f"Received SIGTERM in filter_{CONFIGURATION} with id {ID}, shutting down"
        )
        self.input_queue.request_stop_consuming()
        for consumer in (control_consumer, response_consumer):
            if consumer is not None:
                consumer.request_stop_consuming()

    def close(self):
        if self._closed:
            return
        self._closed = True
        logging.info(f"Closing filter_{CONFIGURATION} with id {ID}")

        try:
            self.input_queue.close()
        except Exception as e:
            logging.error(
                f"Error closing input queue in filter_{CONFIGURATION} with id {ID}: {e}"
            )

        self._close_queue_dict(self._main_control_senders)

        for queue in self.output_queues.values():
            try:
                queue.close()
            except Exception as e:
                logging.error(
                    f"Error closing output queue in filter_{CONFIGURATION} "
                    f"with id {ID}: {e}"
                )

        for exchange in self.output_exchanges:
            try:
                exchange.close()
            except Exception as e:
                logging.error(
                    f"Error closing output exchange in filter_{CONFIGURATION} "
                    f"with id {ID}: {e}"
                )
