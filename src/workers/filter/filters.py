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
from common.routing import queue_name_for_worker, shard_for_client_id
from common.fault_tolerance.handler import EdgeSpec, PersistentStateHandler, WorkerRunner
from common.fault_tolerance.handler.action import Action
from common.fault_tolerance.inbox import InboxStatus, MsgKind

try:
    from filter_state import FilterState
except ImportError:
    from workers.filter.filter_state import FilterState

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
STATE_DIR = os.environ.get("STATE_DIR", "/tmp/filter_state")
SNAPSHOT_INTERVAL = int(os.environ.get("SNAPSHOT_INTERVAL", "1000"))
LEADER_ID = 0


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

        # Durable state + handler. FilterState owns the output batcher buffer
        # (per (destination, client_id)); the worker drives it through the WAL.
        self._state = FilterState(
            self.coordinator, FILTER_OUTPUT_BATCH_BYTES, FILTER_OUTPUT_BATCH_MAX_TX
        )
        self._output_edges = self._build_output_edges()
        self._handler = PersistentStateHandler(
            state_dir=STATE_DIR,
            node_id=f"filter_{CONFIGURATION}_{ID}",
            worker_state=self._state,
            snapshot_every=SNAPSHOT_INTERVAL,
            output_edges=self._output_edges,
        )
        self._runner: WorkerRunner | None = None
        self._data_publishers: dict | None = None

        # One lock serializes the handler (data thread) with every coordinator
        # access on the control/response threads, so the snapshot is consistent.
        self.lock = threading.Lock()
        self._stopped_lock = threading.Lock()
        self.active = True
        self._closed = False
        self.control_thread = None
        self.response_thread = None
        self._control_consumer = None
        self._response_consumer = None

        # Logging-only (non-durable): marks the first chunk seen per client.
        self.first_data_logged_by_client = set()
        self.deserialized_by_client = {}
        self._q4_next_seq_by_client_partition = {}

        # Outputs whose downstream consumer (a WAL-wired worker) deduplicates by
        # (sender_id, seq): these edges carry addressed packets instead of basic ones.
        # Every other output stays basic so its consumer is unaffected.
        #   SUM_Q2_OUTPUT / SUM_Q3_QUEUE   -> sum workers
        #   FILTER_Q5_USD_QUEUE            -> filter_q5_usd
        #   FILTER_Q1_QUEUE / FILTER_DATE_QUEUE -> downstream filters (this wiring)
        self._addressed_outputs = {
            SUM_Q2_OUTPUT, SUM_Q3_QUEUE, FILTER_Q5_USD_QUEUE,
            FILTER_Q1_QUEUE, FILTER_DATE_QUEUE,
        }
        # The q4 entry (scatter_gather q4_filter) is WAL-wired and reads addressed
        # packets, so the q4 edges (per-partition routing keys, or the plain mapper
        # queue) must be addressed too — otherwise q4 hits a struct error and nacks.
        if CONFIGURATION == C_DATE and DATE_ENABLE_Q4:
            self._addressed_outputs.update(self._q4_filter_output_names())
        # q3_barrier (candidates stream) is WAL-wired and reads addressed packets too.
        if CONFIGURATION == C_DATE and DATE_ENABLE_Q3:
            self._addressed_outputs.add(Q3_CANDIDATES_QUEUE)

        # Monotonic seq per (output, client) for legacy addressed edges. Sharded
        # filter/sum edges are handled by the durable SenderSequencer via
        # _output_edges.
        self._sum_seq_lock = threading.Lock()
        self._sum_seq_by_output_client: dict[tuple[str, int], int] = {}

    def _build_output_edges(self) -> dict[str, EdgeSpec]:
        edges: dict[str, EdgeSpec] = {}
        if CONFIGURATION == C_Q5:
            edges[FILTER_Q5_USD_QUEUE] = EdgeSpec(ID, FILTER_Q5_USD_AMOUNT)
        if CONFIGURATION == C_USD:
            if USD_ENABLE_Q1:
                edges[FILTER_Q1_QUEUE] = EdgeSpec(ID, FILTER_Q1_AMOUNT)
            if USD_ENABLE_Q2:
                edges[SUM_Q2_OUTPUT] = EdgeSpec(ID, SUM_Q2_AMOUNT)
            if USD_ENABLE_DATE:
                edges[FILTER_DATE_QUEUE] = EdgeSpec(ID, FILTER_DATE_AMOUNT)
        if CONFIGURATION == C_DATE and DATE_ENABLE_Q3:
            edges[SUM_Q3_QUEUE] = EdgeSpec(ID, SUM_Q3_AMOUNT)
            edges[Q3_CANDIDATES_QUEUE] = EdgeSpec(ID, Q3_BARRIER_AMOUNT)
        if CONFIGURATION == C_DATE and DATE_ENABLE_Q4:
            for output_name in self._q4_filter_output_names():
                edges[output_name] = EdgeSpec(ID, 1)
        return edges

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
                key_fn=middleware.client_id_key,
            )
        if CONFIGURATION == C_USD:
            if USD_ENABLE_Q1:
                output_queues[FILTER_Q1_QUEUE] = middleware.ShardedPublisher(
                    MOM_HOST,
                    FILTER_Q1_EXCHANGE,
                    FILTER_Q1_ROUTING_PREFIX,
                    FILTER_Q1_AMOUNT,
                    key_fn=middleware.client_id_key,
                )
            if USD_ENABLE_Q2:
                output_queues[SUM_Q2_OUTPUT] = middleware.ShardedPublisher(
                    MOM_HOST,
                    SUM_Q2_EXCHANGE,
                    SUM_Q2_ROUTING_PREFIX,
                    SUM_Q2_AMOUNT,
                    key_fn=middleware.client_id_key,
                )
            if USD_ENABLE_DATE:
                output_queues[FILTER_DATE_QUEUE] = middleware.ShardedPublisher(
                    MOM_HOST,
                    FILTER_DATE_EXCHANGE,
                    FILTER_DATE_ROUTING_PREFIX,
                    FILTER_DATE_AMOUNT,
                    key_fn=middleware.client_id_key,
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
                    key_fn=middleware.client_id_key,
                )
                output_queues[Q3_CANDIDATES_QUEUE] = (
                    middleware.ShardedByClientPublisher(
                        MOM_HOST,
                        Q3_CANDIDATES_EXCHANGE,
                        Q3_CANDIDATES_ROUTING_PREFIX,
                        Q3_BARRIER_AMOUNT,
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
        if CONFIGURATION == C_USD:
            if USD_ENABLE_Q1:
                self._ensure_sharded_output_bindings(
                    FILTER_Q1_EXCHANGE, FILTER_Q1_ROUTING_PREFIX, FILTER_Q1_AMOUNT
                )
            if USD_ENABLE_Q2:
                self._ensure_sharded_output_bindings(
                    SUM_Q2_EXCHANGE, SUM_Q2_ROUTING_PREFIX, SUM_Q2_AMOUNT
                )
            if USD_ENABLE_DATE:
                self._ensure_sharded_output_bindings(
                    FILTER_DATE_EXCHANGE,
                    FILTER_DATE_ROUTING_PREFIX,
                    FILTER_DATE_AMOUNT,
                )
            return
        if CONFIGURATION == C_DATE and DATE_ENABLE_Q3:
            self._ensure_sharded_output_bindings(
                SUM_Q3_EXCHANGE, SUM_Q3_ROUTING_PREFIX, SUM_Q3_AMOUNT
            )
            self._ensure_sharded_output_bindings(
                Q3_CANDIDATES_EXCHANGE,
                Q3_CANDIDATES_ROUTING_PREFIX,
                Q3_BARRIER_AMOUNT,
            )
        if CONFIGURATION == C_DATE and DATE_ENABLE_Q4 and Q4_FILTER_INPUT_EXCHANGE:
            self._ensure_sharded_output_bindings(
                Q4_FILTER_INPUT_EXCHANGE,
                Q4_FILTER_INPUT_ROUTING_PREFIX,
                Q4_FILTER_AMOUNT,
            )

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

    def _is_q4_filter_exchange_output(self, queue_name: str) -> bool:
        return (
            CONFIGURATION == C_DATE
            and DATE_ENABLE_Q4
            and bool(Q4_FILTER_INPUT_EXCHANGE)
            and queue_name in self._q4_filter_output_names()
        )

    def _q4_filter_partition_from_output(self, queue_name: str) -> int:
        for partition in range(Q4_FILTER_AMOUNT):
            if queue_name == self._q4_filter_routing_key(partition):
                return partition
        raise ValueError(f"unknown q4 filter output queue: {queue_name!r}")

    def _q4_addressed_packet(
        self,
        msg_type: MessageType,
        client_id: int,
        partition: int,
        payload: bytes,
    ) -> bytes:
        seq_key = (client_id, partition)
        seq = self._q4_next_seq_by_client_partition.get(seq_key, 0)
        self._q4_next_seq_by_client_partition[seq_key] = seq + 1
        return self.internal_packet_serializer.create_addressed_packet(
            msg_type=msg_type,
            client_id_bytes=client_id.to_bytes(16, byteorder="big"),
            sender_id=ID,
            seq=seq,
            payload=payload,
        )

    def _next_sum_seq(self, output_name: str, client_id: int) -> int:
        with self._sum_seq_lock:
            key = (output_name, client_id)
            seq = self._sum_seq_by_output_client.get(key, 0)
            self._sum_seq_by_output_client[key] = (seq + 1) & 0xFFFFFFFF
            return seq

    def _output_shard(self, output_name: str, client_id: int) -> int | None:
        if output_name not in self._output_edges:
            return None
        return shard_for_client_id(client_id, self._output_edges[output_name].shard_count)

    def _logical_output(self, output_name: str, client_id: int, packet: bytes) -> tuple:
        shard = self._output_shard(output_name, client_id)
        if shard is None:
            return (output_name, packet)
        return (output_name, packet, shard)

    def _output_packet(
        self,
        output_name: str,
        msg_type,
        client_id: int,
        payload: bytes,
    ) -> bytes:
        # Sharded filter/sum edges are stamped by PersistentStateHandler's durable
        # SenderSequencer. q4 filter exchange edges are still pre-addressed per
        # (client, partition); remaining addressed edges keep the existing
        # in-memory sequence behavior.
        if output_name in self._output_edges:
            return self.internal_packet_serializer.create_packet(
                msg_type=msg_type,
                client_id_bytes=client_id.to_bytes(16, byteorder="big"),
                payload=payload,
            )
        if self._is_q4_filter_exchange_output(output_name):
            return self._q4_addressed_packet(
                msg_type,
                client_id,
                self._q4_filter_partition_from_output(output_name),
                payload,
            )
        if output_name in self._addressed_outputs:
            return self.internal_packet_serializer.create_addressed_packet(
                msg_type=msg_type,
                client_id_bytes=client_id.to_bytes(16, byteorder="big"),
                sender_id=ID,
                seq=self._next_sum_seq(output_name, client_id),
                payload=payload,
            )
        return self.internal_packet_serializer.create_packet(
            msg_type=msg_type,
            client_id_bytes=client_id.to_bytes(16, byteorder="big"),
            payload=payload,
        )

    def _route_transaction(self, transaction: Transaction) -> list[tuple[str, str]]:
        """(destination, logical_output) pairs this transaction is forwarded to,
        under the current CONFIGURATION. Empty if filtered out. Destination is the
        routing target; logical_output is the EOF/forwarded accounting bucket. They
        differ only for sharded q4 (per-partition destinations roll up to
        Q4_FILTER_OUTPUT). Mirrors the old _forward_transaction routing exactly."""
        if CONFIGURATION == C_Q1:
            return [(GATEWAY_QUEUE, GATEWAY_QUEUE)] if self._filter_transaction(transaction) else []
        if CONFIGURATION == C_Q5:
            return (
                [(FILTER_Q5_USD_QUEUE, FILTER_Q5_USD_QUEUE)]
                if self._filter_transaction(transaction) else []
            )
        if CONFIGURATION == C_USD:
            if not self._filter_transaction(transaction):
                return []
            outs = []
            if USD_ENABLE_Q1:
                outs.append((FILTER_Q1_QUEUE, FILTER_Q1_QUEUE))
            if USD_ENABLE_Q2:
                outs.append((SUM_Q2_OUTPUT, SUM_Q2_OUTPUT))
            if USD_ENABLE_DATE:
                outs.append((FILTER_DATE_QUEUE, FILTER_DATE_QUEUE))
            return outs
        if CONFIGURATION == C_DATE:
            outs = []
            if DATE_ENABLE_Q3 and self._filter_transaction_by_raw_timestamp(
                transaction, "2022/09/01", "2022/09/06"
            ):
                outs.append((SUM_Q3_QUEUE, SUM_Q3_QUEUE))
            if DATE_ENABLE_Q3 and self._filter_transaction_by_raw_timestamp(
                transaction, "2022/09/06", "2022/09/15"
            ):
                outs.append((Q3_CANDIDATES_QUEUE, Q3_CANDIDATES_QUEUE))
            if DATE_ENABLE_Q4 and self._filter_transaction_by_raw_timestamp(
                transaction, "2022/09/01", "2022/09/06"
            ):
                dest = self._q4_filter_output_for_transaction(transaction)
                logical = (
                    Q4_FILTER_OUTPUT if Q4_FILTER_INPUT_EXCHANGE
                    else SCATTER_GATHER_MAPPER_QUEUE
                )
                outs.append((dest, logical))
            return outs
        return []

    def _data_process_payload(self, client_id: int, payload: bytes):
        """business_fn for a DATA batch (runs inside the durable handler, NEW only):
        filter + route each transaction, append the passing ones to the durable
        buffer (predicted via plan_data so the flushed batches can be stamped into
        the outbox), and return (data_change, outputs). Pure w.r.t. worker state —
        apply_change performs the real buffer append, so WAL replay reproduces it."""
        if self._state.is_closed(client_id):
            return FilterState.data_change(client_id, 0, {}, {}), []
        transactions = self.transaction_serializer.deserialize_batch(payload)
        if not transactions:
            return FilterState.data_change(client_id, 0, {}, {}), []

        if client_id not in self.first_data_logged_by_client:
            self.first_data_logged_by_client.add(client_id)
            logging.info(
                "filter_first_chunk_received | filter=%s | id=%s | client_id=%s | "
                "batch_size=%s", CONFIGURATION, ID, client_id, len(transactions),
            )

        appends_by_dest: dict[str, list[bytes]] = {}
        forwarded_by_output: dict[str, int] = {}
        for transaction in transactions:
            routes = self._route_transaction(transaction)
            if not routes:
                continue
            serialized = self.transaction_serializer.serialize(transaction)
            for dest, logical in routes:
                appends_by_dest.setdefault(dest, []).append(serialized)
                forwarded_by_output[logical] = forwarded_by_output.get(logical, 0) + 1

        # plan_data predicts the flushes WITHOUT mutating; apply_change does the
        # real append under the same lock, matching these batches exactly.
        flushed_by_dest = self._state.plan_data(client_id, appends_by_dest)
        outputs = [
            self._logical_output(
                dest,
                client_id,
                self._output_packet(dest, MessageType.DATA, client_id, batch),
            )
            for dest, batches in flushed_by_dest.items()
            for batch in batches
        ]

        change = FilterState.data_change(
            client_id, len(transactions), appends_by_dest, forwarded_by_output
        )
        return change, outputs

    def _eof_outputs(self, client_id: int, counts_by_output: dict) -> list:
        """Logical EOF outputs for this config: one EOF per downstream output
        carrying that output's forwarded total. Sharded q4 fans the logical total
        out to every partition destination (matches the old _forward_eof)."""
        def count(name: str) -> int:
            return int(counts_by_output.get(name, 0))

        def eof(dest: str, total: int):
            payload = self.control_serializer.serialize(
                message_protocol.internal.ControlMessage(
                    sender_id=ID, expected_total=total, processed_count=0
                )
            )
            # _output_packet wraps addressed edges (SUM / filter / q5 / q4) with a
            # seq after the data seqs; every other edge stays basic.
            return self._logical_output(
                dest,
                client_id,
                self._output_packet(dest, MessageType.EOF, client_id, payload),
            )

        if CONFIGURATION == C_Q1:
            return [eof(GATEWAY_QUEUE, count(GATEWAY_QUEUE))]
        if CONFIGURATION == C_Q5:
            return [eof(FILTER_Q5_USD_QUEUE, count(FILTER_Q5_USD_QUEUE))]
        if CONFIGURATION == C_USD:
            outs = []
            if USD_ENABLE_Q1:
                outs.append(eof(FILTER_Q1_QUEUE, count(FILTER_Q1_QUEUE)))
            if USD_ENABLE_Q2:
                outs.append(eof(SUM_Q2_OUTPUT, count(SUM_Q2_OUTPUT)))
            if USD_ENABLE_DATE:
                outs.append(eof(FILTER_DATE_QUEUE, count(FILTER_DATE_QUEUE)))
            return outs
        if CONFIGURATION == C_DATE:
            outs = []
            if DATE_ENABLE_Q3:
                outs.append(eof(SUM_Q3_QUEUE, count(SUM_Q3_QUEUE)))
                outs.append(eof(Q3_CANDIDATES_QUEUE, count(Q3_CANDIDATES_QUEUE)))
            if DATE_ENABLE_Q4:
                total = (
                    count(Q4_FILTER_OUTPUT) if Q4_FILTER_INPUT_EXCHANGE
                    else count(SCATTER_GATHER_MAPPER_QUEUE)
                )
                outs.extend(eof(dest, total) for dest in self._q4_filter_output_names())
            return outs
        return []

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

    def _publish_outputs(self, entries, publishers: dict) -> None:
        """Publish stamped outbox entries, resolving each destination name against
        the given per-thread publishers map."""
        for entry in entries:
            publisher = publishers.get(entry.destination)
            if publisher is None:
                raise KeyError(f"no publisher for destination {entry.destination!r}")
            if entry.shard is None:
                publisher.send(entry.body)
            else:
                publisher.send_to_shard(entry.body, entry.shard)

    def _publish_commit_ack(self, instruction, ack, publishers: dict) -> bool:
        """Publish outputs, commit, ack. A duplicate/replayed input the inbox
        already finished comes back as Action.ACK with ctx=None — just ack it;
        never commit_done(*None). Crash-recovery path."""
        if instruction.action is Action.ACK:
            ack()
            return False
        self._publish_outputs(instruction.outputs, publishers)
        with self.lock:
            self._handler.commit_done(*instruction.ctx)
        ack()
        return True

    def _drain_outputs(self, client_id: int) -> list:
        """DATA outputs for the buffer's leftover batches per destination (read-only
        plan_drain; the close change discards the buffer in apply_change)."""
        return [
            self._logical_output(
                dest,
                client_id,
                self._output_packet(dest, MessageType.DATA, client_id, batch),
            )
            for dest, batch in self._state.plan_drain(client_id).items()
        ]

    def _on_input_message(self, message, ack, nack):
        """Dispatch by type: DATA through the durable runner; upstream EOF drives
        the broadcast coordinator (WAL-tracked)."""
        if not message:
            logging.error("filter_empty_message | filter=%s | id=%s", CONFIGURATION, ID)
            nack(requeue=False)
            return
        msg_type = message[0]
        if msg_type == MessageType.DATA:
            self._runner.process(message, ack, nack)
        elif msg_type == MessageType.EOF:
            self._handle_upstream_eof(message, ack, nack)
        else:
            logging.warning(
                "filter_unknown_message_type | filter=%s | id=%s | type=%s",
                CONFIGURATION, ID, msg_type,
            )
            nack(requeue=False)

    def _handle_upstream_eof(self, message, ack, nack):
        """Upstream EOF (addressed) through the handler as a durable input
        (kind=DATA): on_upstream_eof is idempotent so apply_change replays it.
        N>1 broadcasts EOF_RECEIVED; N=1 flushes the buffer + downstream EOF +
        close. Outputs ride the outbox and are re-published on recovery."""
        try:
            _msg_type, client_id, sender_id, seq, payload = (
                self.internal_packet_serializer.unpack_addressed_packet(message)
            )
            ctrl = self.control_serializer.deserialize(payload)
            expected_total = ctrl.expected_total
            msg_id = f"d:{sender_id}:{client_id}:{seq}"

            with self.lock:
                status = self._handler.inbox.classify(client_id, sender_id, seq)
                if self._state.is_closed(client_id) and status is not InboxStatus.APPLIED:
                    ack()
                    return
                count = self._state.processed_count(client_id)

                def bfn(_pl):
                    eof_change = FilterState.coordinator_upstream_eof_change(
                        client_id, expected_total, count, count
                    )
                    if FILTER_AMOUNT > 1:
                        msg = self.internal_packet_serializer.create_packet(
                            msg_type=MessageType.EOF_RECEIVED,
                            client_id_bytes=client_id.to_bytes(16, byteorder="big"),
                            payload=self.control_serializer.serialize(
                                message_protocol.internal.ControlMessage(
                                    sender_id=ID, expected_total=expected_total,
                                    processed_count=0,
                                )
                            ),
                        )
                        outputs = [
                            (self.coordinator.control_queue_for(i), msg)
                            for i in range(FILTER_AMOUNT)
                        ]
                        return eof_change, outputs
                    # N==1: flush the buffer + downstream EOF, then close.
                    outputs = self._drain_outputs(client_id)
                    outputs += self._eof_outputs(
                        client_id, self._state.forwarded_by_output(client_id)
                    )
                    compound = FilterState.compound_change(
                        eof_change, FilterState.close_change(client_id)
                    )
                    return compound, outputs

                instruction = self._handler.handle(
                    msg_id, client_id, sender_id, seq, payload, bfn
                )

            self._publish_commit_ack(instruction, ack, self._data_publishers)
            logging.info(
                "filter_upstream_eof | filter=%s | id=%s | client_id=%s | "
                "expected_total=%s | local_count=%s",
                CONFIGURATION, ID, client_id, expected_total, count,
            )
        except Exception:
            logging.exception(
                "filter_upstream_eof_error | filter=%s | id=%s", CONFIGURATION, ID
            )
            nack(requeue=True)

    # ─── control path ────────────────────────────────────────────────────────

    def _handle_control(self, message, ack, nack, publishers, response_senders):
        try:
            msg_type, client_id, ctrl = self.coordinator.parse_message(message)
        except Exception:
            logging.exception("filter_control_parse_error | filter=%s | id=%s", CONFIGURATION, ID)
            nack(requeue=False)
            return

        if msg_type == MessageType.FLUSH_ORDER:
            self._handle_flush_order(message, client_id, ctrl, ack, nack, publishers)
            return
        if msg_type == MessageType.EOF_RECEIVED:
            self._handle_eof_received(message, client_id, ctrl, ack, nack, publishers)
            return
        if msg_type == MessageType.PROCESSED_REQUEST:
            # Direct re-report (no state mutation); count from durable WAL state.
            try:
                with self.lock:
                    count = self._state.processed_count(client_id)
                    fwd = sum(self._state.forwarded_by_output(client_id).values())
                    action = self.coordinator.process_control_message(
                        MessageType.PROCESSED_REQUEST, client_id, ctrl, count, fwd
                    )
                if isinstance(action, SendAnswerAction):
                    response_senders[action.queue_name].send(action.message)
                ack()
            except Exception:
                logging.exception(
                    "filter_processed_request_error | filter=%s | id=%s | client_id=%s",
                    CONFIGURATION, ID, client_id,
                )
                nack(requeue=True)
            return
        logging.warning(
            "filter_unexpected_control_type | filter=%s | id=%s | msg_type=%s",
            CONFIGURATION, ID, msg_type,
        )
        ack()

    def _handle_eof_received(self, message, client_id, ctrl, ack, nack, publishers):
        # WAL-tracked (CTRL_EOF_RECEIVED) so _pending_eof is durable: a later
        # PROCESSED_REQUEST must find it to re-report. One per (client, leader).
        sender_id = ctrl.sender_id
        seq = client_id
        msg_id = f"er:{client_id}:{ctrl.sender_id}"
        try:
            with self.lock:
                count = self._state.processed_count(client_id)
                fwd = sum(self._state.forwarded_by_output(client_id).values())

                def bfn(_pl):
                    action = self.coordinator.process_control_message(
                        MessageType.EOF_RECEIVED, client_id, ctrl, count, fwd
                    )
                    change = FilterState.coordinator_msg_change(
                        MessageType.EOF_RECEIVED, client_id, ctrl.sender_id,
                        ctrl.expected_total, ctrl.processed_count, count, fwd,
                    )
                    if isinstance(action, SendAnswerAction):
                        return change, [(action.queue_name, action.message)]
                    return change, []

                instruction = self._handler.handle(
                    msg_id, client_id, sender_id, seq, message, bfn,
                    kind=MsgKind.CTRL_EOF_RECEIVED,
                )
            self._publish_commit_ack(instruction, ack, publishers)
        except Exception:
            logging.exception(
                "filter_eof_received_error | filter=%s | id=%s | client_id=%s",
                CONFIGURATION, ID, client_id,
            )
            nack(requeue=True)

    def _handle_flush_order(self, message, client_id, ctrl, ack, nack, publishers):
        # Non-leader: drain the buffer (final DATA batches) + send a custom JSON
        # FLUSH_ACK (per-output forwarded) to the leader, then cleanup + close.
        # The dynamic leader (it set _leader_expected) ignores its own FLUSH_ORDER.
        leader_id = ctrl.sender_id
        seq = client_id
        msg_id = f"fo:{client_id}:{ctrl.sender_id}"
        try:
            with self.lock:
                status = self._handler.inbox.classify(
                    client_id, leader_id, seq, MsgKind.CTRL_FLUSH_ORDER
                )
                if self._state.is_closed(client_id) and status is not InboxStatus.APPLIED:
                    ack()
                    return
                if self.coordinator.leader_expected(client_id) is not None:
                    ack()  # leader for this client; ignores FLUSH_ORDER
                    return

                def bfn(_pl):
                    drain = self._drain_outputs(client_id)
                    fwd_by_output = self._state.forwarded_by_output(client_id)
                    ack_payload = json.dumps(
                        {"sender_id": ID, "forwarded_by_output": fwd_by_output}
                    ).encode("utf-8")
                    flush_ack = self.internal_packet_serializer.create_packet(
                        msg_type=MessageType.FLUSH_ACK,
                        client_id_bytes=client_id.to_bytes(16, byteorder="big"),
                        payload=ack_payload,
                    )
                    flush_ack_dest = self.coordinator.response_queue_for(leader_id)
                    compound = FilterState.compound_change(
                        FilterState.coordinator_cleanup_change(client_id, is_leader=False),
                        FilterState.close_change(client_id),
                    )
                    return compound, drain + [(flush_ack_dest, flush_ack)]

                instruction = self._handler.handle(
                    msg_id, client_id, leader_id, seq, message, bfn,
                    kind=MsgKind.CTRL_FLUSH_ORDER,
                )
            self._publish_commit_ack(instruction, ack, publishers)
        except Exception:
            logging.exception(
                "filter_flush_order_error | filter=%s | id=%s | client_id=%s",
                CONFIGURATION, ID, client_id,
            )
            nack(requeue=True)

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

        # Combined publishers for the outbox (data destinations + the leader's
        # response queue for the FLUSH_ACK), resolved by destination name.
        publishers = {**output_queues, **response_senders}
        try:
            consumer.start_consuming(
                lambda msg, ack, nack: self._handle_control(
                    msg, ack, nack, publishers, response_senders
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

    def _handle_response(self, message, ack, nack, publishers, control_senders):
        # FLUSH_ACK carries a custom JSON payload (per-output counts), so peek the
        # type with unpack_packet rather than the coordinator's ControlMessage parse.
        try:
            msg_type, client_id, payload = self.internal_packet_serializer.unpack_packet(
                message
            )
        except Exception:
            logging.exception("filter_response_parse_error | filter=%s | id=%s", CONFIGURATION, ID)
            nack(requeue=False)
            return

        if msg_type == MessageType.FLUSH_ACK:
            self._handle_flush_ack(message, client_id, payload, ack, nack, publishers)
            return

        # PROCESSED_ANSWER: transient count-collection on the leader. Direct
        # coordinator call (rebuilt by redelivery + retry); broadcasts FLUSH_ORDER
        # or PROCESSED_REQUEST. Same accepted limitation as sum/aggregator.
        try:
            ctrl = self.control_serializer.deserialize(payload)
            with self.lock:
                action = self.coordinator.process_control_message(msg_type, client_id, ctrl)
            if isinstance(action, BroadcastAction):
                self._do_broadcast(action, control_senders)
            ack()
        except Exception:
            logging.exception(
                "filter_processed_answer_error | filter=%s | id=%s | client_id=%s",
                CONFIGURATION, ID, client_id,
            )
            nack(requeue=True)

    def _handle_flush_ack(self, message, client_id, payload, ack, nack, publishers):
        # Leader accumulates the per-output FLUSH_ACK counts via the WAL. On the last
        # ack (FILTER_AMOUNT-1) it drains its own buffer and emits one downstream EOF
        # per output with the cross-replica total, then closes. The single coordinator
        # mutation + close happen in apply_change.
        sender_id, counts = self._decode_flush_ack_payload(payload)
        seq = client_id
        msg_id = f"fa:{client_id}:{sender_id}"
        try:
            with self.lock:
                status = self._handler.inbox.classify(
                    client_id, sender_id, seq, MsgKind.CTRL_FLUSH_ACK
                )
                if self._state.is_closed(client_id) and status is not InboxStatus.APPLIED:
                    ack()
                    return
                if (
                    status is not InboxStatus.APPLIED
                    and self.coordinator.leader_expected(client_id) is None
                ):
                    ack()  # not the leader for this client
                    return

                # Inbox dedup makes each (client, sender) FLUSH_ACK NEW exactly once,
                # so this ack always increments the count by one.
                new_ack_count = self._state.flushed_ack_count(client_id) + 1

                def bfn(_pl):
                    ack_change = FilterState.flush_ack_change(client_id, sender_id, counts)
                    if new_ack_count < FILTER_AMOUNT - 1:
                        return ack_change, []
                    # Last ack: total = prior acks + this ack + leader's own.
                    total = dict(self._state.all_forwarded_by_output(client_id))
                    for out, value in counts.items():
                        total[out] = total.get(out, 0) + int(value)
                    for out, value in self._state.forwarded_by_output(client_id).items():
                        total[out] = total.get(out, 0) + value
                    outputs = self._drain_outputs(client_id) + self._eof_outputs(client_id, total)
                    compound = FilterState.compound_change(
                        ack_change,
                        FilterState.coordinator_cleanup_change(client_id, is_leader=True),
                        FilterState.close_change(client_id),
                    )
                    return compound, outputs

                instruction = self._handler.handle(
                    msg_id, client_id, sender_id, seq, message, bfn,
                    kind=MsgKind.CTRL_FLUSH_ACK,
                )
            self._publish_commit_ack(instruction, ack, publishers)
        except Exception:
            logging.exception(
                "filter_flush_ack_error | filter=%s | id=%s | client_id=%s",
                CONFIGURATION, ID, client_id,
            )
            nack(requeue=True)

    def _decode_flush_ack_payload(self, payload):
        data = json.loads(payload.decode("utf-8"))
        return data.get("sender_id"), data.get("forwarded_by_output", {})

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

        # Publishers for the leader's WAL outputs (drained DATA + downstream EOFs).
        publishers = {**output_queues, **control_senders}
        try:
            consumer.start_consuming(
                lambda msg, ack, nack: self._handle_response(
                    msg, ack, nack, publishers, control_senders
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

        # Build the data-thread publishers and runner, then recover (restoring the
        # coordinator + batcher buffer) BEFORE spawning the control/response threads,
        # so they never touch shared state mid-recovery. The map combines downstream
        # outputs + control + response queues because recovery re-publishes any
        # pending output, whose destination may be any of those.
        self._data_publishers = {
            **self.output_queues,
            **self._main_control_senders,
            **self._new_response_senders(),
        }
        self._runner = WorkerRunner(
            handler=self._handler,
            publishers=self._data_publishers,
            process_payload=self._data_process_payload,
            lock=self.lock,
        )
        self._runner.recover_and_republish()

        self.control_thread = threading.Thread(target=self._run_control_consumer)
        self.response_thread = threading.Thread(target=self._run_response_consumer)
        self.control_thread.start()
        self.response_thread.start()

        try:
            if self.active:
                self.input_queue.start_consuming(self._on_input_message)
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
