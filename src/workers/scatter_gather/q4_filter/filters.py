import logging
import os
import threading
from dataclasses import dataclass, field

from common.bank_ids import notebook_bank_id
from common.batch_buffer import BatchBuffer
from common.logging_utils import should_log_progress
from common.message_protocol.internal import (
    InternalProtocol,
    Q4CountedEdge,
    Q4CountedEdgeSerializer,
    Q4TransactionEdge,
    Q4_EDGE_INCOMING,
    Q4_EDGE_OUTGOING,
    Q4AccountId,
    TransactionSerializer,
    partition_for_parts,
)
from common.message_protocol.internal.common import ControlMessage, MessageType
from common.message_protocol.internal.control_message_serializer import (
    ControlMessageSerializer,
)
from common.middleware.middleware_rabbitmq import (
    MessageMiddlewareExchangeRabbitMQ,
    MessageMiddlewareQueueRabbitMQ,
)


ID = int(os.environ["ID"])
LEADER_ID = 0
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ.get("INPUT_QUEUE", "q4_filter_input")
INPUT_EXCHANGE = os.environ.get("Q4_FILTER_INPUT_EXCHANGE")
INPUT_ROUTING_PREFIX = os.environ.get(
    "Q4_FILTER_INPUT_ROUTING_PREFIX", "q4_filter"
)
Q4_FILTER_AMOUNT = int(os.environ.get("Q4_FILTER_AMOUNT", "1"))
Q4_FILTER_PREFIX = os.environ.get(
    "Q4_FILTER_PREFIX", "q4_filter"
)
CONTROL_EXCHANGE = os.environ.get(
    "Q4_FILTER_CONTROL_EXCHANGE",
    f"{Q4_FILTER_PREFIX}_control",
)
RESPONSE_QUEUE_PREFIX = os.environ.get(
    "Q4_FILTER_RESPONSE_QUEUE_PREFIX",
    f"{Q4_FILTER_PREFIX}_response",
)
Q4_SUM_EXCHANGE = os.environ["Q4_SUM_EXCHANGE"]
Q4_SUM_AMOUNT = int(os.environ["Q4_SUM_AMOUNT"])
Q4_SUM_ROUTING_PREFIX = os.environ.get(
    "Q4_SUM_ROUTING_PREFIX", "q4_sum"
)
Q4_FILTER_BATCH_BYTES = int(
    os.environ.get("Q4_FILTER_BATCH_BYTES", str(1024 * 1024))
)
Q4_FILTER_BATCH_MAX_EDGES = int(
    os.environ.get("Q4_FILTER_BATCH_MAX_EDGES", "5000")
)


@dataclass
class _SourceState:
    targets: set[Q4AccountId] = field(default_factory=set)
    qualified: bool = False
    pending: list[Q4TransactionEdge] = field(default_factory=list)


class Q4FilterWorker:
    """Notebook-exact Q4 source prefilter.

    The worker owns complete sources through upstream sharding. It keeps a capped
    target set and the source's pending rows in memory until the source reaches
    six distinct targets.
    """

    def __init__(self):
        self._input = self._new_input()
        self._edge_store_output = self._new_edge_store_output()
        self._control_sender = None
        if Q4_FILTER_AMOUNT > 1:
            self._control_sender = MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST,
                CONTROL_EXCHANGE,
                [CONTROL_EXCHANGE],
            )
        self._response_queue_name = f"{RESPONSE_QUEUE_PREFIX}_{ID}"

        self._proto = InternalProtocol()
        self._tx_serializer = TransactionSerializer()
        self._counted_edge_serializer = Q4CountedEdgeSerializer()
        self._control_serializer = ControlMessageSerializer()
        self._batcher = BatchBuffer(
            Q4_FILTER_BATCH_BYTES,
            Q4_FILTER_BATCH_MAX_EDGES,
        )

        self._lock = threading.Lock()
        self._states_by_client: dict[int, dict[Q4AccountId, _SourceState]] = {}
        self._processed_by_client: dict[int, int] = {}
        self._forwarded_by_partition_by_client: dict[int, dict[int, int]] = {}
        self._pending_eof_by_client: dict[int, tuple[int, int]] = {}
        self._leader_expected_by_client: dict[int, int] = {}
        self._leader_processed_by_client: dict[int, int] = {}
        self._leader_forwarded_by_client: dict[int, int] = {}
        self._flushed_by_client: set[int] = set()
        self._closed_by_client: set[int] = set()

        self._control_consumer = None
        self._response_consumer = None
        self._control_thread = None
        self._response_thread = None
        self._closed = False
        self._stopped = False

    def _new_input(self):
        if INPUT_EXCHANGE:
            return MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST,
                INPUT_EXCHANGE,
                [f"{INPUT_ROUTING_PREFIX}_{ID}"],
                queue_name=f"{INPUT_ROUTING_PREFIX}_{ID}",
                exclusive=False,
            )
        if Q4_FILTER_AMOUNT > 1:
            logging.warning(
                "q4_filter_queue_input_with_multiple_workers | "
                "amount=%s | exact_source_ownership_requires_input_exchange",
                Q4_FILTER_AMOUNT,
            )
        return MessageMiddlewareQueueRabbitMQ(MOM_HOST, INPUT_QUEUE)

    def _new_edge_store_output(self):
        return MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST,
            Q4_SUM_EXCHANGE,
            [],
        )

    def _packet(self, msg_type: MessageType, client_id: int, payload: bytes) -> bytes:
        return self._proto.create_packet(
            msg_type=msg_type,
            client_id_bytes=client_id.to_bytes(16, byteorder="big"),
            payload=payload,
        )

    def _control_payload(
        self,
        sender_id: int,
        expected_total: int,
        processed_count: int,
    ) -> bytes:
        return self._control_serializer.serialize(
            ControlMessage(
                sender_id=sender_id,
                expected_total=expected_total,
                processed_count=processed_count,
            )
        )

    def _routing_key(self, partition: int) -> str:
        return f"{Q4_SUM_ROUTING_PREFIX}_{partition}"

    def _edge_store_partition(self, account_id: Q4AccountId) -> int:
        return partition_for_parts(
            (account_id.bank_id, account_id.account),
            Q4_SUM_AMOUNT,
        )

    def _edge_from_transaction(self, tx) -> Q4TransactionEdge:
        return Q4TransactionEdge(
            source=Q4AccountId(
                bank_id=notebook_bank_id(tx.from_bank),
                account=(tx.from_account or "").strip(),
            ),
            target=Q4AccountId(
                bank_id=notebook_bank_id(tx.to_bank),
                account=(tx.to_account or "").strip(),
            ),
        )

    def _append_pending_edge(
        self,
        edge: Q4TransactionEdge,
        state: _SourceState,
    ) -> None:
        state.pending.append(edge)

    def _replay_pending_edges(
        self,
        client_id: int,
        state: _SourceState,
        output=None,
    ) -> int:
        forwarded = 0
        for edge in state.pending:
            forwarded += self._emit_qualified_edge(client_id, edge, output)
        state.pending = []
        return forwarded

    def _emit_counted_edge(
        self,
        client_id: int,
        edge: Q4CountedEdge,
        output=None,
    ) -> None:
        output = output or self._edge_store_output
        partition = self._edge_store_partition(edge.intermediate)
        payload = self._counted_edge_serializer.serialize(edge)
        batch_payload = self._batcher.append((client_id, partition), payload)
        counts = self._forwarded_by_partition_by_client.setdefault(client_id, {})
        counts[partition] = counts.get(partition, 0) + 1
        if batch_payload is not None:
            self._send_batch(client_id, partition, batch_payload, output)

    def _emit_qualified_edge(
        self,
        client_id: int,
        edge: Q4TransactionEdge,
        output=None,
    ) -> int:
        self._emit_counted_edge(
            client_id,
            Q4CountedEdge(
                role=Q4_EDGE_INCOMING,
                intermediate=edge.target,
                endpoint=edge.source,
                count=1,
            ),
            output,
        )
        self._emit_counted_edge(
            client_id,
            Q4CountedEdge(
                role=Q4_EDGE_OUTGOING,
                intermediate=edge.source,
                endpoint=edge.target,
                count=1,
            ),
            output,
        )
        return 2

    def _send_batch(
        self,
        client_id: int,
        partition: int,
        batch_payload: bytes,
        output=None,
    ) -> None:
        output = output or self._edge_store_output
        output.send(
            self._packet(MessageType.DATA, client_id, batch_payload),
            routing_key=self._routing_key(partition),
        )

    def _flush_client_buffers(self, client_id: int, output=None) -> None:
        for (_, partition), batch_payload in self._batcher.flush(
            lambda k: k[0] == client_id
        ):
            self._send_batch(client_id, partition, batch_payload, output)

    def _accept_edge_locked(
        self,
        client_id: int,
        edge: Q4TransactionEdge,
        output=None,
    ) -> int:
        states = self._states_by_client.setdefault(client_id, {})
        state = states.setdefault(edge.source, _SourceState())

        if state.qualified:
            return self._emit_qualified_edge(client_id, edge, output)

        self._append_pending_edge(edge, state)
        if len(state.targets) < 6:
            state.targets.add(edge.target)

        if len(state.targets) < 6:
            return 0

        state.qualified = True
        state.targets.clear()
        return self._replay_pending_edges(client_id, state, output)

    def _handle_data_packet(self, client_id: int, payload: bytes, output=None) -> None:
        transactions = self._tx_serializer.deserialize_batch(payload)
        if not transactions:
            return

        edges = [self._edge_from_transaction(tx) for tx in transactions]
        with self._lock:
            if client_id in self._closed_by_client:
                logging.info(
                    "q4_filter_message_for_closed_client | id=%s | "
                    "client_id=%s",
                    ID,
                    client_id,
                )
                return

            forwarded_count = 0
            for edge in edges:
                forwarded_count += self._accept_edge_locked(client_id, edge, output)

            self._processed_by_client[client_id] = (
                self._processed_by_client.get(client_id, 0) + len(edges)
            )
            processed_total = self._processed_by_client[client_id]
            forwarded_total = sum(
                self._forwarded_by_partition_by_client.get(client_id, {}).values()
            )
            pending = self._pending_eof_by_client.get(client_id)

        if should_log_progress(processed_total):
            logging.info(
                "q4_filter_data_batch | id=%s | client_id=%s | "
                "batch_size=%s | forwarded_in_batch=%s | processed_total=%s | "
                "forwarded_total=%s | pending_eof=%s",
                ID,
                client_id,
                len(edges),
                forwarded_count,
                processed_total,
                forwarded_total,
                pending is not None,
            )

        if pending is not None:
            self._flush_client_buffers(client_id, output)
            _, leader_id = pending
            self._report_to_leader(
                client_id,
                leader_id,
                processed_count=len(edges),
                forwarded_count=forwarded_count,
            )

        if Q4_FILTER_AMOUNT == 1:
            self._try_forward_single_worker_eof(client_id, output)

    def _handle_upstream_eof(self, client_id: int, payload: bytes) -> None:
        control = self._control_serializer.deserialize(payload)
        expected_total = control.expected_total

        if Q4_FILTER_AMOUNT == 1:
            with self._lock:
                if client_id in self._closed_by_client:
                    return
                self._pending_eof_by_client[client_id] = (expected_total, ID)
            self._try_forward_single_worker_eof(client_id)
            return

        with self._lock:
            if client_id in self._closed_by_client:
                return
            if client_id in self._pending_eof_by_client:
                logging.info(
                    "q4_filter_duplicate_upstream_eof | id=%s | "
                    "client_id=%s",
                    ID,
                    client_id,
                )
                return
            self._pending_eof_by_client[client_id] = (expected_total, LEADER_ID)
            processed_snapshot = self._processed_by_client.get(client_id, 0)
            forwarded_snapshot = sum(
                self._forwarded_by_partition_by_client.get(client_id, {}).values()
            )
            if ID == LEADER_ID:
                self._leader_expected_by_client[client_id] = expected_total

        logging.info(
            "q4_filter_upstream_eof | id=%s | client_id=%s | "
            "expected_total=%s | processed_snapshot=%s | forwarded_snapshot=%s",
            ID,
            client_id,
            expected_total,
            processed_snapshot,
            forwarded_snapshot,
        )
        self._report_to_leader(
            client_id,
            LEADER_ID,
            processed_count=processed_snapshot,
            forwarded_count=forwarded_snapshot,
        )

    def _try_forward_single_worker_eof(self, client_id: int, output=None) -> None:
        with self._lock:
            pending = self._pending_eof_by_client.get(client_id)
            if pending is None or client_id in self._closed_by_client:
                return
            expected_total, _ = pending
            if self._processed_by_client.get(client_id, 0) < expected_total:
                return

        self._flush_and_close_client(client_id, output)

    def _report_to_leader(
        self,
        client_id: int,
        leader_id: int,
        processed_count: int,
        forwarded_count: int,
    ) -> None:
        response_queue = MessageMiddlewareQueueRabbitMQ(
            MOM_HOST,
            f"{RESPONSE_QUEUE_PREFIX}_{leader_id}",
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

    def _send_flush_order(self, client_id: int) -> None:
        if self._control_sender is None:
            raise RuntimeError("q4 source prefilter control sender is not configured")
        self._control_sender.send(
            self._packet(
                MessageType.FLUSH_ORDER,
                client_id,
                self._control_payload(ID, 0, 0),
            )
        )

    def _handle_leader_report(self, message: bytes, ack, nack) -> None:
        try:
            msg_type, client_id, payload = self._proto.unpack_packet(message)
            if msg_type != MessageType.PROCESSED_ANSWER:
                raise ValueError(
                    f"unexpected q4 source prefilter response type: {msg_type}"
                )

            control = self._control_serializer.deserialize(payload)
            should_flush = False
            expected_total = None
            processed_total = None

            with self._lock:
                if client_id in self._flushed_by_client:
                    ack()
                    return
                self._leader_processed_by_client[client_id] = (
                    self._leader_processed_by_client.get(client_id, 0)
                    + control.processed_count
                )
                self._leader_forwarded_by_client[client_id] = (
                    self._leader_forwarded_by_client.get(client_id, 0)
                    + control.expected_total
                )
                expected_total = self._leader_expected_by_client.get(client_id)
                processed_total = self._leader_processed_by_client[client_id]

                if expected_total is not None and processed_total >= expected_total:
                    self._flushed_by_client.add(client_id)
                    should_flush = True

            if should_flush:
                logging.info(
                    "q4_filter_flush_ready | id=%s | client_id=%s | "
                    "processed_total=%s | expected_total=%s",
                    ID,
                    client_id,
                    processed_total,
                    expected_total,
                )
                self._send_flush_order(client_id)

            ack()
        except Exception:
            logging.exception("q4_filter_response_error | id=%s", ID)
            nack()

    def _handle_flush_order(self, message: bytes, ack, nack, output=None) -> None:
        try:
            msg_type, client_id, _ = self._proto.unpack_packet(message)
            if msg_type != MessageType.FLUSH_ORDER:
                raise ValueError(
                    f"unexpected q4 source prefilter control type: {msg_type}"
                )

            self._flush_and_close_client(client_id, output)
            ack()
        except Exception:
            logging.exception("q4_filter_control_error | id=%s", ID)
            nack()

    def _forward_eof_to_edge_store(
        self,
        client_id: int,
        counts_by_partition: dict[int, int],
        output=None,
    ) -> None:
        output = output or self._edge_store_output
        for partition in range(Q4_SUM_AMOUNT):
            expected_total = int(counts_by_partition.get(partition, 0))
            output.send(
                self._packet(
                    MessageType.EOF,
                    client_id,
                    self._control_payload(
                        sender_id=ID,
                        expected_total=expected_total,
                        processed_count=0,
                    ),
                ),
                routing_key=self._routing_key(partition),
            )
            logging.info(
                "q4_filter_forward_eof | id=%s | client_id=%s | "
                "edge_store_partition=%s | expected_total=%s",
                ID,
                client_id,
                partition,
                expected_total,
            )

    def _flush_and_close_client(self, client_id: int, output=None) -> None:
        with self._lock:
            if client_id in self._closed_by_client:
                return
            counts_by_partition = dict(
                self._forwarded_by_partition_by_client.get(client_id, {})
            )
            self._states_by_client.pop(client_id, None)
            self._processed_by_client.pop(client_id, None)
            self._forwarded_by_partition_by_client.pop(client_id, None)
            self._pending_eof_by_client.pop(client_id, None)
            self._leader_expected_by_client.pop(client_id, None)
            self._leader_processed_by_client.pop(client_id, None)
            self._leader_forwarded_by_client.pop(client_id, None)
            self._flushed_by_client.discard(client_id)
            self._closed_by_client.add(client_id)

        self._flush_client_buffers(client_id, output)
        self._forward_eof_to_edge_store(client_id, counts_by_partition, output)

    def _on_message(self, raw, ack, nack):
        try:
            msg_type, client_id, payload = self._proto.unpack_packet(raw)
            if msg_type == MessageType.DATA:
                self._handle_data_packet(client_id, payload)
            elif msg_type == MessageType.EOF:
                self._handle_upstream_eof(client_id, payload)
            else:
                raise ValueError(f"unexpected q4 source prefilter message: {msg_type}")
            ack()
        except Exception:
            logging.exception("q4_filter_error | id=%s", ID)
            nack()

    def _start_control_consumer(self) -> None:
        control_consumer = MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST,
            CONTROL_EXCHANGE,
            [CONTROL_EXCHANGE],
        )
        self._control_consumer = control_consumer
        output = self._new_edge_store_output()
        try:
            if not self._stopped:
                control_consumer.start_consuming(
                    lambda message, ack, nack: self._handle_flush_order(
                        message, ack, nack, output
                    )
                )
        finally:
            output.close()
            control_consumer.close()

    def _start_response_consumer(self) -> None:
        response_consumer = MessageMiddlewareQueueRabbitMQ(
            MOM_HOST,
            self._response_queue_name,
        )
        self._response_consumer = response_consumer
        try:
            if not self._stopped:
                response_consumer.start_consuming(self._handle_leader_report)
        finally:
            response_consumer.close()

    def start(self) -> None:
        logging.info(
            "q4_filter_start | id=%s | amount=%s | input_queue=%s | "
            "input_exchange=%s | edge_store_exchange=%s | sum_amount=%s",
            ID,
            Q4_FILTER_AMOUNT,
            INPUT_QUEUE,
            INPUT_EXCHANGE,
            Q4_SUM_EXCHANGE,
            Q4_SUM_AMOUNT,
        )
        if Q4_FILTER_AMOUNT > 1:
            self._control_thread = threading.Thread(target=self._start_control_consumer)
            self._control_thread.start()
            if ID == LEADER_ID:
                self._response_thread = threading.Thread(
                    target=self._start_response_consumer
                )
                self._response_thread.start()

        try:
            if not self._stopped:
                self._input.start_consuming(self._on_message)
        finally:
            self.handle_sigterm()
            if self._control_thread is not None:
                self._control_thread.join(timeout=5)
            if self._response_thread is not None:
                self._response_thread.join(timeout=5)
            self.close()

    def handle_sigterm(self) -> None:
        if self._stopped:
            return
        self._stopped = True
        logging.info("q4_filter_shutdown | id=%s", ID)
        self._input.request_stop_consuming()
        if self._control_consumer is not None:
            self._control_consumer.request_stop_consuming()
        if self._response_consumer is not None:
            self._response_consumer.request_stop_consuming()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        for resource in (self._input, self._edge_store_output, self._control_sender):
            if resource is None:
                continue
            try:
                resource.close()
            except Exception as e:
                logging.warning(
                    "q4_filter_close_error | id=%s | error=%s",
                    ID,
                    e,
                )
