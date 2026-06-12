import logging
import os
import threading
import time
from dataclasses import dataclass, field

from common.bank_ids import notebook_bank_id
from common.batch_buffer import BatchBuffer
from common.eof_coordinator import (
    BroadcastAction,
    EofCoordinator,
    FlushAction,
    SendAnswerAction,
)
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
INPUT_EXCHANGE = os.environ.get("Q4_FILTER_INPUT_EXCHANGE")
INPUT_ROUTING_PREFIX = os.environ.get(
    "Q4_FILTER_INPUT_ROUTING_PREFIX", "q4_filter"
)
Q4_FILTER_AMOUNT = int(os.environ.get("Q4_FILTER_AMOUNT", "1"))
Q4_FILTER_PREFIX = os.environ.get("Q4_FILTER_PREFIX", "q4_filter")
Q4_SUM_EXCHANGE = os.environ["Q4_SUM_EXCHANGE"]
Q4_SUM_AMOUNT = int(os.environ["Q4_SUM_AMOUNT"])
Q4_SUM_ROUTING_PREFIX = os.environ.get("Q4_SUM_ROUTING_PREFIX", "q4_sum")
Q4_FILTER_BATCH_BYTES = int(
    os.environ.get("Q4_FILTER_BATCH_BYTES", str(1024 * 1024))
)
Q4_FILTER_BATCH_MAX_EDGES = int(
    os.environ.get("Q4_FILTER_BATCH_MAX_EDGES", "5000")
)
Q4_FILTER_CONTROL_QUEUE_PREFIX = f"{Q4_FILTER_PREFIX}_control"
Q4_FILTER_RESPONSE_QUEUE_PREFIX = f"{Q4_FILTER_PREFIX}_response"


@dataclass
class _SourceState:
    targets: set[Q4AccountId] = field(default_factory=set)
    qualified: bool = False
    pending: list[Q4TransactionEdge] = field(default_factory=list)


class Q4FilterWorker:
    def __init__(self):
        self._coordinator = EofCoordinator(
            instance_id=ID,
            total_instances=Q4_FILTER_AMOUNT,
            control_queue_prefix=Q4_FILTER_CONTROL_QUEUE_PREFIX,
            response_queue_prefix=Q4_FILTER_RESPONSE_QUEUE_PREFIX,
            mode="flush_order",
            leader_id=LEADER_ID,
        )

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
        self._closed_by_client: set[int] = set()

        self._input = self._new_input()
        self._edge_store_output = self._new_edge_store_output()
        self.control_consumer = None
        self.response_consumer = None
        self._control_thread = None
        self._response_thread = None
        self._closed = False
        self._stopped = False

    # ---------- connection helpers ----------

    def _new_input(self):
        if INPUT_EXCHANGE:
            return MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST,
                INPUT_EXCHANGE,
                [f"{INPUT_ROUTING_PREFIX}_{ID}"],
                queue_name=f"{INPUT_ROUTING_PREFIX}_{ID}",
                exclusive=False,
            )
        return MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, os.environ.get("INPUT_QUEUE", "q4_filter_input")
        )

    def _new_edge_store_output(self):
        return MessageMiddlewareExchangeRabbitMQ(MOM_HOST, Q4_SUM_EXCHANGE, [])

    def _new_control_senders(self) -> dict:
        return {
            self._coordinator.control_queue_for(i): MessageMiddlewareQueueRabbitMQ(
                MOM_HOST, self._coordinator.control_queue_for(i)
            )
            for i in range(Q4_FILTER_AMOUNT)
        }

    # ---------- packet helpers ----------

    def _packet(self, msg_type: MessageType, client_id: int, payload: bytes) -> bytes:
        return self._proto.create_packet(
            msg_type=msg_type,
            client_id_bytes=client_id.to_bytes(16, byteorder="big"),
            payload=payload,
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

    # ---------- edge emit helpers ----------

    def _emit_counted_edge(self, client_id: int, edge: Q4CountedEdge, output=None) -> None:
        output = output or self._edge_store_output
        partition = self._edge_store_partition(edge.intermediate)
        payload = self._counted_edge_serializer.serialize(edge)
        batch_payload = self._batcher.append((client_id, partition), payload)
        counts = self._forwarded_by_partition_by_client.setdefault(client_id, {})
        counts[partition] = counts.get(partition, 0) + 1
        if batch_payload is not None:
            self._send_batch(client_id, partition, batch_payload, output)

    def _emit_qualified_edge(self, client_id: int, edge: Q4TransactionEdge, output=None) -> int:
        self._emit_counted_edge(
            client_id,
            Q4CountedEdge(role=Q4_EDGE_INCOMING, intermediate=edge.target, endpoint=edge.source, count=1),
            output,
        )
        self._emit_counted_edge(
            client_id,
            Q4CountedEdge(role=Q4_EDGE_OUTGOING, intermediate=edge.source, endpoint=edge.target, count=1),
            output,
        )
        return 2

    def _send_batch(self, client_id: int, partition: int, batch_payload: bytes, output=None) -> None:
        output = output or self._edge_store_output
        output.send(
            self._packet(MessageType.DATA, client_id, batch_payload),
            routing_key=self._routing_key(partition),
        )

    def _flush_client_buffers(self, client_id: int, output=None) -> None:
        for (_, partition), batch_payload in self._batcher.flush(lambda k: k[0] == client_id):
            self._send_batch(client_id, partition, batch_payload, output)

    def _forward_eof_to_edge_store(
        self, client_id: int, counts_by_partition: dict[int, int], output=None
    ) -> None:
        output = output or self._edge_store_output
        for partition in range(Q4_SUM_AMOUNT):
            expected_total = int(counts_by_partition.get(partition, 0))
            payload = self._control_serializer.serialize(
                ControlMessage(sender_id=ID, expected_total=expected_total, processed_count=0)
            )
            output.send(
                self._packet(MessageType.EOF, client_id, payload),
                routing_key=self._routing_key(partition),
            )
            logging.info(
                "q4_filter_forward_eof | id=%s | client_id=%s | "
                "edge_store_partition=%s | expected_total=%s",
                ID, client_id, partition, expected_total,
            )

    def _do_broadcast(self, action: BroadcastAction, control_senders: dict) -> None:
        if action.sleep_before > 0:
            time.sleep(action.sleep_before)
        for qname in action.queue_names:
            control_senders[qname].send(action.message)

    # ---------- source gate ----------

    def _accept_edge_locked(self, client_id: int, edge: Q4TransactionEdge, output=None) -> int:
        states = self._states_by_client.setdefault(client_id, {})
        state = states.setdefault(edge.source, _SourceState())
        if state.qualified:
            return self._emit_qualified_edge(client_id, edge, output)
        state.pending.append(edge)
        if len(state.targets) < 6:
            state.targets.add(edge.target)
        if len(state.targets) < 6:
            return 0
        state.qualified = True
        state.targets.clear()
        forwarded = 0
        for e in state.pending:
            forwarded += self._emit_qualified_edge(client_id, e, output)
        state.pending = []
        return forwarded

    # ---------- data path ----------

    def _handle_data_packet(self, client_id: int, payload: bytes) -> None:
        transactions = self._tx_serializer.deserialize_batch(payload)
        if not transactions:
            return

        edges = [self._edge_from_transaction(tx) for tx in transactions]
        with self._lock:
            if client_id in self._closed_by_client:
                logging.info(
                    "q4_filter_message_for_closed_client | id=%s | client_id=%s", ID, client_id
                )
                return
            for edge in edges:
                self._accept_edge_locked(client_id, edge)
            self._processed_by_client[client_id] = (
                self._processed_by_client.get(client_id, 0) + len(edges)
            )
            processed_total = self._processed_by_client[client_id]
            forwarded_total = sum(self._forwarded_by_partition_by_client.get(client_id, {}).values())

        if should_log_progress(processed_total):
            logging.info(
                "q4_filter_data_batch | id=%s | client_id=%s | "
                "batch_size=%s | processed_total=%s | forwarded_total=%s",
                ID, client_id, len(edges), processed_total, forwarded_total,
            )

    def _handle_upstream_eof(self, client_id: int, payload: bytes, response_queue) -> None:
        ctrl = self._control_serializer.deserialize(payload)
        with self._lock:
            if client_id in self._closed_by_client:
                return
            count = self._processed_by_client.get(client_id, 0)
            forwarded = sum(self._forwarded_by_partition_by_client.get(client_id, {}).values())
            action = self._coordinator.on_upstream_eof(
                client_id, ctrl.expected_total, count, forwarded
            )
        if action is None:
            return
        logging.info(
            "q4_filter_upstream_eof | id=%s | client_id=%s | "
            "expected_total=%s | processed_snapshot=%s | forwarded_snapshot=%s",
            ID, client_id, ctrl.expected_total, count, forwarded,
        )
        if isinstance(action, SendAnswerAction):
            response_queue.send(action.message)
        elif isinstance(action, FlushAction):
            # N=1 shortcut: flush directly from data thread
            with self._lock:
                if client_id in self._closed_by_client:
                    return
                counts_by_partition = dict(
                    self._forwarded_by_partition_by_client.get(client_id, {})
                )
                self._states_by_client.pop(client_id, None)
                self._processed_by_client.pop(client_id, None)
                self._forwarded_by_partition_by_client.pop(client_id, None)
                self._closed_by_client.add(client_id)
            self._flush_client_buffers(client_id)
            self._forward_eof_to_edge_store(client_id, counts_by_partition)

    def _on_message(self, raw, ack, nack, response_queue) -> None:
        try:
            msg_type, client_id, payload = self._proto.unpack_packet(raw)
            if msg_type == MessageType.DATA:
                self._handle_data_packet(client_id, payload)
            elif msg_type == MessageType.EOF:
                self._handle_upstream_eof(client_id, payload, response_queue)
            else:
                raise ValueError(f"unexpected q4 source prefilter message: {msg_type}")
            ack()
        except Exception:
            logging.exception("q4_filter_error | id=%s", ID)
            nack()

    # ---------- control path ----------

    def _handle_control(self, message, ack, nack, response_sender, output) -> None:
        try:
            msg_type, client_id, ctrl = self._coordinator.parse_message(message)
        except Exception:
            logging.exception("q4_filter_control_parse_error | id=%s", ID)
            nack()
            return

        counts_by_partition = None
        with self._lock:
            count = self._processed_by_client.get(client_id, 0)
            forwarded = sum(self._forwarded_by_partition_by_client.get(client_id, {}).values())
            action = self._coordinator.process_control_message(
                msg_type, client_id, ctrl, count, forwarded
            )
            if isinstance(action, FlushAction):
                if client_id in self._closed_by_client:
                    ack()
                    return
                counts_by_partition = dict(
                    self._forwarded_by_partition_by_client.get(client_id, {})
                )
                self._states_by_client.pop(client_id, None)
                self._processed_by_client.pop(client_id, None)
                self._forwarded_by_partition_by_client.pop(client_id, None)
                self._closed_by_client.add(client_id)
                self._coordinator.cleanup_client(client_id)

        if action is None:
            ack()
            return
        if isinstance(action, SendAnswerAction):
            response_sender.send(action.message)
            ack()
        elif isinstance(action, FlushAction):
            self._flush_client_buffers(client_id, output)
            self._forward_eof_to_edge_store(client_id, counts_by_partition, output)
            response_sender.send(self._coordinator.build_flush_ack(client_id, 0))
            ack()
        else:
            logging.warning(
                "q4_filter_unexpected_control_action | id=%s | action=%s", ID, action
            )
            ack()

    def _start_control_consumer(self) -> None:
        # Assign before the _stopped check so handle_sigterm can always reach this consumer.
        control_consumer = MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, self._coordinator.my_control_queue()
        )
        self.control_consumer = control_consumer
        response_sender = MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, self._coordinator.response_queue_for(LEADER_ID)
        )
        output = self._new_edge_store_output()
        try:
            if not self._stopped:
                control_consumer.start_consuming(
                    lambda msg, ack, nack: self._handle_control(
                        msg, ack, nack, response_sender, output
                    )
                )
        finally:
            response_sender.close()
            output.close()
            control_consumer.close()

    # ---------- response path (leader only) ----------

    def _handle_response(self, message, ack, nack, control_senders, output) -> None:
        try:
            msg_type, client_id, ctrl = self._coordinator.parse_message(message)
        except Exception:
            logging.exception("q4_filter_response_parse_error | id=%s", ID)
            nack()
            return

        counts_by_partition = None
        with self._lock:
            action = self._coordinator.process_control_message(msg_type, client_id, ctrl)
            if isinstance(action, FlushAction) and action.is_leader:
                if client_id in self._closed_by_client:
                    ack()
                    return
                counts_by_partition = dict(
                    self._forwarded_by_partition_by_client.get(client_id, {})
                )
                self._states_by_client.pop(client_id, None)
                self._processed_by_client.pop(client_id, None)
                self._forwarded_by_partition_by_client.pop(client_id, None)
                self._closed_by_client.add(client_id)

        if action is None:
            ack()
            return
        if isinstance(action, BroadcastAction):
            self._do_broadcast(action, control_senders)
            ack()
        elif isinstance(action, FlushAction) and action.is_leader:
            self._flush_client_buffers(client_id, output)
            self._forward_eof_to_edge_store(client_id, counts_by_partition, output)
            ack()
        else:
            logging.warning(
                "q4_filter_unexpected_response_action | id=%s | action=%s", ID, action
            )
            ack()

    def _start_response_consumer(self) -> None:
        # Assign before the _stopped check so handle_sigterm can always reach this consumer.
        response_consumer = MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, self._coordinator.my_response_queue()
        )
        self.response_consumer = response_consumer
        control_senders = self._new_control_senders()
        output = self._new_edge_store_output()
        try:
            if not self._stopped:
                response_consumer.start_consuming(
                    lambda msg, ack, nack: self._handle_response(
                        msg, ack, nack, control_senders, output
                    )
                )
        finally:
            for q in control_senders.values():
                q.close()
            output.close()
            response_consumer.close()

    # ---------- lifecycle ----------

    def start(self) -> None:
        logging.info(
            "q4_filter_start | id=%s | amount=%s | input_exchange=%s | "
            "edge_store_exchange=%s | sum_amount=%s | leader=%s",
            ID, Q4_FILTER_AMOUNT, INPUT_EXCHANGE, Q4_SUM_EXCHANGE, Q4_SUM_AMOUNT,
            ID == LEADER_ID,
        )
        self._control_thread = threading.Thread(target=self._start_control_consumer)
        self._control_thread.start()
        if self._coordinator.needs_response_consumer():
            self._response_thread = threading.Thread(target=self._start_response_consumer)
            self._response_thread.start()

        data_response = MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, self._coordinator.response_queue_for(LEADER_ID)
        )
        try:
            if not self._stopped:
                self._input.start_consuming(
                    lambda msg, ack, nack: self._on_message(msg, ack, nack, data_response)
                )
        finally:
            data_response.close()
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
        if self.control_consumer is not None:
            self.control_consumer.request_stop_consuming()
        if self.response_consumer is not None:
            self.response_consumer.request_stop_consuming()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        for resource in (self._input, self._edge_store_output):
            if resource is None:
                continue
            try:
                resource.close()
            except Exception as e:
                logging.warning("q4_filter_close_error | id=%s | error=%s", ID, e)
