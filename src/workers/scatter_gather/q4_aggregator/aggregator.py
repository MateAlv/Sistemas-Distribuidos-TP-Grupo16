import logging
import os
import threading

from common.fault_tolerance.handler import Action, EdgeSpec, PersistentStateHandler
from common.fault_tolerance.inbox import InboxStatus, MsgKind
from common.logging_utils import should_log_progress
from common.message_protocol.internal import (
    InternalProtocol,
    Q4AccountId,
    Q4AccountIdSerializer,
    Q4PairPathsSerializer,
    partition_for_parts,
)
from common.message_protocol.internal.common import ControlMessage, MessageType
from common.message_protocol.internal.control_message_serializer import (
    ControlMessageSerializer,
)
from common.middleware import ShardedPublisher
from common.middleware.middleware_rabbitmq import (
    ensure_exchange_queue_bindings,
    MessageMiddlewareExchangeRabbitMQ,
)
from common.routing import queue_name_for_worker

try:
    from q4_aggregator_state import Q4AggregatorState
except ImportError:
    from workers.scatter_gather.q4_aggregator.q4_aggregator_state import (
        Q4AggregatorState,
    )


ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
Q4_AGGREGATOR_EXCHANGE = os.environ["Q4_AGGREGATOR_EXCHANGE"]
Q4_AGGREGATOR_ROUTING_PREFIX = os.environ.get(
    "Q4_AGGREGATOR_ROUTING_PREFIX", "q4_aggregator"
)
Q4_JOINER_AMOUNT = int(os.environ["Q4_JOINER_AMOUNT"])
Q4_DEDUPER_EXCHANGE = os.environ["Q4_DEDUPER_EXCHANGE"]
Q4_DEDUPER_AMOUNT = int(os.environ["Q4_DEDUPER_AMOUNT"])
Q4_DEDUPER_ROUTING_PREFIX = os.environ.get(
    "Q4_DEDUPER_ROUTING_PREFIX", "q4_deduper"
)
Q4_AGGREGATOR_BATCH_MAX_ACCOUNTS = int(
    os.environ.get("Q4_AGGREGATOR_BATCH_MAX_ACCOUNTS", "5000")
)
STATE_DIR = os.environ.get("STATE_DIR", "")
SNAPSHOT_INTERVAL = int(os.environ.get("SNAPSHOT_INTERVAL", "1000"))
Q4_DEDUPER_EDGE = "q4_deduper"


class Q4AggregatorWorker:
    """Sums weighted path contributions per (A, B) and emits account candidates."""

    def __init__(self):
        self._proto = InternalProtocol()
        self._pair_paths_serializer = Q4PairPathsSerializer()
        self._account_serializer = Q4AccountIdSerializer()
        self._control_serializer = ControlMessageSerializer()

        self._lock = threading.Lock()
        self._state = Q4AggregatorState(Q4_JOINER_AMOUNT, Q4_DEDUPER_AMOUNT)
        self._handler = PersistentStateHandler(
            state_dir=STATE_DIR,
            node_id=f"q4_aggregator_{ID}",
            worker_state=self._state,
            snapshot_every=SNAPSHOT_INTERVAL,
            output_edges={
                Q4_DEDUPER_EDGE: EdgeSpec(
                    sender_id=ID,
                    shard_count=Q4_DEDUPER_AMOUNT,
                )
            },
        )

        self._input = MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST,
            Q4_AGGREGATOR_EXCHANGE,
            [self._input_routing_key()],
            queue_name=self._input_routing_key(),
            exclusive=False,
        )
        self._account_deduper_output = self._new_account_deduper_output()
        self._publishers = {Q4_DEDUPER_EDGE: self._account_deduper_output}

        self._closed = False
        self._stopped = False

        self._handler.recover()
        self._republish_pending()

    def _input_routing_key(self) -> str:
        return queue_name_for_worker(Q4_AGGREGATOR_ROUTING_PREFIX, ID)

    def _new_account_deduper_output(self) -> ShardedPublisher:
        return ShardedPublisher(
            MOM_HOST,
            Q4_DEDUPER_EXCHANGE,
            Q4_DEDUPER_ROUTING_PREFIX,
            Q4_DEDUPER_AMOUNT,
        )

    def _ensure_output_bindings(self) -> None:
        ensure_exchange_queue_bindings(
            MOM_HOST,
            Q4_DEDUPER_EXCHANGE,
            {
                queue_name_for_worker(Q4_DEDUPER_ROUTING_PREFIX, index): (
                    queue_name_for_worker(Q4_DEDUPER_ROUTING_PREFIX, index)
                )
                for index in range(Q4_DEDUPER_AMOUNT)
            },
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

    def _account_partition(self, account: Q4AccountId) -> int:
        return partition_for_parts(
            (account.bank_id, account.account),
            Q4_DEDUPER_AMOUNT,
        )

    def _build_data_outputs(self, client_id: int, payload: bytes) -> list:
        outputs = []
        by_partition = self._state.predict_data_outputs(client_id, payload)
        for partition in sorted(by_partition):
            for chunk in _chunks(
                by_partition[partition],
                Q4_AGGREGATOR_BATCH_MAX_ACCOUNTS,
            ):
                batch_payload = self._account_serializer.serialize_batch(chunk)
                outputs.append(
                    (
                        Q4_DEDUPER_EDGE,
                        self._packet(MessageType.DATA, client_id, batch_payload),
                        partition,
                    )
                )
        return outputs

    def _build_eof_outputs(self, client_id: int) -> list:
        outputs = []
        counts = self._state.forwarded_by_partition(client_id)
        for partition in range(Q4_DEDUPER_AMOUNT):
            expected_total = int(counts.get(partition, 0))
            outputs.append(
                (
                    Q4_DEDUPER_EDGE,
                    self._packet(
                        MessageType.EOF,
                        client_id,
                        self._control_payload(ID, expected_total, 0),
                    ),
                    partition,
                )
            )
            logging.info(
                "q4_aggregator_forward_eof | id=%s | client_id=%s | "
                "account_deduper_partition=%s | expected_total=%s",
                ID,
                client_id,
                partition,
                expected_total,
            )
        return outputs

    def _publish(self, entries) -> None:
        for entry in entries:
            publisher = self._publishers.get(entry.destination)
            if publisher is None:
                raise KeyError(f"no publisher for destination {entry.destination!r}")
            if entry.shard is None:
                publisher.send(entry.body)
            else:
                publisher.send_to_shard(entry.body, entry.shard)

    def _publish_then_commit(self, instruction) -> None:
        if instruction.action is Action.PUBLISH_THEN_COMMIT:
            self._publish(instruction.outputs)
            with self._lock:
                self._handler.commit_done(*instruction.ctx)

    def _republish_pending(self) -> None:
        for entry in self._handler.outbox_to_republish():
            try:
                self._publish([entry])
            except Exception:
                logging.exception(
                    "q4_aggregator_republish_error | id=%s | destination=%s",
                    ID,
                    entry.destination,
                )

    def _handle_abort(self, msg_id, client_id, sender_id, seq, ack) -> None:
        with self._lock:
            if self._state.is_closed(client_id):
                ack()
                return
            instruction = self._handler.handle(
                msg_id, client_id, sender_id, seq, b"",
                lambda _: (
                    Q4AggregatorState.abort_change(client_id),
                    [
                        (Q4_DEDUPER_EDGE, self._packet(MessageType.ABORT, client_id, b""), partition)
                        for partition in range(Q4_DEDUPER_AMOUNT)
                    ],
                ),
                kind=MsgKind.ABORT,
            )
        self._publish_then_commit(instruction)
        ack()
        logging.info("q4_aggregator_abort | id=%s | client_id=%s", ID, client_id)

    def _handle_data(self, msg_id, client_id, sender_id, seq, payload, ack) -> None:
        def bfn(data):
            return (
                Q4AggregatorState.data_change(client_id, data),
                self._build_data_outputs(client_id, data),
            )

        with self._lock:
            status = self._handler.inbox.classify(client_id, sender_id, seq)
            if self._state.is_closed(client_id) and status is not InboxStatus.APPLIED:
                ack()
                return
            instruction = self._handler.handle(
                msg_id, client_id, sender_id, seq, payload, bfn
            )
        self._publish_then_commit(instruction)
        ack()

        processed = self._state.processed_count(client_id)
        if should_log_progress(processed):
            logging.info(
                "q4_aggregator_data_batch | id=%s | client_id=%s | "
                "processed_total=%s | forwarded_total=%s",
                ID,
                client_id,
                processed,
                self._state.forwarded_total(client_id),
            )

    def _handle_eof(self, msg_id, client_id, sender_id, seq, payload, ack) -> None:
        control = self._control_serializer.deserialize(payload)
        upstream_id = control.sender_id

        with self._lock:
            status = self._handler.inbox.classify(client_id, sender_id, seq)
            if self._state.is_closed(client_id) and status is not InboxStatus.APPLIED:
                ack()
                return
            completes = self._state.eof_would_complete(client_id, upstream_id)
            eof_count = self._state.eof_count(client_id)
            processed_total = self._state.processed_count(client_id)

            def bfn(_data):
                eof_change = Q4AggregatorState.eof_change(client_id, upstream_id)
                if completes:
                    return (
                        Q4AggregatorState.compound_change(
                            eof_change, Q4AggregatorState.close_change(client_id)
                        ),
                        self._build_eof_outputs(client_id),
                    )
                return eof_change, []

            instruction = self._handler.handle(
                msg_id, client_id, sender_id, seq, payload, bfn
            )

        self._publish_then_commit(instruction)
        ack()
        logging.info(
            "q4_aggregator_eof_received | id=%s | client_id=%s | "
            "block_joiner_id=%s | eof_count=%s | expected_eofs=%s | "
            "sender_expected_total=%s | processed_total=%s | flushed=%s",
            ID,
            client_id,
            upstream_id,
            eof_count + 1,
            Q4_JOINER_AMOUNT,
            control.expected_total,
            processed_total,
            completes,
        )

    def _on_message(self, raw, ack, nack):
        try:
            msg_type, client_id, sender_id, seq, payload = (
                self._proto.unpack_addressed_packet(raw)
            )
            msg_id = f"{sender_id}:{seq}"
            if msg_type == MessageType.DATA:
                self._handle_data(msg_id, client_id, sender_id, seq, payload, ack)
            elif msg_type == MessageType.EOF:
                self._handle_eof(msg_id, client_id, sender_id, seq, payload, ack)
            elif msg_type == MessageType.ABORT:
                self._handle_abort(f"abort:{sender_id}:{client_id}:{seq}", client_id, sender_id, seq, ack)
            else:
                raise ValueError(f"unexpected q4 pair reducer message type: {msg_type}")
        except Exception:
            logging.exception("q4_aggregator_error | id=%s", ID)
            nack(requeue=True)

    def start(self) -> None:
        self._ensure_output_bindings()
        self._republish_pending()
        logging.info(
            "q4_aggregator_start | id=%s | input_exchange=%s | input_key=%s | "
            "joiner_amount=%s | account_deduper_exchange=%s | deduper_amount=%s",
            ID,
            Q4_AGGREGATOR_EXCHANGE,
            self._input_routing_key(),
            Q4_JOINER_AMOUNT,
            Q4_DEDUPER_EXCHANGE,
            Q4_DEDUPER_AMOUNT,
        )
        try:
            if not self._stopped:
                self._input.start_consuming(self._on_message)
        finally:
            self.handle_sigterm()
            self.close()

    def handle_sigterm(self) -> None:
        if self._stopped:
            return
        self._stopped = True
        logging.info("q4_aggregator_shutdown | id=%s", ID)
        self._input.request_stop_consuming()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        for resource in (self._input, self._account_deduper_output):
            try:
                resource.close()
            except Exception as e:
                logging.warning(
                    "q4_aggregator_close_error | id=%s | error=%s",
                    ID,
                    e,
                )


def _chunks(items: list, size: int):
    for i in range(0, len(items), size):
        yield items[i : i + size]
