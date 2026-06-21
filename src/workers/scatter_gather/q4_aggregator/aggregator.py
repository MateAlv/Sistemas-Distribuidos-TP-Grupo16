import logging
import os
import threading
from common.upstream_eof_counter import UpstreamEofCounter

from common.batch_buffer import BatchBuffer
from common.logging_utils import should_log_progress
from common.message_protocol.internal import (
    InternalProtocol,
    Q4AccountId,
    Q4AccountIdSerializer,
    Q4PairPathsSerializer,
    Q4_QUALIFY_THRESHOLD,
    partition_for_parts,
)
from common.message_protocol.internal.common import ControlMessage, MessageType
from common.message_protocol.internal.control_message_serializer import (
    ControlMessageSerializer,
)
from common.middleware.middleware_rabbitmq import (
    ensure_exchange_queue_bindings,
    MessageMiddlewareExchangeRabbitMQ,
)
from common.routing import queue_name_for_worker


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
Q4_AGGREGATOR_BATCH_BYTES = int(
    os.environ.get("Q4_AGGREGATOR_BATCH_BYTES", str(1024 * 1024))
)
Q4_AGGREGATOR_BATCH_MAX_ACCOUNTS = int(
    os.environ.get("Q4_AGGREGATOR_BATCH_MAX_ACCOUNTS", "5000")
)

class Q4AggregatorWorker:
    """Sums weighted path contributions per (A, B) and emits account candidates."""

    def __init__(self):
        self._input = MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST,
            Q4_AGGREGATOR_EXCHANGE,
            [self._input_routing_key()],
            queue_name=self._input_routing_key(),
            exclusive=False,
        )
        self._account_deduper_output = self._new_account_deduper_output()

        self._proto = InternalProtocol()
        self._pair_paths_serializer = Q4PairPathsSerializer()
        self._account_serializer = Q4AccountIdSerializer()
        self._control_serializer = ControlMessageSerializer()
        self._batcher = BatchBuffer(
            Q4_AGGREGATOR_BATCH_BYTES,
            Q4_AGGREGATOR_BATCH_MAX_ACCOUNTS,
        )

        self._lock = threading.Lock()
        self._pair_counts_by_client: dict[int, dict[tuple[str, str, str, str], int]] = {}
        self._qualified_pairs_by_client: dict[int, set[tuple[str, str, str, str]]] = {}
        self._processed_by_client: dict[int, int] = {}
        self._eof_counter = UpstreamEofCounter(Q4_JOINER_AMOUNT)
        self._forwarded_by_partition_by_client: dict[int, dict[int, int]] = {}
        self._closed_by_client: set[int] = set()

        self._closed = False
        self._stopped = False

    def _input_routing_key(self) -> str:
        return queue_name_for_worker(Q4_AGGREGATOR_ROUTING_PREFIX, ID)

    def _new_account_deduper_output(self):
        return MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST,
            Q4_DEDUPER_EXCHANGE,
            [],
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

    def _pair_key(self, source: Q4AccountId, target: Q4AccountId):
        return (source.bank_id, source.account, target.bank_id, target.account)

    def _source_from_key(self, key) -> Q4AccountId:
        return Q4AccountId(bank_id=key[0], account=key[1])

    def _target_from_key(self, key) -> Q4AccountId:
        return Q4AccountId(bank_id=key[2], account=key[3])

    def _account_partition(self, account: Q4AccountId) -> int:
        return partition_for_parts(
            (account.bank_id, account.account),
            Q4_DEDUPER_AMOUNT,
        )

    def _account_routing_key(self, partition: int) -> str:
        return f"{Q4_DEDUPER_ROUTING_PREFIX}_{partition}"

    def _send_batch(
        self,
        client_id: int,
        partition: int,
        batch_payload: bytes,
        output=None,
    ) -> None:
        output = output or self._account_deduper_output
        output.send(
            self._packet(MessageType.DATA, client_id, batch_payload),
            routing_key=self._account_routing_key(partition),
        )

    def _append_account_candidate(
        self,
        client_id: int,
        account: Q4AccountId,
        output=None,
    ) -> None:
        partition = self._account_partition(account)
        payload = self._account_serializer.serialize(account)
        batch_payload = self._batcher.append((client_id, partition), payload)
        counts = self._forwarded_by_partition_by_client.setdefault(client_id, {})
        counts[partition] = counts.get(partition, 0) + 1
        if batch_payload is not None:
            self._send_batch(client_id, partition, batch_payload, output)

    def _emit_qualified_pair(self, client_id: int, key, output=None) -> bool:
        """A pair (A, B) just crossed the threshold. Mark it qualified (so future
        contributions are ignored) and emit both accounts A and B as account
        candidates to the deduper. Returns False if the pair was already qualified."""
        qualified = self._qualified_pairs_by_client.setdefault(client_id, set())
        if key in qualified:
            return False
        qualified.add(key)
        self._append_account_candidate(client_id, self._source_from_key(key), output)
        self._append_account_candidate(client_id, self._target_from_key(key), output)
        return True

    def _flush_client_buffers(self, client_id: int, output=None) -> None:
        for (_, partition), batch_payload in self._batcher.flush(
            lambda k: k[0] == client_id
        ):
            self._send_batch(client_id, partition, batch_payload, output)

    def _accept_pair_paths(self, client_id: int, payload: bytes) -> int:
        """Main data path. For each Q4PairPaths, add its path_count to the running
        total for (A, B). Skip A == B and already-qualified pairs. The moment a
        total crosses the threshold, emit account candidates and forget the pair."""
        batch = self._pair_paths_serializer.deserialize_batch(payload)
        if not batch:
            return 0

        emitted_pairs = 0
        with self._lock:
            if client_id in self._closed_by_client:
                logging.info(
                    "q4_aggregator_message_for_closed_client | id=%s | client_id=%s",
                    ID,
                    client_id,
                )
                return 0

            counts = self._pair_counts_by_client.setdefault(client_id, {})
            qualified = self._qualified_pairs_by_client.setdefault(client_id, set())

            for pair_paths in batch:
                if pair_paths.source == pair_paths.target:
                    continue
                key = self._pair_key(pair_paths.source, pair_paths.target)
                if key in qualified:
                    continue
                total = min(
                    Q4_QUALIFY_THRESHOLD,
                    counts.get(key, 0) + int(pair_paths.path_count),
                )
                if total >= Q4_QUALIFY_THRESHOLD:
                    counts.pop(key, None)
                    emitted_pairs += int(
                        self._emit_qualified_pair(client_id, key)
                    )
                else:
                    counts[key] = total

            self._processed_by_client[client_id] = (
                self._processed_by_client.get(client_id, 0) + len(batch)
            )
            processed_total = self._processed_by_client[client_id]

        if should_log_progress(processed_total):
            logging.info(
                "q4_aggregator_data_batch | id=%s | client_id=%s | "
                "batch_size=%s | processed_total=%s | emitted_pairs=%s",
                ID,
                client_id,
                len(batch),
                processed_total,
                emitted_pairs,
            )
        return len(batch)

    def _handle_eof(self, client_id: int, payload: bytes) -> None:
        control = self._control_serializer.deserialize(payload)

        with self._lock:
            should_emit = self._eof_counter.on_eof(client_id, control.sender_id)
            eof_count = self._eof_counter.count(client_id)
            processed_total = self._processed_by_client.get(client_id, 0)

        logging.info(
            "q4_aggregator_eof_received | id=%s | client_id=%s | "
            "block_joiner_id=%s | eof_count=%s | expected_eofs=%s | "
            "sender_expected_total=%s | processed_total=%s",
            ID, client_id, control.sender_id, eof_count, Q4_JOINER_AMOUNT,
            control.expected_total, processed_total,
        )

        if should_emit:
            self._emit_and_close_client(client_id)

    def _forward_eof_to_dedupers(
        self,
        client_id: int,
        counts_by_partition: dict[int, int],
        output=None,
    ) -> None:
        """Send one EOF to every deduper partition, each carrying how many account
        candidates we sent that deduper so it knows when we're done."""
        output = output or self._account_deduper_output
        for partition in range(Q4_DEDUPER_AMOUNT):
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
                routing_key=self._account_routing_key(partition),
            )
            logging.info(
                "q4_aggregator_forward_eof | id=%s | client_id=%s | "
                "account_deduper_partition=%s | expected_total=%s",
                ID,
                client_id,
                partition,
                expected_total,
            )

    def _emit_and_close_client(self, client_id: int, output=None) -> None:
        """End-of-client. Sub-threshold pair counters get dropped (noise), batched
        account candidates are flushed, per-client state is freed, and EOF is
        forwarded to every deduper partition."""
        with self._lock:
            if client_id in self._closed_by_client:
                return
            pending_pairs = len(self._pair_counts_by_client.get(client_id, {}))
            logging.info(
                "q4_aggregator_close_client | id=%s | client_id=%s | "
                "pending_subthreshold_pairs=%s",
                ID,
                client_id,
                pending_pairs,
            )

        self._flush_client_buffers(client_id, output)

        with self._lock:
            counts_by_partition = dict(
                self._forwarded_by_partition_by_client.get(client_id, {})
            )
            self._pair_counts_by_client.pop(client_id, None)
            self._qualified_pairs_by_client.pop(client_id, None)
            self._processed_by_client.pop(client_id, None)
            self._eof_counter.close(client_id)
            self._forwarded_by_partition_by_client.pop(client_id, None)
            self._closed_by_client.add(client_id)

        self._forward_eof_to_dedupers(client_id, counts_by_partition, output)

    def _on_message(self, raw, ack, nack):
        try:
            msg_type, client_id, payload = self._proto.unpack_packet(raw)
            if msg_type == MessageType.DATA:
                self._accept_pair_paths(client_id, payload)
            elif msg_type == MessageType.EOF:
                self._handle_eof(client_id, payload)
            else:
                raise ValueError(f"unexpected q4 pair reducer message type: {msg_type}")
            ack()
        except Exception:
            logging.exception("q4_aggregator_error | id=%s", ID)
            nack()

    def start(self) -> None:
        self._ensure_output_bindings()
        logging.info(
            "q4_aggregator_start | id=%s | input_exchange=%s | input_key=%s | "
            "joiner_amount=%s | account_deduper_exchange=%s | "
            "deduper_amount=%s",
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
