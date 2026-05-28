import logging
import os
import threading
from collections import defaultdict

from common.batch_buffer import BatchBuffer
from common.logging_utils import should_log_progress
from common.message_protocol.internal import (
    InternalProtocol,
    Q4AccountId,
    Q4AccountIdSerializer,
    Q4PairDeltaSerializer,
    Q4_QUALIFY_THRESHOLD,
    partition_for_parts,
)
from common.message_protocol.internal.common import ControlMessage, MessageType
from common.message_protocol.internal.control_message_serializer import (
    ControlMessageSerializer,
)
from common.middleware.middleware_rabbitmq import MessageMiddlewareExchangeRabbitMQ


ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
Q4_PAIR_REDUCER_EXCHANGE = os.environ["Q4_PAIR_REDUCER_EXCHANGE"]
Q4_PAIR_REDUCER_ROUTING_PREFIX = os.environ.get(
    "Q4_PAIR_REDUCER_ROUTING_PREFIX", "q4_pair_reducer"
)
Q4_BLOCK_JOINER_AMOUNT = int(os.environ["Q4_BLOCK_JOINER_AMOUNT"])
Q4_ACCOUNT_DEDUPER_EXCHANGE = os.environ["Q4_ACCOUNT_DEDUPER_EXCHANGE"]
Q4_ACCOUNT_DEDUPER_AMOUNT = int(os.environ["Q4_ACCOUNT_DEDUPER_AMOUNT"])
Q4_ACCOUNT_DEDUPER_ROUTING_PREFIX = os.environ.get(
    "Q4_ACCOUNT_DEDUPER_ROUTING_PREFIX", "q4_account_deduper"
)
Q4_PAIR_REDUCER_BATCH_BYTES = int(
    os.environ.get("Q4_PAIR_REDUCER_BATCH_BYTES", str(1024 * 1024))
)
Q4_PAIR_REDUCER_BATCH_MAX_ACCOUNTS = int(
    os.environ.get("Q4_PAIR_REDUCER_BATCH_MAX_ACCOUNTS", "5000")
)

class Q4PairReducerWorker:
    """Sums weighted path contributions per (A, B) and emits account candidates."""

    def __init__(self):
        self._input = MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST,
            Q4_PAIR_REDUCER_EXCHANGE,
            [self._input_routing_key()],
            queue_name=self._input_routing_key(),
            exclusive=False,
        )
        self._account_deduper_output = self._new_account_deduper_output()

        self._proto = InternalProtocol()
        self._pair_delta_serializer = Q4PairDeltaSerializer()
        self._account_serializer = Q4AccountIdSerializer()
        self._control_serializer = ControlMessageSerializer()
        self._batcher = BatchBuffer(
            Q4_PAIR_REDUCER_BATCH_BYTES,
            Q4_PAIR_REDUCER_BATCH_MAX_ACCOUNTS,
        )

        self._lock = threading.Lock()
        self._pair_counts_by_client: dict[int, dict[tuple[str, str, str, str], int]] = {}
        self._qualified_pairs_by_client: dict[int, set[tuple[str, str, str, str]]] = {}
        self._processed_by_client: dict[int, int] = {}
        self._eofs_by_client: dict[int, set[int]] = defaultdict(set)
        self._forwarded_by_partition_by_client: dict[int, dict[int, int]] = {}
        self._closed_by_client: set[int] = set()

        self._closed = False
        self._stopped = False

    def _input_routing_key(self) -> str:
        return f"{Q4_PAIR_REDUCER_ROUTING_PREFIX}_{ID}"

    def _new_account_deduper_output(self):
        return MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST,
            Q4_ACCOUNT_DEDUPER_EXCHANGE,
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

    def _pair_key(self, source: Q4AccountId, target: Q4AccountId):
        return (source.bank_id, source.account, target.bank_id, target.account)

    def _source_from_key(self, key) -> Q4AccountId:
        return Q4AccountId(bank_id=key[0], account=key[1])

    def _target_from_key(self, key) -> Q4AccountId:
        return Q4AccountId(bank_id=key[2], account=key[3])

    def _account_partition(self, account: Q4AccountId) -> int:
        return partition_for_parts(
            (account.bank_id, account.account),
            Q4_ACCOUNT_DEDUPER_AMOUNT,
        )

    def _account_routing_key(self, partition: int) -> str:
        return f"{Q4_ACCOUNT_DEDUPER_ROUTING_PREFIX}_{partition}"

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

    def _accept_pair_deltas(self, client_id: int, payload: bytes) -> int:
        deltas = self._pair_delta_serializer.deserialize_batch(payload)
        if not deltas:
            return 0

        emitted_pairs = 0
        with self._lock:
            if client_id in self._closed_by_client:
                logging.info(
                    "q4_pair_reducer_message_for_closed_client | id=%s | client_id=%s",
                    ID,
                    client_id,
                )
                return 0

            counts = self._pair_counts_by_client.setdefault(client_id, {})
            qualified = self._qualified_pairs_by_client.setdefault(client_id, set())

            for delta in deltas:
                if delta.source == delta.target:
                    continue
                key = self._pair_key(delta.source, delta.target)
                if key in qualified:
                    continue
                total = min(
                    Q4_QUALIFY_THRESHOLD,
                    counts.get(key, 0) + int(delta.weight),
                )
                if total >= Q4_QUALIFY_THRESHOLD:
                    counts.pop(key, None)
                    emitted_pairs += int(
                        self._emit_qualified_pair(client_id, key)
                    )
                else:
                    counts[key] = total

            self._processed_by_client[client_id] = (
                self._processed_by_client.get(client_id, 0) + len(deltas)
            )
            processed_total = self._processed_by_client[client_id]

        if should_log_progress(processed_total):
            logging.info(
                "q4_pair_reducer_data_batch | id=%s | client_id=%s | "
                "batch_size=%s | processed_total=%s | emitted_pairs=%s",
                ID,
                client_id,
                len(deltas),
                processed_total,
                emitted_pairs,
            )
        return len(deltas)

    def _handle_eof(self, client_id: int, payload: bytes) -> None:
        control = self._control_serializer.deserialize(payload)
        should_emit = False

        with self._lock:
            if client_id in self._closed_by_client:
                return
            if control.sender_id in self._eofs_by_client[client_id]:
                logging.info(
                    "q4_pair_reducer_duplicate_eof | id=%s | client_id=%s | "
                    "block_joiner_id=%s",
                    ID,
                    client_id,
                    control.sender_id,
                )
                return

            self._eofs_by_client[client_id].add(control.sender_id)
            eof_count = len(self._eofs_by_client[client_id])
            processed_total = self._processed_by_client.get(client_id, 0)
            if eof_count >= Q4_BLOCK_JOINER_AMOUNT:
                should_emit = True

        logging.info(
            "q4_pair_reducer_eof_received | id=%s | client_id=%s | "
            "block_joiner_id=%s | eof_count=%s | expected_eofs=%s | "
            "sender_expected_total=%s | processed_total=%s",
            ID,
            client_id,
            control.sender_id,
            eof_count,
            Q4_BLOCK_JOINER_AMOUNT,
            control.expected_total,
            processed_total,
        )

        if should_emit:
            self._emit_and_close_client(client_id)

    def _forward_eof_to_account_dedupers(
        self,
        client_id: int,
        counts_by_partition: dict[int, int],
        output=None,
    ) -> None:
        output = output or self._account_deduper_output
        for partition in range(Q4_ACCOUNT_DEDUPER_AMOUNT):
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
                "q4_pair_reducer_forward_eof | id=%s | client_id=%s | "
                "account_deduper_partition=%s | expected_total=%s",
                ID,
                client_id,
                partition,
                expected_total,
            )

    def _emit_and_close_client(self, client_id: int, output=None) -> None:
        with self._lock:
            if client_id in self._closed_by_client:
                return
            pending_pairs = len(self._pair_counts_by_client.get(client_id, {}))
            logging.info(
                "q4_pair_reducer_close_client | id=%s | client_id=%s | "
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
            self._eofs_by_client.pop(client_id, None)
            self._forwarded_by_partition_by_client.pop(client_id, None)
            self._closed_by_client.add(client_id)

        self._forward_eof_to_account_dedupers(client_id, counts_by_partition, output)

    def _on_message(self, raw, ack, nack):
        try:
            msg_type, client_id, payload = self._proto.unpack_packet(raw)
            if msg_type == MessageType.DATA:
                self._accept_pair_deltas(client_id, payload)
            elif msg_type == MessageType.EOF:
                self._handle_eof(client_id, payload)
            else:
                raise ValueError(f"unexpected q4 pair reducer message type: {msg_type}")
            ack()
        except Exception:
            logging.exception("q4_pair_reducer_error | id=%s", ID)
            nack()

    def start(self) -> None:
        logging.info(
            "q4_pair_reducer_start | id=%s | input_exchange=%s | input_key=%s | "
            "block_joiner_amount=%s | account_deduper_exchange=%s | "
            "account_deduper_amount=%s",
            ID,
            Q4_PAIR_REDUCER_EXCHANGE,
            self._input_routing_key(),
            Q4_BLOCK_JOINER_AMOUNT,
            Q4_ACCOUNT_DEDUPER_EXCHANGE,
            Q4_ACCOUNT_DEDUPER_AMOUNT,
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
        logging.info("q4_pair_reducer_shutdown | id=%s", ID)
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
                    "q4_pair_reducer_close_error | id=%s | error=%s",
                    ID,
                    e,
                )
