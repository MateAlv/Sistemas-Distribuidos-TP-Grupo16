import logging
import os
import threading
from collections import Counter, defaultdict

from common.batch_buffer import BatchBuffer
from common.logging_utils import should_log_progress
from common.message_protocol.internal import (
    InternalProtocol,
    Q4BlockJoinEdge,
    Q4BlockJoinEdgeSerializer,
    Q4CountedEdgeSerializer,
    Q4_EDGE_INCOMING,
    Q4_EDGE_OUTGOING,
    Q4AccountId,
    partition_for_parts,
)
from common.message_protocol.internal.common import ControlMessage, MessageType
from common.message_protocol.internal.control_message_serializer import (
    ControlMessageSerializer,
)
from common.middleware.middleware_rabbitmq import MessageMiddlewareExchangeRabbitMQ


ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
Q4_EDGE_STORE_EXCHANGE = os.environ["Q4_EDGE_STORE_EXCHANGE"]
Q4_EDGE_STORE_ROUTING_PREFIX = os.environ.get(
    "Q4_EDGE_STORE_ROUTING_PREFIX", "q4_edge_store"
)
Q4_SOURCE_PREFILTER_AMOUNT = int(os.environ.get("Q4_SOURCE_PREFILTER_AMOUNT", "1"))
Q4_BLOCK_JOINER_EXCHANGE = os.environ["Q4_BLOCK_JOINER_EXCHANGE"]
Q4_BLOCK_JOINER_AMOUNT = int(os.environ["Q4_BLOCK_JOINER_AMOUNT"])
Q4_BLOCK_JOINER_ROUTING_PREFIX = os.environ.get(
    "Q4_BLOCK_JOINER_ROUTING_PREFIX", "q4_block_joiner"
)
Q4_EDGE_STORE_BATCH_BYTES = int(
    os.environ.get("Q4_EDGE_STORE_BATCH_BYTES", str(1024 * 1024))
)
Q4_EDGE_STORE_BATCH_MAX_EDGES = int(
    os.environ.get("Q4_EDGE_STORE_BATCH_MAX_EDGES", "5000")
)
Q4_EDGE_STORE_HOT_PAIR_THRESHOLD = int(
    os.environ.get("Q4_EDGE_STORE_HOT_PAIR_THRESHOLD", "1000000")
)
Q4_EDGE_STORE_HOT_A_BUCKETS = int(os.environ.get("Q4_EDGE_STORE_HOT_A_BUCKETS", "16"))
Q4_EDGE_STORE_HOT_B_BUCKETS = int(os.environ.get("Q4_EDGE_STORE_HOT_B_BUCKETS", "16"))


class Q4EdgeStoreWorker:
    """Aggregates counted Q4 edges for the intermediary shard owned by this worker."""

    def __init__(self):
        self._input = MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST,
            Q4_EDGE_STORE_EXCHANGE,
            [self._input_routing_key()],
            queue_name=self._input_routing_key(),
            exclusive=False,
        )
        self._block_joiner_output = self._new_block_joiner_output()

        self._proto = InternalProtocol()
        self._counted_edge_serializer = Q4CountedEdgeSerializer()
        self._block_edge_serializer = Q4BlockJoinEdgeSerializer()
        self._control_serializer = ControlMessageSerializer()
        self._batcher = BatchBuffer(
            Q4_EDGE_STORE_BATCH_BYTES,
            Q4_EDGE_STORE_BATCH_MAX_EDGES,
        )

        self._lock = threading.Lock()
        self._incoming_by_client = defaultdict(lambda: defaultdict(Counter))
        self._outgoing_by_client = defaultdict(lambda: defaultdict(Counter))
        self._processed_by_client: dict[int, int] = {}
        self._eofs_by_client: dict[int, set[int]] = defaultdict(set)
        self._forwarded_by_partition_by_client: dict[int, dict[int, int]] = {}
        self._closed_by_client: set[int] = set()

        self._closed = False
        self._stopped = False

    def _input_routing_key(self) -> str:
        return f"{Q4_EDGE_STORE_ROUTING_PREFIX}_{ID}"

    def _new_block_joiner_output(self):
        return MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST,
            Q4_BLOCK_JOINER_EXCHANGE,
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

    def _block_routing_key(self, partition: int) -> str:
        return f"{Q4_BLOCK_JOINER_ROUTING_PREFIX}_{partition}"

    def _account_parts(self, account: Q4AccountId):
        return (account.bank_id, account.account)

    def _account_bucket(self, account: Q4AccountId, bucket_count: int) -> int:
        return partition_for_parts(self._account_parts(account), bucket_count)

    def _block_partition(
        self,
        intermediate: Q4AccountId,
        a_bucket: int,
        b_bucket: int,
    ) -> int:
        return partition_for_parts(
            (*self._account_parts(intermediate), a_bucket, b_bucket),
            Q4_BLOCK_JOINER_AMOUNT,
        )

    def _block_plan(self, incoming_size: int, outgoing_size: int) -> tuple[int, int]:
        estimated_pairs = incoming_size * outgoing_size
        if estimated_pairs <= Q4_EDGE_STORE_HOT_PAIR_THRESHOLD:
            return 1, 1
        return (
            max(1, Q4_EDGE_STORE_HOT_A_BUCKETS),
            max(1, Q4_EDGE_STORE_HOT_B_BUCKETS),
        )

    def _send_batch(
        self,
        client_id: int,
        partition: int,
        batch_payload: bytes,
        output=None,
    ) -> None:
        output = output or self._block_joiner_output
        output.send(
            self._packet(MessageType.DATA, client_id, batch_payload),
            routing_key=self._block_routing_key(partition),
        )

    def _append_block_edge(
        self,
        client_id: int,
        block_edge: Q4BlockJoinEdge,
        output=None,
    ) -> None:
        partition = self._block_partition(
            block_edge.intermediate,
            block_edge.a_bucket,
            block_edge.b_bucket,
        )
        payload = self._block_edge_serializer.serialize(block_edge)
        batch_payload = self._batcher.append((client_id, partition), payload)
        counts = self._forwarded_by_partition_by_client.setdefault(client_id, {})
        counts[partition] = counts.get(partition, 0) + 1
        if batch_payload is not None:
            self._send_batch(client_id, partition, batch_payload, output)

    def _flush_client_buffers(self, client_id: int, output=None) -> None:
        for (_, partition), batch_payload in self._batcher.flush(
            lambda k: k[0] == client_id
        ):
            self._send_batch(client_id, partition, batch_payload, output)

    def _accept_counted_edges(self, client_id: int, payload: bytes) -> int:
        edges = self._counted_edge_serializer.deserialize_batch(payload)
        if not edges:
            return 0

        with self._lock:
            if client_id in self._closed_by_client:
                logging.info(
                    "q4_edge_store_message_for_closed_client | id=%s | client_id=%s",
                    ID,
                    client_id,
                )
                return 0

            for edge in edges:
                if edge.role == Q4_EDGE_INCOMING:
                    self._incoming_by_client[client_id][edge.intermediate][
                        edge.endpoint
                    ] += edge.count
                elif edge.role == Q4_EDGE_OUTGOING:
                    self._outgoing_by_client[client_id][edge.intermediate][
                        edge.endpoint
                    ] += edge.count
                else:
                    raise ValueError(f"unexpected Q4 counted edge role: {edge.role}")

            self._processed_by_client[client_id] = (
                self._processed_by_client.get(client_id, 0) + len(edges)
            )
            processed_total = self._processed_by_client[client_id]

        if should_log_progress(processed_total):
            logging.info(
                "q4_edge_store_data_batch | id=%s | client_id=%s | "
                "batch_size=%s | processed_total=%s",
                ID,
                client_id,
                len(edges),
                processed_total,
            )
        return len(edges)

    def _handle_eof(self, client_id: int, payload: bytes) -> None:
        control = self._control_serializer.deserialize(payload)
        should_emit = False

        with self._lock:
            if client_id in self._closed_by_client:
                return
            if control.sender_id in self._eofs_by_client[client_id]:
                logging.info(
                    "q4_edge_store_duplicate_eof | id=%s | client_id=%s | "
                    "source_prefilter_id=%s",
                    ID,
                    client_id,
                    control.sender_id,
                )
                return

            self._eofs_by_client[client_id].add(control.sender_id)
            eof_count = len(self._eofs_by_client[client_id])
            processed_total = self._processed_by_client.get(client_id, 0)
            if eof_count >= Q4_SOURCE_PREFILTER_AMOUNT:
                should_emit = True

        logging.info(
            "q4_edge_store_eof_received | id=%s | client_id=%s | "
            "source_prefilter_id=%s | eof_count=%s | expected_eofs=%s | "
            "sender_expected_total=%s | processed_total=%s",
            ID,
            client_id,
            control.sender_id,
            eof_count,
            Q4_SOURCE_PREFILTER_AMOUNT,
            control.expected_total,
            processed_total,
        )

        if should_emit:
            self._emit_and_close_client(client_id)

    def _emit_client_blocks(self, client_id: int, output=None) -> None:
        incoming = self._incoming_by_client.get(client_id, {})
        outgoing = self._outgoing_by_client.get(client_id, {})
        intermediaries = set(incoming) & set(outgoing)

        for intermediate in intermediaries:
            incoming_counts = incoming[intermediate]
            outgoing_counts = outgoing[intermediate]
            if not incoming_counts or not outgoing_counts:
                continue
            a_buckets, b_buckets = self._block_plan(
                len(incoming_counts),
                len(outgoing_counts),
            )
            logging.info(
                "q4_edge_store_plan_intermediate | id=%s | client_id=%s | "
                "intermediate_bank=%s | intermediate_account=%s | "
                "incoming_endpoints=%s | outgoing_endpoints=%s | "
                "a_buckets=%s | b_buckets=%s",
                ID,
                client_id,
                intermediate.bank_id,
                intermediate.account,
                len(incoming_counts),
                len(outgoing_counts),
                a_buckets,
                b_buckets,
            )
            for endpoint, count in incoming_counts.items():
                a_bucket = self._account_bucket(endpoint, a_buckets)
                for b_bucket in range(b_buckets):
                    self._append_block_edge(
                        client_id,
                        Q4BlockJoinEdge(
                            role=Q4_EDGE_INCOMING,
                            intermediate=intermediate,
                            endpoint=endpoint,
                            a_bucket=a_bucket,
                            b_bucket=b_bucket,
                            count=count,
                        ),
                        output,
                    )

            for endpoint, count in outgoing_counts.items():
                b_bucket = self._account_bucket(endpoint, b_buckets)
                for a_bucket in range(a_buckets):
                    self._append_block_edge(
                        client_id,
                        Q4BlockJoinEdge(
                            role=Q4_EDGE_OUTGOING,
                            intermediate=intermediate,
                            endpoint=endpoint,
                            a_bucket=a_bucket,
                            b_bucket=b_bucket,
                            count=count,
                        ),
                        output,
                    )

    def _forward_eof_to_block_joiners(
        self,
        client_id: int,
        counts_by_partition: dict[int, int],
        output=None,
    ) -> None:
        output = output or self._block_joiner_output
        for partition in range(Q4_BLOCK_JOINER_AMOUNT):
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
                routing_key=self._block_routing_key(partition),
            )
            logging.info(
                "q4_edge_store_forward_eof | id=%s | client_id=%s | "
                "block_joiner_partition=%s | expected_total=%s",
                ID,
                client_id,
                partition,
                expected_total,
            )

    def _emit_and_close_client(self, client_id: int, output=None) -> None:
        with self._lock:
            if client_id in self._closed_by_client:
                return

        self._emit_client_blocks(client_id, output)
        self._flush_client_buffers(client_id, output)

        with self._lock:
            counts_by_partition = dict(
                self._forwarded_by_partition_by_client.get(client_id, {})
            )
            self._incoming_by_client.pop(client_id, None)
            self._outgoing_by_client.pop(client_id, None)
            self._processed_by_client.pop(client_id, None)
            self._eofs_by_client.pop(client_id, None)
            self._forwarded_by_partition_by_client.pop(client_id, None)
            self._closed_by_client.add(client_id)

        self._forward_eof_to_block_joiners(client_id, counts_by_partition, output)

    def _on_message(self, raw, ack, nack):
        try:
            msg_type, client_id, payload = self._proto.unpack_packet(raw)
            if msg_type == MessageType.DATA:
                self._accept_counted_edges(client_id, payload)
            elif msg_type == MessageType.EOF:
                self._handle_eof(client_id, payload)
            else:
                raise ValueError(f"unexpected q4 edge store message type: {msg_type}")
            ack()
        except Exception:
            logging.exception("q4_edge_store_error | id=%s", ID)
            nack()

    def start(self) -> None:
        logging.info(
            "q4_edge_store_start | id=%s | input_exchange=%s | input_key=%s | "
            "source_prefilter_amount=%s | block_joiner_exchange=%s | "
            "block_joiner_amount=%s",
            ID,
            Q4_EDGE_STORE_EXCHANGE,
            self._input_routing_key(),
            Q4_SOURCE_PREFILTER_AMOUNT,
            Q4_BLOCK_JOINER_EXCHANGE,
            Q4_BLOCK_JOINER_AMOUNT,
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
        logging.info("q4_edge_store_shutdown | id=%s", ID)
        self._input.request_stop_consuming()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        for resource in (self._input, self._block_joiner_output):
            try:
                resource.close()
            except Exception as e:
                logging.warning("q4_edge_store_close_error | id=%s | error=%s", ID, e)
