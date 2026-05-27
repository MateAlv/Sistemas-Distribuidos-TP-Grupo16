import os
import logging
import threading

from common import message_protocol
from common.batch_buffer import BatchBuffer
from common.logging_utils import should_log_progress
from common.middleware.middleware_rabbitmq import (
    MessageMiddlewareQueueRabbitMQ,
    MessageMiddlewareExchangeRabbitMQ,
)
from common.message_protocol.internal import partition_for_key
from common.message_protocol.internal.common import ControlMessage, MessageType
from common.message_protocol.internal.control_message_serializer import ControlMessageSerializer
from common.constants import EDGE_A_TO_M, EDGE_M_TO_B

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SG_LINKER_EXCHANGE = os.environ["SG_LINKER_EXCHANGE"]
SG_LINKER_AMOUNT = int(os.environ["SG_LINKER_AMOUNT"])
SG_MAPPER_BATCH_BYTES = int(os.environ.get("SG_MAPPER_BATCH_BYTES", str(1024 * 1024)))
SG_MAPPER_BATCH_MAX_TX = int(os.environ.get("SG_MAPPER_BATCH_MAX_TX", "5000"))
SG_MAPPER_AMOUNT = int(os.environ.get("SG_MAPPER_AMOUNT", "1"))
SG_MAPPER_PREFIX = os.environ.get("SG_MAPPER_PREFIX", "sg_mapper")
SG_MAPPER_CONTROL_EXCHANGE = os.environ.get(
    "SG_MAPPER_CONTROL_EXCHANGE", f"{SG_MAPPER_PREFIX}_control"
)
SG_MAPPER_RESPONSE_QUEUE_PREFIX = os.environ.get(
    "SG_MAPPER_RESPONSE_QUEUE_PREFIX", f"{SG_MAPPER_PREFIX}_response"
)


class ScatterGatherMapper:
    def __init__(self):
        self._input = MessageMiddlewareQueueRabbitMQ(MOM_HOST, INPUT_QUEUE)
        self._linkers = self._new_linker_outputs()
        self._control_sender = None
        if SG_MAPPER_AMOUNT > 1:
            self._control_sender = MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST,
                SG_MAPPER_CONTROL_EXCHANGE,
                [SG_MAPPER_CONTROL_EXCHANGE],
            )
        self._response_queue_name = f"{SG_MAPPER_RESPONSE_QUEUE_PREFIX}_{ID}"
        self._tx_ser = message_protocol.internal.TransactionSerializer()
        self._proto = message_protocol.internal.InternalProtocol()
        self._control_serializer = ControlMessageSerializer()


        self._batcher = BatchBuffer(SG_MAPPER_BATCH_BYTES, SG_MAPPER_BATCH_MAX_TX)

        self._lock = threading.Lock()
        self._processed_by_client = {}
        self._forwarded_by_client = {}
        self._pending_eof_by_client = {}
        self._leader_processed_by_client = {}
        self._leader_forwarded_by_client = {}
        self._leader_expected_by_client = {}
        self._closed_by_client = set()

        self._control_consumer = None
        self._response_consumer = None
        self._control_thread = None
        self._response_thread = None
        self._closed = False
        self._stopped = False

    def _new_linker_outputs(self):
        return [
            MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, SG_LINKER_EXCHANGE, [f"sg_linker_{i}"]
            )
            for i in range(SG_LINKER_AMOUNT)
        ]

    def _on_message(self, raw, ack, nack):
        try:
            msg_type, client_id, payload = self._proto.unpack_packet(raw)
            if msg_type == MessageType.DATA:
                self._handle_data_packet(client_id, payload)
            elif msg_type == MessageType.EOF:
                self._handle_upstream_eof(client_id, payload)
            else:
                raise ValueError(f"unexpected mapper message type: {msg_type}")
            ack()
        except Exception:
            logging.exception("mapper_%s error", ID)
            nack()

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

    def _publish_edges(
        self,
        client_id: int,
        partition: int,
        tag: int,
        batch_payload: bytes,
        linkers=None,
    ) -> None:
        linkers = linkers or self._linkers
        msg = self._proto.create_packet(
            msg_type=MessageType.DATA,
            client_id_bytes=client_id.to_bytes(16, byteorder="big"),
            payload=bytes([tag]) + batch_payload,
        )
        linkers[partition].send(msg)

    def _append_edge(
        self,
        client_id: int,
        tag: int,
        tx_bytes: bytes,
        partition: int,
        linkers=None,
    ) -> None:
        batch_payload = self._batcher.append(
            (client_id, partition, tag), tx_bytes
        )
        if batch_payload is not None:
            self._publish_edges(client_id, partition, tag, batch_payload, linkers)

    def _flush_client_buffers(self, client_id: int, linkers=None) -> None:
        for (_, partition, tag), batch_payload in self._batcher.flush(
            lambda k: k[0] == client_id
        ):
            self._publish_edges(client_id, partition, tag, batch_payload, linkers)

    def _forward_transaction(self, client_id: int, tx, linkers=None) -> int:
        payload = self._tx_ser.serialize(tx)
        self._append_edge(
            client_id,
            EDGE_A_TO_M,
            payload,
            partition_for_key(tx.to_account, SG_LINKER_AMOUNT),
            linkers,
        )
        self._append_edge(
            client_id,
            EDGE_M_TO_B,
            payload,
            partition_for_key(tx.from_account, SG_LINKER_AMOUNT),
            linkers,
        )
        return 2

    def _handle_data_packet(self, client_id: int, payload: bytes) -> None:
        transactions = self._tx_ser.deserialize_batch(payload)
        if not transactions:
            return

        with self._lock:
            if client_id in self._closed_by_client:
                logging.info(
                    "mapper_%s message_for_closed_client | client_id=%s",
                    ID,
                    client_id,
                )
                return

            forwarded_count = 0
            for tx in transactions:
                forwarded_count += self._forward_transaction(client_id, tx)
            self._processed_by_client[client_id] = (
                self._processed_by_client.get(client_id, 0) + len(transactions)
            )
            self._forwarded_by_client[client_id] = (
                self._forwarded_by_client.get(client_id, 0) + forwarded_count
            )
            processed_total = self._processed_by_client[client_id]
            forwarded_total = self._forwarded_by_client[client_id]
            pending = self._pending_eof_by_client.get(client_id)

        if should_log_progress(processed_total):
            logging.info(
                "mapper_%s data_batch | client_id=%s | batch_size=%s | "
                "edges_in_batch=%s | processed_total=%s | forwarded_total=%s | "
                "pending_eof=%s",
                ID,
                client_id,
                len(transactions),
                forwarded_count,
                processed_total,
                forwarded_total,
                pending is not None,
            )

        if pending is not None:
            self._flush_client_buffers(client_id)
            _, leader_id = pending
            self._report_to_leader(
                client_id,
                leader_id,
                processed_count=len(transactions),
                forwarded_count=forwarded_count,
            )

    def _forward_eof_to_linkers(
        self, client_id: int, expected_total: int, linkers=None
    ) -> None:
        linkers = linkers or self._linkers
        payload = self._control_payload(
            sender_id=ID,
            expected_total=expected_total,
            processed_count=0,
        )
        msg = self._proto.create_packet(
            msg_type=MessageType.EOF,
            client_id_bytes=client_id.to_bytes(16, byteorder="big"),
            payload=payload,
        )
        for linker in linkers:
            linker.send(msg)
        logging.info(
            "mapper_%s forwarded_eof | client_id=%s | linkers=%s",
            ID,
            client_id,
            len(linkers),
        )

    def _handle_upstream_eof(self, client_id: int, payload: bytes) -> None:
        if SG_MAPPER_AMOUNT == 1:
            with self._lock:
                if client_id in self._closed_by_client:
                    return
                expected_total = self._forwarded_by_client.get(client_id, 0)
            # Flush before cleanup so every buffered edge reaches the linker
            # ahead of the EOF; cleanup then discards the now-empty buffers.
            self._flush_client_buffers(client_id)
            with self._lock:
                self._cleanup_client_locked(client_id)
            self._forward_eof_to_linkers(client_id, expected_total)
            return

        control_message = self._control_serializer.deserialize(payload)
        expected_total = control_message.expected_total
        with self._lock:
            if client_id in self._closed_by_client:
                return
            self._leader_expected_by_client[client_id] = expected_total

        logging.info(
            "mapper_%s upstream_eof | client_id=%s | expected_total=%s",
            ID,
            client_id,
            expected_total,
        )
        self._broadcast_eof(client_id, expected_total)

    def _broadcast_eof(self, client_id: int, expected_total: int) -> None:
        if self._control_sender is None:
            raise RuntimeError("mapper control sender is not configured")
        self._control_sender.send(
            self._packet(
                MessageType.EOF_RECEIVED,
                client_id,
                self._control_payload(ID, expected_total, 0),
            )
        )

    def _report_to_leader(
        self,
        client_id: int,
        leader_id: int,
        processed_count: int,
        forwarded_count: int,
    ) -> None:
        response_queue = MessageMiddlewareQueueRabbitMQ(
            MOM_HOST,
            f"{SG_MAPPER_RESPONSE_QUEUE_PREFIX}_{leader_id}",
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

    def _handle_eof_broadcast(self, message: bytes, ack, nack, linkers=None) -> None:
        try:
            msg_type, client_id, payload = self._proto.unpack_packet(message)
            if msg_type != MessageType.EOF_RECEIVED:
                raise ValueError(f"unexpected mapper control message type: {msg_type}")

            control_message = self._control_serializer.deserialize(payload)
            leader_id = control_message.sender_id
            expected_total = control_message.expected_total
            duplicate_eof = False

            with self._lock:
                if (
                    client_id in self._closed_by_client
                    or client_id in self._pending_eof_by_client
                ):
                    duplicate_eof = True
                    processed_count = 0
                    forwarded_count = 0
                else:
                    processed_count = self._processed_by_client.get(client_id, 0)
                    forwarded_count = self._forwarded_by_client.get(client_id, 0)
                    # Mark pending before flushing: any DATA the main thread is
                    # still processing now sees the pending state and flushes its
                    # own tail, so no buffered edge can slip past the EOF.
                    self._pending_eof_by_client[client_id] = (
                        expected_total,
                        leader_id,
                    )

            if not duplicate_eof:
                # Flush the edges buffered before the EOF so they reach the
                # linker ahead of it, published on this control thread's own
                # linker channels.
                self._flush_client_buffers(client_id, linkers)
                logging.info(
                    "mapper_%s eof_control_snapshot | client_id=%s | "
                    "leader_id=%s | processed_count=%s | forwarded_count=%s | "
                    "expected_total=%s",
                    ID,
                    client_id,
                    leader_id,
                    processed_count,
                    forwarded_count,
                    expected_total,
                )
                self._report_to_leader(
                    client_id,
                    leader_id,
                    processed_count=processed_count,
                    forwarded_count=forwarded_count,
                )
            else:
                logging.info(
                    "mapper_%s duplicate_eof_control | client_id=%s | "
                    "leader_id=%s | expected_total=%s",
                    ID,
                    client_id,
                    leader_id,
                    expected_total,
                )
            ack()
        except Exception:
            logging.exception("mapper_%s control_error", ID)
            nack()

    def _handle_leader_report(self, message: bytes, ack, nack, linkers=None) -> None:
        try:
            msg_type, client_id, payload = self._proto.unpack_packet(message)
            if msg_type != MessageType.PROCESSED_ANSWER:
                raise ValueError(f"unexpected mapper response message type: {msg_type}")

            control_message = self._control_serializer.deserialize(payload)
            should_forward_eof = False
            expected_total = None
            forwarded_total = 0

            with self._lock:
                if client_id in self._closed_by_client:
                    ack()
                    return

                self._leader_processed_by_client[client_id] = (
                    self._leader_processed_by_client.get(client_id, 0)
                    + control_message.processed_count
                )
                self._leader_forwarded_by_client[client_id] = (
                    self._leader_forwarded_by_client.get(client_id, 0)
                    + control_message.expected_total
                )
                expected_total = self._leader_expected_by_client.get(client_id)

                if (
                    expected_total is not None
                    and self._leader_processed_by_client[client_id] >= expected_total
                ):
                    should_forward_eof = True
                    forwarded_total = self._leader_forwarded_by_client[client_id]
                    self._cleanup_client_locked(client_id)

            if should_forward_eof:
                self._forward_eof_to_linkers(client_id, forwarded_total, linkers)

            ack()
        except Exception:
            logging.exception("mapper_%s response_error", ID)
            nack()

    def _start_control_consumer(self) -> None:
        control_consumer = MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST,
            SG_MAPPER_CONTROL_EXCHANGE,
            [SG_MAPPER_CONTROL_EXCHANGE],
        )
        self._control_consumer = control_consumer
        # Dedicated linker channels: this thread publishes the EOF-time buffer
        # flush, and pika channels are not shared across threads.
        linkers = self._new_linker_outputs()
        try:
            if not self._stopped:
                control_consumer.start_consuming(
                    lambda message, ack, nack: self._handle_eof_broadcast(
                        message, ack, nack, linkers
                    )
                )
        finally:
            for linker in linkers:
                linker.close()
            control_consumer.close()

    def _start_response_consumer(self) -> None:
        response_consumer = MessageMiddlewareQueueRabbitMQ(
            MOM_HOST,
            self._response_queue_name,
        )
        self._response_consumer = response_consumer
        linkers = self._new_linker_outputs()
        try:
            if not self._stopped:
                response_consumer.start_consuming(
                    lambda message, ack, nack: self._handle_leader_report(
                        message, ack, nack, linkers
                    )
                )
        finally:
            for linker in linkers:
                linker.close()
            response_consumer.close()

    def _cleanup_client_locked(self, client_id: int) -> None:
        self._batcher.discard(lambda k: k[0] == client_id)
        self._processed_by_client.pop(client_id, None)
        self._forwarded_by_client.pop(client_id, None)
        self._pending_eof_by_client.pop(client_id, None)
        self._leader_processed_by_client.pop(client_id, None)
        self._leader_forwarded_by_client.pop(client_id, None)
        self._leader_expected_by_client.pop(client_id, None)
        self._closed_by_client.add(client_id)

    def start(self):
        logging.info("mapper_%s starting", ID)
        if SG_MAPPER_AMOUNT > 1:
            self._control_thread = threading.Thread(target=self._start_control_consumer)
            self._response_thread = threading.Thread(target=self._start_response_consumer)
            self._control_thread.start()
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

    def handle_sigterm(self):
        if self._stopped:
            return
        self._stopped = True
        logging.info("mapper_%s sigterm", ID)
        self._input.request_stop_consuming()
        if self._control_consumer is not None:
            self._control_consumer.request_stop_consuming()
        if self._response_consumer is not None:
            self._response_consumer.request_stop_consuming()

    def close(self):
        if self._closed:
            return
        self._closed = True
        self._input.close()
        if self._control_sender is not None:
            self._control_sender.close()
        for linker in self._linkers:
            linker.close()
