import logging
import os
import signal
import threading
import zlib

from common import middleware
from common.constants import C_Q2, C_Q3
from common.domain.transaction import Transaction
from common.message_protocol.common import ControlMessage, MessageType
from common.message_protocol.control_message_serializer import ControlMessageSerializer
from common.message_protocol.internal import InternalProtocol
from common.message_protocol.transaction_serializer import TransactionSerializer

try:
    from processors import create_sum_processor
except ImportError:
    from workers.sum.processors import create_sum_processor


ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
CONFIGURATION = os.getenv("CONFIGURATION", C_Q2)
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = f"{SUM_PREFIX}_control"
SUM_RESPONSE_QUEUE_PREFIX = f"{SUM_PREFIX}_response"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]


class SumWorker:
    def __init__(self):
        if CONFIGURATION not in (C_Q2, C_Q3):
            raise ValueError(f"Invalid sum configuration: {CONFIGURATION}")

        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST,
            INPUT_QUEUE,
        )
        self.control_sender = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST,
            SUM_CONTROL_EXCHANGE,
            [SUM_CONTROL_EXCHANGE],
        )
        self.output_exchanges = self._new_output_exchanges()
        self.response_queue_name = f"{SUM_RESPONSE_QUEUE_PREFIX}_{ID}"

        self.internal_protocol = InternalProtocol()
        self.transaction_serializer = TransactionSerializer()
        self.control_serializer = ControlMessageSerializer()

        self.lock = threading.Lock()
        self.processed_by_client = {}
        self.pending_eof_by_client = {}
        self.leader_processed_by_client = {}
        self.leader_forwarded_by_client = {}
        self.leader_expected_by_client = {}
        self.processors_by_client = {}

        self.control_consumer = None
        self.response_consumer = None
        self.control_thread = None
        self.response_thread = None
        self.closed = False

    def _new_output_exchanges(self):
        return [
            middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST,
                AGGREGATION_PREFIX,
                [f"{AGGREGATION_PREFIX}_{i}"],
            )
            for i in range(AGGREGATION_AMOUNT)
        ]

    def _aggregation_index(self, partition_key: str) -> int:
        return zlib.crc32(partition_key.encode("utf-8")) % AGGREGATION_AMOUNT

    def _packet(self, msg_type: MessageType, client_id: int, payload: bytes) -> bytes:
        return self.internal_protocol.create_packet(
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
        return self.control_serializer.serialize(
            ControlMessage(
                sender_id=sender_id,
                expected_total=expected_total,
                processed_count=processed_count,
            )
        )

    def _send_control(
        self,
        exchange,
        msg_type: MessageType,
        client_id: int,
        expected_total: int,
        processed_count: int,
    ) -> None:
        exchange.send(
            self._packet(
                msg_type,
                client_id,
                self._control_payload(ID, expected_total, processed_count),
            )
        )

    def _forward_partial(
        self,
        client_id: int,
        partition_key: str,
        payload: bytes,
        exchanges,
    ) -> None:
        index = self._aggregation_index(partition_key)
        exchanges[index].send(
            self._packet(MessageType.DATA, client_id, payload)
        )

    def _forward_eof_to_aggregators(
        self,
        client_id: int,
        expected_total: int,
        exchanges,
    ) -> None:
        payload = self._control_payload(
            sender_id=ID,
            expected_total=expected_total,
            processed_count=0,
        )
        for exchange in exchanges:
            exchange.send(self._packet(MessageType.EOF, client_id, payload))

    def _process_transaction(self, client_id: int, transaction: Transaction) -> None:
        self._processor_for_client(client_id).process(transaction)
        logging.debug(
            "sum_transaction | configuration=%s | id=%s | client_id=%s | "
            "from_bank=%s | payment_format=%s | amount=%s",
            CONFIGURATION,
            ID,
            client_id,
            transaction.from_bank,
            transaction.format,
            transaction.amount,
        )

    def _processor_for_client(self, client_id: int):
        return self.processors_by_client.setdefault(
            client_id,
            create_sum_processor(CONFIGURATION),
        )

    def _partials_for_transaction(
        self,
        client_id: int,
        transaction: Transaction,
    ):
        return self._processor_for_client(client_id).partials_for_transaction(
            transaction
        )

    def _partials_for_client(self, client_id: int):
        processor = self.processors_by_client.get(client_id)
        if processor is None:
            return []
        return processor.partials()

    def _forward_partials(self, client_id: int, partials, exchanges) -> int:
        forwarded = 0
        for partition_key, payload in partials:
            self._forward_partial(client_id, partition_key, payload, exchanges)
            forwarded += 1
        return forwarded

    def _forward_late_transaction(
        self,
        client_id: int,
        transaction: Transaction,
    ) -> int:
        forwarded = 0
        for partition_key, payload in self._partials_for_transaction(
            client_id,
            transaction,
        ):
            self._forward_partial(
                client_id,
                partition_key,
                payload,
                self.output_exchanges,
            )
            forwarded += 1
        return forwarded

    def _report_to_leader(
        self,
        client_id: int,
        leader_id: int,
        processed_count: int,
        forwarded_count: int,
    ) -> None:
        response_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST,
            f"{SUM_RESPONSE_QUEUE_PREFIX}_{leader_id}",
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

    def _broadcast_eof(self, client_id: int, expected_total: int) -> None:
        self._send_control(
            self.control_sender,
            MessageType.EOF_RECEIVED,
            client_id,
            expected_total,
            0,
        )

    def _handle_data_packet(self, client_id: int, payload: bytes) -> None:
        transaction = self.transaction_serializer.deserialize(payload)

        with self.lock:
            self._process_transaction(client_id, transaction)
            self.processed_by_client[client_id] = (
                self.processed_by_client.get(client_id, 0) + 1
            )
            pending = self.pending_eof_by_client.get(client_id)

        if pending is None:
            return

        _, leader_id = pending
        forwarded_count = self._forward_late_transaction(client_id, transaction)
        self._report_to_leader(
            client_id,
            leader_id,
            processed_count=1,
            forwarded_count=forwarded_count,
        )

    def _handle_upstream_eof(self, client_id: int, payload: bytes) -> None:
        control_message = self.control_serializer.deserialize(payload)
        expected_total = control_message.expected_total

        with self.lock:
            self.leader_expected_by_client[client_id] = expected_total

        logging.info(
            "sum_upstream_eof | configuration=%s | id=%s | client_id=%s | "
            "expected_total=%s",
            CONFIGURATION,
            ID,
            client_id,
            expected_total,
        )
        self._broadcast_eof(client_id, expected_total)

    def _handle_eof_broadcast(self, message: bytes, ack, nack, output_exchanges) -> None:
        try:
            msg_type, client_id, payload = self.internal_protocol.unpack_packet(message)
            if msg_type != MessageType.EOF_RECEIVED:
                raise ValueError(f"Unexpected sum control message type: {msg_type}")

            control_message = self.control_serializer.deserialize(payload)
            leader_id = control_message.sender_id
            expected_total = control_message.expected_total

            with self.lock:
                processed_count = self.processed_by_client.get(client_id, 0)
                partials = self._partials_for_client(client_id)
                self.pending_eof_by_client[client_id] = (
                    expected_total,
                    leader_id,
                )

            forwarded_count = self._forward_partials(
                client_id,
                partials,
                output_exchanges,
            )
            self._report_to_leader(
                client_id,
                leader_id,
                processed_count=processed_count,
                forwarded_count=forwarded_count,
            )
            ack()
        except Exception:
            logging.exception("sum_control_error | configuration=%s | id=%s", CONFIGURATION, ID)
            nack()

    def _handle_leader_report(self, message: bytes, ack, nack, output_exchanges) -> None:
        try:
            msg_type, client_id, payload = self.internal_protocol.unpack_packet(message)
            if msg_type != MessageType.PROCESSED_ANSWER:
                raise ValueError(f"Unexpected sum response message type: {msg_type}")

            control_message = self.control_serializer.deserialize(payload)
            should_forward_eof = False
            expected_total = None

            with self.lock:
                self.leader_processed_by_client[client_id] = (
                    self.leader_processed_by_client.get(client_id, 0)
                    + control_message.processed_count
                )
                self.leader_forwarded_by_client[client_id] = (
                    self.leader_forwarded_by_client.get(client_id, 0)
                    + control_message.expected_total
                )
                expected_total = self.leader_expected_by_client.get(client_id)

                if (
                    expected_total is not None
                    and self.leader_processed_by_client[client_id] == expected_total
                ):
                    should_forward_eof = True
                    forwarded_total = self.leader_forwarded_by_client[client_id]
                    self._cleanup_client(client_id)

            if should_forward_eof:
                self._forward_eof_to_aggregators(
                    client_id,
                    forwarded_total,
                    output_exchanges,
                )

            ack()
        except Exception:
            logging.exception("sum_response_error | configuration=%s | id=%s", CONFIGURATION, ID)
            nack()

    def _cleanup_client(self, client_id: int) -> None:
        self.processed_by_client.pop(client_id, None)
        self.pending_eof_by_client.pop(client_id, None)
        self.leader_processed_by_client.pop(client_id, None)
        self.leader_forwarded_by_client.pop(client_id, None)
        self.leader_expected_by_client.pop(client_id, None)
        self.processors_by_client.pop(client_id, None)

    def process_message(self, message: bytes, ack, nack) -> None:
        try:
            msg_type, client_id, payload = self.internal_protocol.unpack_packet(message)

            if msg_type == MessageType.DATA:
                self._handle_data_packet(client_id, payload)
            elif msg_type == MessageType.EOF:
                self._handle_upstream_eof(client_id, payload)
            else:
                raise ValueError(f"Unexpected sum data message type: {msg_type}")

            ack()
        except Exception:
            logging.exception("sum_data_error | configuration=%s | id=%s", CONFIGURATION, ID)
            nack()

    def _start_control_consumer(self) -> None:
        control_consumer = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST,
            SUM_CONTROL_EXCHANGE,
            [SUM_CONTROL_EXCHANGE],
        )
        self.control_consumer = control_consumer
        output_exchanges = self._new_output_exchanges()

        try:
            control_consumer.start_consuming(
                lambda message, ack, nack: self._handle_eof_broadcast(
                    message,
                    ack,
                    nack,
                    output_exchanges,
                )
            )
        finally:
            for exchange in output_exchanges:
                exchange.close()
            control_consumer.close()

    def _start_response_consumer(self) -> None:
        response_consumer = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST,
            self.response_queue_name,
        )
        self.response_consumer = response_consumer
        output_exchanges = self._new_output_exchanges()

        try:
            response_consumer.start_consuming(
                lambda message, ack, nack: self._handle_leader_report(
                    message,
                    ack,
                    nack,
                    output_exchanges,
                )
            )
        finally:
            for exchange in output_exchanges:
                exchange.close()
            response_consumer.close()

    def start(self) -> None:
        signal.signal(signal.SIGTERM, lambda *_: self.handle_sigterm())
        self.control_thread = threading.Thread(target=self._start_control_consumer)
        self.response_thread = threading.Thread(target=self._start_response_consumer)
        self.control_thread.start()
        self.response_thread.start()

        try:
            self.input_queue.start_consuming(self.process_message)
        finally:
            self.handle_sigterm()
            self.control_thread.join(timeout=5)
            self.response_thread.join(timeout=5)
            self.close()

    def handle_sigterm(self) -> None:
        if self.closed:
            return

        logging.info(
            "sum_shutdown | configuration=%s | id=%s",
            CONFIGURATION,
            ID,
        )
        self.input_queue.stop_consuming()
        if self.control_consumer is not None:
            self.control_consumer.stop_consuming()
        if self.response_consumer is not None:
            self.response_consumer.stop_consuming()

    def close(self) -> None:
        if self.closed:
            return
        self.closed = True

        for resource in [
            self.input_queue,
            self.control_sender,
            *self.output_exchanges,
        ]:
            try:
                resource.close()
            except Exception as e:
                logging.warning(
                    "sum_close_error | configuration=%s | id=%s | error=%s",
                    CONFIGURATION,
                    ID,
                    e,
                )
