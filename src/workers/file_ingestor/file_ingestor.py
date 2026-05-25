import logging
import threading
from dataclasses import dataclass

from common.domain.transaction import Transaction
from common.message_protocol.external.types import file_type_name
from common.message_protocol.internal import (
    InternalProtocol,
    LineBatchSerializer,
)
from common.message_protocol.internal.common import ControlMessage, MessageType
from common.message_protocol.internal.control_message_serializer import (
    ControlMessageSerializer,
)
from common.message_protocol.internal.transaction_serializer import TransactionSerializer
from common.middleware import MessageMiddlewareQueueRabbitMQ
from common.middleware.middleware_rabbitmq import MessageMiddlewareExchangeRabbitMQ
from workers.file_ingestor.line_batch_parser import LineBatchParser


@dataclass(frozen=True)
class FileIngestorConfig:
    id: int
    mom_host: str
    queue_name: str
    transaction_output_exchange: str
    control_exchange: str
    response_queue_prefix: str
    logging_level: str


class FileIngestor:
    def __init__(self, config: FileIngestorConfig) -> None:
        self._config = config

        self._internal_protocol = InternalProtocol()
        self._transaction_serializer = TransactionSerializer()
        self._line_batch_serializer = LineBatchSerializer()
        self._control_serializer = ControlMessageSerializer()

        self._input_queue: MessageMiddlewareQueueRabbitMQ | None = None
        self._transaction_output: MessageMiddlewareExchangeRabbitMQ | None = None
        self._control_sender: MessageMiddlewareExchangeRabbitMQ | None = None
        self._control_consumer: MessageMiddlewareExchangeRabbitMQ | None = None
        self._response_consumer: MessageMiddlewareQueueRabbitMQ | None = None
        self._control_thread: threading.Thread | None = None
        self._response_thread: threading.Thread | None = None
        self._closed = False

        self._lock = threading.Lock()
        self._processed_by_client: dict[int, int] = {}
        self._pending_eof_by_client: dict[int, tuple[int, int]] = {}
        self._leader_processed_by_client: dict[int, int] = {}
        self._leader_forwarded_by_client: dict[int, int] = {}
        self._leader_expected_by_client: dict[int, int] = {}

    @property
    def _response_queue_name(self) -> str:
        return f"{self._config.response_queue_prefix}_{self._config.id}"

    def start(self) -> None:
        logging.info(
            "file_ingestor_start | id=%s | mom_host=%s | queue=%s | "
            "control_exchange=%s | response_queue=%s",
            self._config.id,
            self._config.mom_host,
            self._config.queue_name,
            self._config.control_exchange,
            self._response_queue_name,
        )

        self._control_sender = MessageMiddlewareExchangeRabbitMQ(
            self._config.mom_host,
            self._config.control_exchange,
            [self._config.control_exchange],
        )

        self._control_thread = threading.Thread(target=self._run_control_consumer)
        self._response_thread = threading.Thread(target=self._run_response_consumer)
        self._control_thread.start()
        self._response_thread.start()

        self._input_queue = MessageMiddlewareQueueRabbitMQ(
            self._config.mom_host,
            self._config.queue_name,
        )
        try:
            self._input_queue.start_consuming(self._process_message)
        finally:
            self.stop()
            if self._control_thread is not None:
                self._control_thread.join(timeout=5)
            if self._response_thread is not None:
                self._response_thread.join(timeout=5)
            self._close()

    def stop(self) -> None:
        logging.info("file_ingestor_stop | id=%s", self._config.id)
        for consumer in (
            self._input_queue,
            self._control_consumer,
            self._response_consumer,
        ):
            if consumer is not None:
                try:
                    consumer.stop_consuming()
                except Exception:
                    pass

    def _process_message(self, message: bytes, ack, nack) -> None:
        try:
            if not message:
                raise ValueError("empty file ingestor message")

            msg_type, client_id, payload = self._internal_protocol.unpack_packet(message)

            if msg_type == MessageType.DATA:
                self._handle_line_batch(client_id, payload)
            elif msg_type == MessageType.EOF:
                self._handle_upstream_eof(client_id, payload)
            else:
                raise ValueError(f"unknown file ingestor message type: {msg_type}")

            ack()
        except Exception as e:
            logging.error(
                "file_ingestor_message_error | id=%s | error=%s",
                self._config.id,
                e,
            )
            nack()

    def _handle_line_batch(self, client_id: int, payload: bytes) -> None:
        batch = self._line_batch_serializer.deserialize(payload)
        transactions = LineBatchParser.parse(batch)

        if transactions:
            self._send_transaction_batch(
                self._transaction_sender(), client_id, transactions
            )

        forwarded = len(transactions)
        with self._lock:
            self._processed_by_client[client_id] = (
                self._processed_by_client.get(client_id, 0) + forwarded
            )
            pending = self._pending_eof_by_client.get(client_id)

        logging.info(
            "file_ingestor_line_batch | id=%s | client_id=%s | file_type=%s | "
            "path=%s | batch_id=%s | first_line_number=%s | lines=%s | "
            "transactions_sent=%s",
            self._config.id,
            client_id,
            file_type_name(batch.file_type),
            batch.rel_path,
            batch.batch_id,
            batch.first_line_number,
            len(batch.lines),
            forwarded,
        )

        if pending is not None and forwarded:
            _, leader_id = pending
            self._report_to_leader(
                client_id,
                leader_id,
                processed_count=forwarded,
                forwarded_count=forwarded,
            )

    def _handle_upstream_eof(self, client_id: int, payload: bytes) -> None:
        control = self._control_serializer.deserialize(payload)
        expected_total = control.expected_total

        with self._lock:
            self._leader_expected_by_client[client_id] = expected_total

        logging.info(
            "file_ingestor_upstream_eof | id=%s | client_id=%s | expected_total=%s",
            self._config.id,
            client_id,
            expected_total,
        )
        self._broadcast_eof(client_id, expected_total)

    def _run_control_consumer(self) -> None:
        self._control_consumer = MessageMiddlewareExchangeRabbitMQ(
            self._config.mom_host,
            self._config.control_exchange,
            [self._config.control_exchange],
        )
        try:
            self._control_consumer.start_consuming(self._handle_eof_broadcast)
        except Exception as e:
            if not self._closed:
                logging.error(
                    "file_ingestor_control_consumer_stopped | id=%s | error=%s",
                    self._config.id,
                    e,
                )
        finally:
            try:
                self._control_consumer.close()
            except Exception:
                pass

    def _handle_eof_broadcast(self, message: bytes, ack, nack) -> None:
        try:
            msg_type, client_id, payload = self._internal_protocol.unpack_packet(message)
            if msg_type != MessageType.EOF_RECEIVED:
                raise ValueError(f"unexpected control message type: {msg_type}")

            control = self._control_serializer.deserialize(payload)
            leader_id = control.sender_id

            with self._lock:
                duplicate = client_id in self._pending_eof_by_client
                if not duplicate:
                    snapshot = self._processed_by_client.get(client_id, 0)
                    self._pending_eof_by_client[client_id] = (
                        control.expected_total,
                        leader_id,
                    )

            if duplicate:
                logging.info(
                    "file_ingestor_duplicate_eof | id=%s | client_id=%s | leader_id=%s",
                    self._config.id,
                    client_id,
                    leader_id,
                )
                ack()
                return

            self._report_to_leader(
                client_id,
                leader_id,
                processed_count=snapshot,
                forwarded_count=snapshot,
            )
            ack()
        except Exception as e:
            logging.error(
                "file_ingestor_control_error | id=%s | error=%s",
                self._config.id,
                e,
            )
            nack()

    def _broadcast_eof(self, client_id: int, expected_total: int) -> None:
        self._control_sender.send(
            self._packet(
                MessageType.EOF_RECEIVED,
                client_id,
                self._control_payload(self._config.id, expected_total, 0),
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
            self._config.mom_host,
            f"{self._config.response_queue_prefix}_{leader_id}",
        )
        try:
            response_queue.send(
                self._packet(
                    MessageType.PROCESSED_ANSWER,
                    client_id,
                    self._control_payload(
                        self._config.id,
                        forwarded_count,
                        processed_count,
                    ),
                )
            )
        finally:
            response_queue.close()

    def _run_response_consumer(self) -> None:
        self._response_consumer = MessageMiddlewareQueueRabbitMQ(
            self._config.mom_host,
            self._response_queue_name,
        )
        eof_sender = self._new_transaction_sender()
        try:
            self._response_consumer.start_consuming(
                lambda message, ack, nack: self._handle_leader_report(
                    message, ack, nack, eof_sender
                )
            )
        except Exception as e:
            if not self._closed:
                logging.error(
                    "file_ingestor_response_consumer_stopped | id=%s | error=%s",
                    self._config.id,
                    e,
                )
        finally:
            try:
                eof_sender.close()
            except Exception:
                pass
            try:
                self._response_consumer.close()
            except Exception:
                pass

    def _handle_leader_report(self, message: bytes, ack, nack, eof_sender) -> None:
        try:
            msg_type, client_id, payload = self._internal_protocol.unpack_packet(message)
            if msg_type != MessageType.PROCESSED_ANSWER:
                raise ValueError(f"unexpected response message type: {msg_type}")

            control = self._control_serializer.deserialize(payload)
            should_forward = False
            forwarded_total = 0

            with self._lock:
                self._leader_processed_by_client[client_id] = (
                    self._leader_processed_by_client.get(client_id, 0)
                    + control.processed_count
                )
                self._leader_forwarded_by_client[client_id] = (
                    self._leader_forwarded_by_client.get(client_id, 0)
                    + control.expected_total
                )
                expected_total = self._leader_expected_by_client.get(client_id)

                if (
                    expected_total is not None
                    and self._leader_processed_by_client[client_id] >= expected_total
                ):
                    should_forward = True
                    forwarded_total = self._leader_forwarded_by_client[client_id]
                    self._cleanup_client(client_id)

            if should_forward:
                self._forward_eof_downstream(eof_sender, client_id, forwarded_total)
                logging.info(
                    "file_ingestor_eof_forwarded | id=%s | client_id=%s | "
                    "expected_total=%s",
                    self._config.id,
                    client_id,
                    forwarded_total,
                )

            ack()
        except Exception as e:
            logging.error(
                "file_ingestor_response_error | id=%s | error=%s",
                self._config.id,
                e,
            )
            nack()

    def _cleanup_client(self, client_id: int) -> None:
        self._processed_by_client.pop(client_id, None)
        self._pending_eof_by_client.pop(client_id, None)
        self._leader_processed_by_client.pop(client_id, None)
        self._leader_forwarded_by_client.pop(client_id, None)
        self._leader_expected_by_client.pop(client_id, None)

    def _send_transaction(
        self,
        sender: MessageMiddlewareExchangeRabbitMQ,
        client_id: int,
        transaction: Transaction,
    ) -> None:
        sender.send(
            self._packet(
                MessageType.DATA,
                client_id,
                self._transaction_serializer.serialize(transaction),
            )
        )

    def _send_transaction_batch(
        self,
        sender: MessageMiddlewareExchangeRabbitMQ,
        client_id: int,
        transactions: list[Transaction],
    ) -> None:
        sender.send(
            self._packet(
                MessageType.DATA,
                client_id,
                self._transaction_serializer.serialize_batch(transactions),
            )
        )

    def _forward_eof_downstream(
        self,
        sender: MessageMiddlewareExchangeRabbitMQ,
        client_id: int,
        expected_total: int,
    ) -> None:
        sender.send(
            self._packet(
                MessageType.EOF,
                client_id,
                self._control_payload(self._config.id, expected_total, 0),
            )
        )

    def _transaction_sender(self) -> MessageMiddlewareExchangeRabbitMQ:
        if self._transaction_output is None:
            self._transaction_output = self._new_transaction_sender()
        return self._transaction_output

    def _new_transaction_sender(self) -> MessageMiddlewareExchangeRabbitMQ:
        return MessageMiddlewareExchangeRabbitMQ(
            host=self._config.mom_host,
            exchange_name=self._config.transaction_output_exchange,
            routing_keys=[],
            exchange_type="fanout",
        )

    def _packet(self, msg_type: MessageType, client_id: int, payload: bytes) -> bytes:
        return self._internal_protocol.create_packet(
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


    def _close(self) -> None:
        if self._closed:
            return
        self._closed = True
        for resource in (
            self._input_queue,
            self._transaction_output,
            self._control_sender,
        ):
            if resource is None:
                continue
            try:
                resource.close()
            except Exception as e:
                logging.warning(
                    "file_ingestor_close_error | id=%s | error=%s",
                    self._config.id,
                    e,
                )
