import logging
from dataclasses import dataclass

from common.domain.transaction import Transaction
from common.message_protocol.external.types import file_type_name
from common.message_protocol.internal import (
    InternalProtocol,
    LineBatch,
    LineBatchSerializer,
)
from common.message_protocol.internal.common import MessageType
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
    logging_level: str


class FileIngestor:
    def __init__(self, config: FileIngestorConfig) -> None:
        self._config = config
        self._consumer: MessageMiddlewareQueueRabbitMQ | None = None
        self._transaction_output: MessageMiddlewareExchangeRabbitMQ | None = None
        self._transaction_serializer = TransactionSerializer()
        self._line_batch_serializer = LineBatchSerializer()
        self._control_serializer = ControlMessageSerializer()
        self._internal_protocol = InternalProtocol()

    def start(self) -> None:
        logging.info(
            "file_ingestor_start | id=%s | mom_host=%s | queue=%s",
            self._config.id,
            self._config.mom_host,
            self._config.queue_name,
        )

        with MessageMiddlewareQueueRabbitMQ(
            self._config.mom_host,
            self._config.queue_name,
        ) as consumer:
            self._consumer = consumer
            consumer.start_consuming(self._on_message)

    def stop(self) -> None:
        logging.info("file_ingestor_stop | id=%s", self._config.id)
        if self._consumer is not None:
            self._consumer.stop_consuming()
        if self._transaction_output is not None:
            self._transaction_output.close()

    def _on_message(self, message: bytes, ack, nack) -> None:
        try:
            if not message:
                raise ValueError("empty file ingestor message")

            msg_type, client_id, payload = self._internal_protocol.unpack_packet(message)

            if msg_type == MessageType.DATA:
                batch = self._line_batch_serializer.deserialize(payload)
                self._handle_line_batch(client_id, batch)
                ack()
                return

            if msg_type == MessageType.EOF:
                self._forward_eof_payload(client_id, payload)
                ack()
                return

            raise ValueError(f"unknown file ingestor message type: {msg_type}")
        except Exception as e:
            logging.error(
                "file_ingestor_message_error | id=%s | error=%s",
                self._config.id,
                e,
            )
            nack()

    def _handle_line_batch(self, client_id: int, batch: LineBatch) -> None:
        transactions = LineBatchParser.parse(batch)
        for transaction in transactions:
            self._send_transaction(client_id, transaction)

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
            len(transactions),
        )

    def _send_transaction(self, client_id: int, transaction: Transaction) -> None:
        payload = self._transaction_serializer.serialize(transaction)
        message = self._internal_protocol.create_packet(
            msg_type=MessageType.DATA,
            client_id_bytes=client_id.to_bytes(16, byteorder="big"),
            payload=payload,
        )
        self._transaction_sender().send(message)

    def _forward_eof_payload(self, client_id: int, payload: bytes) -> None:
        control = self._control_serializer.deserialize(payload)
        message = self._internal_protocol.create_packet(
            msg_type=MessageType.EOF,
            client_id_bytes=client_id.to_bytes(16, byteorder="big"),
            payload=payload,
        )
        self._transaction_sender().send(message)
        logging.info(
            "file_ingestor_eof_forwarded | id=%s | client_id=%s | "
            "sender_id=%s | expected_total=%s | processed_count=%s",
            self._config.id,
            client_id,
            control.sender_id,
            control.expected_total,
            control.processed_count,
        )

    def _transaction_sender(self) -> MessageMiddlewareExchangeRabbitMQ:
        if self._transaction_output is None:
            self._transaction_output = MessageMiddlewareExchangeRabbitMQ(
                host=self._config.mom_host,
                exchange_name=self._config.transaction_output_exchange,
                routing_keys=[],
                exchange_type="fanout",
            )
        return self._transaction_output
