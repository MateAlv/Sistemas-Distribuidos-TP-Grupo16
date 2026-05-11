import logging
from dataclasses import dataclass

from common.message_protocol.external import FileChunk
from common.message_protocol.external.types import (
    MSG_CHUNK,
    MSG_EOF,
    file_ingestor_routing_key,
)
from common.middleware.middleware_rabbitmq import MessageMiddlewareExchangeRabbitMQ


@dataclass(frozen=True)
class FileIngestorConfig:
    id: int
    mom_host: str
    file_ingestor_exchange: str
    queue_name: str
    logging_level: str


class FileIngestor:
    def __init__(self, config: FileIngestorConfig) -> None:
        self._config = config
        self._stopped = False
        self._consumer: MessageMiddlewareExchangeRabbitMQ | None = None
        self._chunks_received = 0
        self._eofs_received = 0

    def start(self) -> None:
        routing_key = file_ingestor_routing_key(self._config.id)
        logging.info(
            "file_ingestor_start | id=%s | mom_host=%s | exchange=%s | queue=%s | "
            "routing_key=%s",
            self._config.id,
            self._config.mom_host,
            self._config.file_ingestor_exchange,
            self._config.queue_name,
            routing_key,
        )

        with MessageMiddlewareExchangeRabbitMQ(
            host=self._config.mom_host,
            exchange_name=self._config.file_ingestor_exchange,
            routing_keys=[routing_key],
            queue_name=self._config.queue_name,
            exclusive=False,
        ) as consumer:
            self._consumer = consumer
            consumer.start_consuming(self._on_message)

    def stop(self) -> None:
        self._stopped = True
        logging.info("file_ingestor_stop | id=%s", self._config.id)
        if self._consumer is not None:
            self._consumer.stop_consuming()

    def _on_message(self, message: bytes, ack, nack) -> None:
        try:
            if not message:
                raise ValueError("empty file ingestor message")

            msg_type = message[0]
            payload = message[1:]

            if msg_type == MSG_CHUNK:
                self._handle_chunk(FileChunk.deserialize(payload))
                ack()
                return

            if msg_type == MSG_EOF:
                self._handle_eof(_deserialize_eof(payload))
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

    def _handle_chunk(self, chunk: FileChunk) -> None:
        self._chunks_received += 1
        logging.info(
            "file_ingestor_chunk | id=%s | client_id=%s | path=%s | offset=%s | "
            "payload_bytes=%s | chunks_received=%s",
            self._config.id,
            chunk.client_id(),
            chunk.path(),
            chunk.offset(),
            chunk.payload_size(),
            self._chunks_received,
        )

    def _handle_eof(self, client_id: int) -> None:
        self._eofs_received += 1
        logging.info(
            "file_ingestor_eof | id=%s | client_id=%s | eofs_received=%s",
            self._config.id,
            client_id,
            self._eofs_received,
        )


def _deserialize_eof(payload: bytes) -> int:
    if len(payload) != 4:
        raise ValueError(f"invalid EOF payload size: {len(payload)}")
    return int.from_bytes(payload, byteorder="big")
