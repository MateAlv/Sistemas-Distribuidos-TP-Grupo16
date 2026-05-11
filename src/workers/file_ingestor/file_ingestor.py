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
    max_line_bytes: int
    logging_level: str


@dataclass(frozen=True)
class FileKey:
    client_id: int
    rel_path: str


@dataclass
class FileState:
    pending: bytes = b""
    expected_offset: int = 0
    chunks_received: int = 0
    lines_received: int = 0
    bytes_received: int = 0


class FileIngestor:
    def __init__(self, config: FileIngestorConfig) -> None:
        self._config = config
        self._stopped = False
        self._consumer: MessageMiddlewareExchangeRabbitMQ | None = None
        self._chunks_received = 0
        self._eofs_received = 0
        self._files: dict[FileKey, FileState] = {}

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
        key = FileKey(client_id=chunk.client_id(), rel_path=chunk.path())
        state = self._files.setdefault(key, FileState())

        if chunk.offset() != state.expected_offset:
            raise ValueError(
                "unexpected chunk offset "
                f"(client_id={key.client_id}, path={key.rel_path}, "
                f"expected={state.expected_offset}, received={chunk.offset()})"
            )

        complete_lines, state.pending = _split_complete_lines(
            state.pending + chunk.payload(),
            self._config.max_line_bytes,
        )

        for line in complete_lines:
            self._handle_line(key, state, line)

        state.expected_offset += chunk.payload_size()
        state.bytes_received += chunk.payload_size()
        state.chunks_received += 1
        self._chunks_received += 1

        logging.info(
            "file_ingestor_chunk | id=%s | client_id=%s | path=%s | offset=%s | "
            "payload_bytes=%s | complete_lines=%s | pending_bytes=%s | "
            "file_chunks=%s | chunks_received=%s",
            self._config.id,
            key.client_id,
            key.rel_path,
            chunk.offset(),
            chunk.payload_size(),
            len(complete_lines),
            len(state.pending),
            state.chunks_received,
            self._chunks_received,
        )

    def _handle_eof(self, client_id: int) -> None:
        keys = [key for key in self._files if key.client_id == client_id]

        for key in keys:
            state = self._files[key]
            if state.pending:
                self._handle_line(key, state, state.pending)
                state.pending = b""

            logging.info(
                "file_ingestor_file_finished | id=%s | client_id=%s | path=%s | "
                "chunks=%s | lines=%s | bytes=%s",
                self._config.id,
                key.client_id,
                key.rel_path,
                state.chunks_received,
                state.lines_received,
                state.bytes_received,
            )
            del self._files[key]

        self._eofs_received += 1
        logging.info(
            "file_ingestor_eof | id=%s | client_id=%s | files_finished=%s | "
            "eofs_received=%s",
            self._config.id,
            client_id,
            len(keys),
            self._eofs_received,
        )

    def _handle_line(self, key: FileKey, state: FileState, line: bytes) -> None:
        clean_line = line[:-1] if line.endswith(b"\r") else line
        if not clean_line:
            return

        _validate_line_size(clean_line, self._config.max_line_bytes)
        state.lines_received += 1
        logging.debug(
            "file_ingestor_line | id=%s | client_id=%s | path=%s | line_bytes=%s | "
            "file_lines=%s",
            self._config.id,
            key.client_id,
            key.rel_path,
            len(clean_line),
            state.lines_received,
        )


def _deserialize_eof(payload: bytes) -> int:
    if len(payload) != 4:
        raise ValueError(f"invalid EOF payload size: {len(payload)}")
    return int.from_bytes(payload, byteorder="big")


def _split_complete_lines(data: bytes, max_line_bytes: int) -> tuple[list[bytes], bytes]:
    lines = data.split(b"\n")
    pending = lines[-1]
    _validate_line_size(pending, max_line_bytes)
    return lines[:-1], pending


def _validate_line_size(line: bytes, max_line_bytes: int) -> None:
    if len(line) > max_line_bytes:
        raise ValueError(f"line exceeded max_line_bytes={max_line_bytes}")
