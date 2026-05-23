import logging
from dataclasses import dataclass, field

from common.message_protocol.external import FileChunk, FileEof
from common.message_protocol.external.types import (
    FILE_TYPE_ACCOUNTS,
    FILE_TYPE_TRANSACTIONS,
    MSG_CHUNK,
    MSG_EOF,
    file_ingestor_routing_key,
    file_type_name,
)
from common.message_protocol.internal import (
    ControlMessage,
    ControlMessageSerializer,
    InternalProtocol,
    LineBatch,
    LineBatchSerializer,
    MessageType,
)
from common.middleware import MessageMiddlewareQueueRabbitMQ
from common.middleware.middleware_rabbitmq import MessageMiddlewareExchangeRabbitMQ
from workers.common.csv_rows import parse_csv_line
from workers.common.line_splitter import LineSplitter


@dataclass(frozen=True)
class FileSplitterConfig:
    id: int
    mom_host: str
    input_exchange: str
    queue_name: str
    output_queue: str
    max_line_bytes: int
    max_batch_bytes: int
    logging_level: str
    input_routing_key: str | None = None


@dataclass(frozen=True)
class FileKey:
    client_id: int
    rel_path: str


@dataclass
class FileState:
    splitter: LineSplitter
    file_type: int | None = None
    header: tuple[str, ...] | None = None
    batch_lines: list[bytes] = field(default_factory=list)
    batch_first_line_number: int = 0
    batch_payload_bytes: int = 0
    batch_id: int = 0
    lines_seen: int = 0
    data_lines_emitted: int = 0
    chunks_received: int = 0
    bytes_received: int = 0


class FileSplitter:
    def __init__(self, config: FileSplitterConfig) -> None:
        self._config = config
        self._consumer: MessageMiddlewareExchangeRabbitMQ | None = None
        self._line_batch_output: MessageMiddlewareQueueRabbitMQ | None = None
        self._line_batch_serializer = LineBatchSerializer()
        self._control_serializer = ControlMessageSerializer()
        self._internal_protocol = InternalProtocol()
        self._files: dict[FileKey, FileState] = {}
        self._chunks_received = 0
        self._eofs_received = 0

    def start(self) -> None:
        routing_key = self._input_routing_key()
        logging.info(
            "file_splitter_start | id=%s | mom_host=%s | exchange=%s | queue=%s | "
            "routing_key=%s | output_queue=%s | max_batch_bytes=%s",
            self._config.id,
            self._config.mom_host,
            self._config.input_exchange,
            self._config.queue_name,
            routing_key,
            self._config.output_queue,
            self._config.max_batch_bytes,
        )

        with MessageMiddlewareExchangeRabbitMQ(
            host=self._config.mom_host,
            exchange_name=self._config.input_exchange,
            routing_keys=[routing_key],
            queue_name=self._config.queue_name,
            exclusive=False,
        ) as consumer:
            self._consumer = consumer
            consumer.start_consuming(self._on_message)

    def stop(self) -> None:
        logging.info("file_splitter_stop | id=%s", self._config.id)
        if self._consumer is not None:
            self._consumer.stop_consuming()
        if self._line_batch_output is not None:
            self._line_batch_output.close()

    def _on_message(self, message: bytes, ack, nack) -> None:
        try:
            if not message:
                raise ValueError("empty file splitter message")

            msg_type = message[0]
            payload = message[1:]

            if msg_type == MSG_CHUNK:
                self._handle_chunk(FileChunk.deserialize(payload))
                ack()
                return

            if msg_type == MSG_EOF:
                eof = _deserialize_eof(payload)
                if isinstance(eof, FileEof):
                    self._handle_file_eof(eof)
                else:
                    self._handle_eof(eof)
                ack()
                return

            raise ValueError(f"unknown file splitter message type: {msg_type}")
        except Exception as e:
            logging.error(
                "file_splitter_message_error | id=%s | error=%s",
                self._config.id,
                e,
            )
            nack()

    def _handle_chunk(self, chunk: FileChunk) -> None:
        key = FileKey(client_id=chunk.client_id(), rel_path=chunk.path())
        state = self._state_for(key)

        if state.file_type is None:
            state.file_type = chunk.file_type()
        elif state.file_type != chunk.file_type():
            raise ValueError(
                "file_type changed for file "
                f"(client_id={key.client_id}, path={key.rel_path}, "
                f"expected={state.file_type}, received={chunk.file_type()})"
            )

        try:
            complete_lines = state.splitter.push(chunk.offset(), chunk.payload())
        except ValueError as exc:
            raise ValueError(
                f"{exc} (client_id={key.client_id}, path={key.rel_path})"
            ) from exc

        for line in complete_lines:
            self._handle_line(key, state, line)

        state.bytes_received += chunk.payload_size()
        state.chunks_received += 1
        self._chunks_received += 1

        logging.info(
            "file_splitter_chunk | id=%s | client_id=%s | file_type=%s | path=%s | "
            "offset=%s | payload_bytes=%s | complete_lines=%s | pending_bytes=%s | "
            "file_chunks=%s | chunks_received=%s",
            self._config.id,
            key.client_id,
            file_type_name(state.file_type),
            key.rel_path,
            chunk.offset(),
            chunk.payload_size(),
            len(complete_lines),
            state.splitter.pending_size(),
            state.chunks_received,
            self._chunks_received,
        )

    def _handle_file_eof(self, eof: FileEof) -> None:
        key = FileKey(client_id=eof.client_id(), rel_path=eof.path())
        file_finished, saw_transactions_file, emitted = self._finish_file(
            key,
            expected_file_type=eof.file_type(),
        )

        if saw_transactions_file:
            self._send_eof(eof.client_id(), emitted)

        self._eofs_received += 1
        logging.info(
            "file_splitter_eof | id=%s | client_id=%s | file_type=%s | path=%s | "
            "files_finished=%s | transactions_file=%s | expected_total=%s | "
            "eofs_received=%s",
            self._config.id,
            eof.client_id(),
            file_type_name(eof.file_type()),
            eof.path(),
            1 if file_finished else 0,
            saw_transactions_file,
            emitted,
            self._eofs_received,
        )

    def _handle_eof(self, client_id: int) -> None:
        keys = [key for key in self._files if key.client_id == client_id]
        expected_total = 0
        saw_transactions_file = False

        for key in keys:
            _, file_saw_transactions, file_emitted = self._finish_file(key)
            saw_transactions_file = saw_transactions_file or file_saw_transactions
            expected_total += file_emitted

        if saw_transactions_file:
            self._send_eof(client_id, expected_total)

        self._eofs_received += 1
        logging.info(
            "file_splitter_eof | id=%s | client_id=%s | files_finished=%s | "
            "transactions_file=%s | expected_total=%s | eofs_received=%s",
            self._config.id,
            client_id,
            len(keys),
            saw_transactions_file,
            expected_total,
            self._eofs_received,
        )

    def _finish_file(
        self,
        key: FileKey,
        expected_file_type: int | None = None,
    ) -> tuple[bool, bool, int]:
        state = self._files.get(key)
        if state is None:
            logging.warning(
                "file_splitter_file_eof_missing | id=%s | client_id=%s | path=%s",
                self._config.id,
                key.client_id,
                key.rel_path,
            )
            return False, False, 0

        if expected_file_type is not None and state.file_type != expected_file_type:
            raise ValueError(
                "file EOF type mismatch "
                f"(client_id={key.client_id}, path={key.rel_path}, "
                f"expected={state.file_type}, received={expected_file_type})"
            )

        for line in state.splitter.finish():
            self._handle_line(key, state, line)

        if state.file_type == FILE_TYPE_TRANSACTIONS:
            self._flush_batch(key, state)

        saw_transactions_file = state.file_type == FILE_TYPE_TRANSACTIONS
        emitted = state.data_lines_emitted if saw_transactions_file else 0

        logging.info(
            "file_splitter_file_finished | id=%s | client_id=%s | file_type=%s | "
            "path=%s | chunks=%s | lines=%s | batches=%s | expected_total=%s | bytes=%s",
            self._config.id,
            key.client_id,
            file_type_name(state.file_type),
            key.rel_path,
            state.chunks_received,
            state.lines_seen,
            state.batch_id,
            emitted,
            state.bytes_received,
        )
        del self._files[key]
        return True, saw_transactions_file, emitted

    def _handle_line(self, key: FileKey, state: FileState, line: bytes) -> None:
        state.lines_seen += 1

        if state.file_type == FILE_TYPE_ACCOUNTS:
            return

        if state.file_type != FILE_TYPE_TRANSACTIONS:
            raise ValueError(
                "unknown file_type for file "
                f"(client_id={key.client_id}, path={key.rel_path}, "
                f"file_type={state.file_type})"
            )

        if state.header is None:
            state.header = _parse_header(line)
            logging.debug(
                "file_splitter_transaction_header | id=%s | client_id=%s | path=%s | "
                "columns=%s",
                self._config.id,
                key.client_id,
                key.rel_path,
                len(state.header),
            )
            return

        self._append_line(key, state, line)

    def _append_line(self, key: FileKey, state: FileState, line: bytes) -> None:
        line_size = _line_item_size(line)
        candidate_size = self._batch_packet_size(key, state, line_size)
        if state.batch_lines and candidate_size > self._config.max_batch_bytes:
            self._flush_batch(key, state)

        if not state.batch_lines:
            state.batch_first_line_number = state.lines_seen
            state.batch_payload_bytes = 0

        state.batch_lines.append(line)
        state.batch_payload_bytes += line_size

    def _flush_batch(self, key: FileKey, state: FileState) -> None:
        if not state.batch_lines:
            return

        if state.header is None:
            raise ValueError(
                "missing transaction header "
                f"(client_id={key.client_id}, path={key.rel_path})"
            )

        batch = LineBatch(
            file_type=FILE_TYPE_TRANSACTIONS,
            rel_path=key.rel_path,
            batch_id=state.batch_id,
            first_line_number=state.batch_first_line_number,
            header=state.header,
            lines=tuple(state.batch_lines),
        )
        payload = self._line_batch_serializer.serialize(batch)
        message = self._internal_protocol.create_packet(
            msg_type=MessageType.DATA,
            client_id_bytes=key.client_id.to_bytes(16, byteorder="big"),
            payload=payload,
        )
        self._line_batch_sender().send(message)
        state.data_lines_emitted += len(batch.lines)

        logging.info(
            "file_splitter_batch | id=%s | client_id=%s | path=%s | batch_id=%s | "
            "first_line_number=%s | lines=%s | bytes=%s | expected_total=%s",
            self._config.id,
            key.client_id,
            key.rel_path,
            batch.batch_id,
            batch.first_line_number,
            len(batch.lines),
            len(message),
            state.data_lines_emitted,
        )

        state.batch_id += 1
        state.batch_lines.clear()
        state.batch_first_line_number = 0
        state.batch_payload_bytes = 0

    def _send_eof(self, client_id: int, expected_total: int) -> None:
        payload = self._control_serializer.serialize(
            ControlMessage(
                sender_id=self._config.id,
                expected_total=expected_total,
                processed_count=0,
            )
        )
        message = self._internal_protocol.create_packet(
            msg_type=MessageType.EOF,
            client_id_bytes=client_id.to_bytes(16, byteorder="big"),
            payload=payload,
        )
        self._line_batch_sender().send(message)

    def _batch_packet_size(
        self,
        key: FileKey,
        state: FileState,
        additional_line_bytes: int = 0,
    ) -> int:
        if state.header is None:
            raise ValueError(
                "missing transaction header "
                f"(client_id={key.client_id}, path={key.rel_path})"
            )
        return (
            InternalProtocol.HEADER_SIZE
            + LineBatchSerializer.FIXED_HEADER_SIZE
            + len(key.rel_path.encode("utf-8"))
            + sum(_string_item_size(column) for column in state.header)
            + state.batch_payload_bytes
            + additional_line_bytes
        )

    def _state_for(self, key: FileKey) -> FileState:
        state = self._files.get(key)
        if state is None:
            state = FileState(splitter=LineSplitter(self._config.max_line_bytes))
            self._files[key] = state
        return state

    def _line_batch_sender(self) -> MessageMiddlewareQueueRabbitMQ:
        if self._line_batch_output is None:
            self._line_batch_output = MessageMiddlewareQueueRabbitMQ(
                self._config.mom_host,
                self._config.output_queue,
            )
        return self._line_batch_output

    def _input_routing_key(self) -> str:
        return self._config.input_routing_key or file_ingestor_routing_key(
            self._config.id
        )


def _parse_header(line: bytes) -> tuple[str, ...]:
    clean_line = line[:-1] if line.endswith(b"\r") else line
    if not clean_line:
        raise ValueError("empty transaction header")
    return tuple(parse_csv_line(clean_line))


def _deserialize_eof(payload: bytes) -> int | FileEof:
    if len(payload) == 4:
        return int.from_bytes(payload, byteorder="big")
    return FileEof.deserialize(payload)


def _line_item_size(line: bytes) -> int:
    return 4 + len(line)


def _string_item_size(value: str) -> int:
    return 4 + len(str(value).encode("utf-8"))
