import csv
import logging
from dataclasses import dataclass

from common.domain.transaction import Transaction
from common.message_protocol.external import FileChunk, FileEof
from common.message_protocol.external.types import (
    FILE_TYPE_ACCOUNTS,
    FILE_TYPE_TRANSACTIONS,
    MSG_CHUNK,
    MSG_EOF,
    file_type_name,
    file_ingestor_routing_key,
)
from common.message_protocol.internal.common import ControlMessage, MessageType
from common.message_protocol.internal.control_message_serializer import ControlMessageSerializer
from common.message_protocol.internal import InternalProtocol
from common.message_protocol.internal.transaction_serializer import TransactionSerializer
from common.middleware import MessageMiddlewareQueueRabbitMQ
from common.middleware.middleware_rabbitmq import MessageMiddlewareExchangeRabbitMQ


@dataclass(frozen=True)
class FileIngestorConfig:
    id: int
    mom_host: str
    file_ingestor_exchange: str
    queue_name: str
    transaction_output_queue: str
    max_line_bytes: int
    logging_level: str


@dataclass(frozen=True)
class FileKey:
    client_id: int
    rel_path: str


@dataclass
class FileState:
    pending: bytes = b""
    file_type: int | None = None
    header: tuple[str, ...] | None = None
    expected_offset: int = 0
    chunks_received: int = 0
    lines_received: int = 0
    transactions_sent: int = 0
    bytes_received: int = 0


class FileIngestor:
    def __init__(self, config: FileIngestorConfig) -> None:
        self._config = config
        self._stopped = False
        self._consumer: MessageMiddlewareExchangeRabbitMQ | None = None
        self._chunks_received = 0
        self._eofs_received = 0
        self._files: dict[FileKey, FileState] = {}
        self._transaction_output: MessageMiddlewareQueueRabbitMQ | None = None
        self._transaction_serializer = TransactionSerializer()
        self._control_serializer = ControlMessageSerializer()
        self._internal_protocol = InternalProtocol()

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
        if self._transaction_output is not None:
            self._transaction_output.close()

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
                eof = _deserialize_eof(payload)
                if isinstance(eof, FileEof):
                    self._handle_file_eof(eof)
                else:
                    self._handle_eof(eof)
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

        if state.file_type is None:
            state.file_type = chunk.file_type()
        elif state.file_type != chunk.file_type():
            raise ValueError(
                "file_type changed for file "
                f"(client_id={key.client_id}, path={key.rel_path}, "
                f"expected={state.file_type}, received={chunk.file_type()})"
            )

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
            "file_ingestor_chunk | id=%s | client_id=%s | file_type=%s | path=%s | "
            "offset=%s | payload_bytes=%s | complete_lines=%s | pending_bytes=%s | "
            "file_chunks=%s | chunks_received=%s",
            self._config.id,
            key.client_id,
            file_type_name(state.file_type),
            key.rel_path,
            chunk.offset(),
            chunk.payload_size(),
            len(complete_lines),
            len(state.pending),
            state.chunks_received,
            self._chunks_received,
        )

    def _handle_file_eof(self, eof: FileEof) -> None:
        key = FileKey(client_id=eof.client_id(), rel_path=eof.path())
        file_finished, saw_transactions_file, transactions_sent = self._finish_file(
            key,
            expected_file_type=eof.file_type(),
        )

        if saw_transactions_file:
            self._send_eof(eof.client_id(), transactions_sent)

        self._eofs_received += 1
        logging.info(
            "file_ingestor_eof | id=%s | client_id=%s | file_type=%s | path=%s | "
            "files_finished=%s | transactions_file=%s | transactions_sent=%s | "
            "eofs_received=%s",
            self._config.id,
            eof.client_id(),
            file_type_name(eof.file_type()),
            eof.path(),
            1 if file_finished else 0,
            saw_transactions_file,
            transactions_sent,
            self._eofs_received,
        )

    def _handle_eof(self, client_id: int) -> None:
        keys = [key for key in self._files if key.client_id == client_id]
        transactions_sent = 0
        saw_transactions_file = False

        for key in keys:
            _, file_saw_transactions, file_transactions_sent = self._finish_file(key)
            saw_transactions_file = saw_transactions_file or file_saw_transactions
            transactions_sent += file_transactions_sent

        if saw_transactions_file:
            self._send_eof(client_id, transactions_sent)

        self._eofs_received += 1
        logging.info(
            "file_ingestor_eof | id=%s | client_id=%s | files_finished=%s | "
            "transactions_file=%s | transactions_sent=%s | eofs_received=%s",
            self._config.id,
            client_id,
            len(keys),
            saw_transactions_file,
            transactions_sent,
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
                "file_ingestor_file_eof_missing | id=%s | client_id=%s | path=%s",
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

        if state.pending:
            self._handle_line(key, state, state.pending)
            state.pending = b""

        saw_transactions_file = state.file_type == FILE_TYPE_TRANSACTIONS
        transactions_sent = state.transactions_sent if saw_transactions_file else 0

        logging.info(
            "file_ingestor_file_finished | id=%s | client_id=%s | file_type=%s | "
            "path=%s | chunks=%s | lines=%s | transactions_sent=%s | bytes=%s",
            self._config.id,
            key.client_id,
            file_type_name(state.file_type),
            key.rel_path,
            state.chunks_received,
            state.lines_received,
            state.transactions_sent,
            state.bytes_received,
        )
        del self._files[key]
        return True, saw_transactions_file, transactions_sent

    def _handle_line(self, key: FileKey, state: FileState, line: bytes) -> None:
        clean_line = line[:-1] if line.endswith(b"\r") else line
        if not clean_line:
            return

        _validate_line_size(clean_line, self._config.max_line_bytes)
        state.lines_received += 1

        if state.file_type == FILE_TYPE_TRANSACTIONS:
            self._handle_transaction_line(key, state, clean_line)
            return

        if state.file_type == FILE_TYPE_ACCOUNTS:
            logging.debug(
                "file_ingestor_line | id=%s | client_id=%s | file_type=%s | "
                "path=%s | line_bytes=%s | file_lines=%s",
                self._config.id,
                key.client_id,
                file_type_name(state.file_type),
                key.rel_path,
                len(clean_line),
                state.lines_received,
            )
            return

        raise ValueError(
            "unknown file_type for file "
            f"(client_id={key.client_id}, path={key.rel_path}, "
            f"file_type={state.file_type})"
        )

    def _handle_transaction_line(
        self,
        key: FileKey,
        state: FileState,
        line: bytes,
    ) -> None:
        fields = _parse_csv_line(line)

        if state.header is None:
            state.header = tuple(fields)
            logging.debug(
                "file_ingestor_transaction_header | id=%s | client_id=%s | path=%s | "
                "columns=%s",
                self._config.id,
                key.client_id,
                key.rel_path,
                len(fields),
            )
            return

        transaction = _parse_transaction(state.header, fields)
        self._send_transaction(key.client_id, transaction)
        state.transactions_sent += 1

    def _send_transaction(self, client_id: int, transaction: Transaction) -> None:
        payload = self._transaction_serializer.serialize(transaction)
        message = self._internal_protocol.create_packet(
            msg_type=MessageType.DATA,
            client_id_bytes=client_id.to_bytes(16, byteorder="big"),
            payload=payload,
        )
        self._transaction_sender().send(message)

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
        self._transaction_sender().send(message)

    def _transaction_sender(self) -> MessageMiddlewareQueueRabbitMQ:
        if self._transaction_output is None:
            self._transaction_output = MessageMiddlewareQueueRabbitMQ(
                self._config.mom_host,
                self._config.transaction_output_queue,
            )
        return self._transaction_output


def _deserialize_eof(payload: bytes) -> int | FileEof:
    if len(payload) == 4:
        return int.from_bytes(payload, byteorder="big")
    return FileEof.deserialize(payload)


def _split_complete_lines(data: bytes, max_line_bytes: int) -> tuple[list[bytes], bytes]:
    lines = data.split(b"\n")
    pending = lines[-1]
    _validate_line_size(pending, max_line_bytes)
    return lines[:-1], pending


def _validate_line_size(line: bytes, max_line_bytes: int) -> None:
    if len(line) > max_line_bytes:
        raise ValueError(f"line exceeded max_line_bytes={max_line_bytes}")


def _parse_csv_line(line: bytes) -> list[str]:
    rows = list(csv.reader([line.decode("utf-8")]))
    if len(rows) != 1:
        raise ValueError("expected exactly one CSV row")
    return rows[0]


def _parse_transaction(header: tuple[str, ...], fields: list[str]) -> Transaction:
    if len(fields) != len(header):
        raise ValueError(
            f"transaction row has {len(fields)} fields, expected {len(header)}"
        )

    from_bank_idx = _column_index(header, "From Bank")
    to_bank_idx = _column_index(header, "To Bank")
    return Transaction(
        date=_required(fields, _column_index(header, "Timestamp")),
        from_bank=_required(fields, from_bank_idx),
        from_account=_required(
            fields,
            _column_index_after(header, "Account", from_bank_idx),
        ),
        to_bank=_required(fields, to_bank_idx),
        to_account=_required(
            fields,
            _column_index_after(header, "Account", to_bank_idx),
        ),
        amount=float(_required(fields, _column_index(header, "Amount Paid"))),
        currency=_required(fields, _column_index(header, "Payment Currency")),
        format=_required(fields, _column_index(header, "Payment Format")),
    )


def _column_index(header: tuple[str, ...], name: str) -> int:
    try:
        return header.index(name)
    except ValueError as exc:
        raise ValueError(f"missing required transaction column: {name}") from exc


def _column_index_after(header: tuple[str, ...], name: str, start_index: int) -> int:
    for index in range(start_index + 1, len(header)):
        if header[index] == name:
            return index
    raise ValueError(f"missing required transaction column after {start_index}: {name}")


def _required(fields: list[str], index: int) -> str:
    value = fields[index].strip()
    if not value:
        raise ValueError(f"empty required transaction field at index {index}")
    return value
