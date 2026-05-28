from common.message_protocol.external import FileChunk, FileEof
from common.message_protocol.external.types import (
    FILE_TYPE_ACCOUNTS,
    FILE_TYPE_TRANSACTIONS,
)
from common.message_protocol.internal import (
    ControlMessageSerializer,
    InternalProtocol,
    LineBatchSerializer,
    MessageType,
)
import workers.file_splitter.file_splitter as file_splitter_module
from workers.file_splitter.file_splitter import FileSplitter, FileSplitterConfig


HEADER_LINE = (
    b"Timestamp,From Bank,Account,To Bank,Account,Amount Paid,"
    b"Payment Currency,Payment Format"
)
ROW_1 = b"2022/09/01 00:08,1,from-1,2,to-1,12.5,US Dollar,Wire"
ROW_2 = b"2022/09/01 00:09,3,from-2,4,to-2,22.0,US Dollar,ACH"
ROW_3 = b"2022/09/01 00:10,5,from-3,6,to-3,33.0,US Dollar,Cash"


class RecordingSender:
    def __init__(self):
        self.messages = []
        self.closed = False

    def send(self, message: bytes) -> None:
        if self.closed:
            raise RuntimeError("send on closed sender")
        self.messages.append(message)

    def close(self) -> None:
        self.closed = True


def test_file_splitter_emits_line_batches_and_authoritative_eof():
    splitter, sender = _splitter(max_batch_bytes=4096)
    path = "LI-Mini_Trans.csv"

    payload = HEADER_LINE + b"\n" + ROW_1 + b"\npar"
    splitter._handle_chunk(
        FileChunk(
            rel_path=path,
            client_id=7,
            file_type=FILE_TYPE_TRANSACTIONS,
            offset=0,
            data=payload,
        )
    )
    splitter._handle_chunk(
        FileChunk(
            rel_path=path,
            client_id=7,
            file_type=FILE_TYPE_TRANSACTIONS,
            offset=len(payload),
            data=b"tial,4,to-2,22.0,US Dollar,ACH\n",
        )
    )
    splitter._handle_file_eof(
        FileEof(
            rel_path=path,
            client_id=7,
            file_type=FILE_TYPE_TRANSACTIONS,
        )
    )

    assert len(sender.messages) == 2

    batch = _line_batch(sender.messages[0])
    eof = _control(sender.messages[1])

    assert batch.file_type == FILE_TYPE_TRANSACTIONS
    assert batch.rel_path == path
    assert batch.batch_id == 0
    assert batch.first_line_number == 2
    assert batch.header == (
        "Timestamp",
        "From Bank",
        "Account",
        "To Bank",
        "Account",
        "Amount Paid",
        "Payment Currency",
        "Payment Format",
    )
    assert batch.lines == (
        ROW_1,
        b"partial,4,to-2,22.0,US Dollar,ACH",
    )
    assert eof.expected_total == len(batch.lines)


def test_file_splitter_flushes_batches_by_size():
    splitter, sender = _splitter(max_batch_bytes=1)
    path = "LI-Mini_Trans.csv"

    splitter._handle_chunk(
        FileChunk(
            rel_path=path,
            client_id=7,
            file_type=FILE_TYPE_TRANSACTIONS,
            offset=0,
            data=HEADER_LINE + b"\n" + ROW_1 + b"\n" + ROW_2 + b"\n" + ROW_3 + b"\n",
        )
    )
    splitter._handle_file_eof(
        FileEof(
            rel_path=path,
            client_id=7,
            file_type=FILE_TYPE_TRANSACTIONS,
        )
    )

    batches = [_line_batch(message) for message in sender.messages[:-1]]
    eof = _control(sender.messages[-1])

    assert [batch.batch_id for batch in batches] == [0, 1, 2]
    assert [batch.first_line_number for batch in batches] == [2, 3, 4]
    assert [batch.lines for batch in batches] == [(ROW_1,), (ROW_2,), (ROW_3,)]
    assert eof.expected_total == 3


def test_file_splitter_flushes_tail_line_on_file_eof():
    splitter, sender = _splitter(max_batch_bytes=4096)
    path = "LI-Mini_Trans.csv"
    payload = HEADER_LINE + b"\n" + ROW_1

    splitter._handle_chunk(
        FileChunk(
            rel_path=path,
            client_id=7,
            file_type=FILE_TYPE_TRANSACTIONS,
            offset=0,
            data=payload,
        )
    )
    splitter._handle_file_eof(
        FileEof(
            rel_path=path,
            client_id=7,
            file_type=FILE_TYPE_TRANSACTIONS,
        )
    )

    batch = _line_batch(sender.messages[0])
    eof = _control(sender.messages[1])

    assert batch.lines == (ROW_1,)
    assert eof.expected_total == 1


def test_file_splitter_strips_cr_from_header_but_keeps_raw_data_lines():
    splitter, sender = _splitter(max_batch_bytes=4096)
    path = "LI-Mini_Trans.csv"

    splitter._handle_chunk(
        FileChunk(
            rel_path=path,
            client_id=7,
            file_type=FILE_TYPE_TRANSACTIONS,
            offset=0,
            data=HEADER_LINE + b"\r\n" + ROW_1 + b"\r\n",
        )
    )
    splitter._handle_file_eof(
        FileEof(
            rel_path=path,
            client_id=7,
            file_type=FILE_TYPE_TRANSACTIONS,
        )
    )

    batch = _line_batch(sender.messages[0])

    assert batch.header[-1] == "Payment Format"
    assert batch.lines == (ROW_1 + b"\r",)


def test_file_splitter_drops_accounts_files():
    splitter, sender = _splitter(max_batch_bytes=4096)
    path = "LI-Mini_accounts.csv"

    splitter._handle_chunk(
        FileChunk(
            rel_path=path,
            client_id=7,
            file_type=FILE_TYPE_ACCOUNTS,
            offset=0,
            data=b"Bank,Account\n1,abc\n",
        )
    )
    splitter._handle_file_eof(
        FileEof(
            rel_path=path,
            client_id=7,
            file_type=FILE_TYPE_ACCOUNTS,
        )
    )

    assert sender.messages == []


def test_file_splitter_stop_does_not_close_outputs_until_start_unwinds(monkeypatch):
    created = {}
    closed_during_stop = []

    class StopRecordingConsumer:
        def __init__(self, *args, **kwargs):
            self.stop_requests = 0
            self.closed = False
            created["consumer"] = self

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            self.close()
            return False

        def start_consuming(self, callback):
            splitter.stop()
            closed_during_stop.append(sender.closed)

        def request_stop_consuming(self):
            self.stop_requests += 1

        def close(self):
            self.closed = True

    monkeypatch.setattr(
        file_splitter_module,
        "MessageMiddlewareExchangeRabbitMQ",
        StopRecordingConsumer,
    )
    splitter, sender = _splitter(max_batch_bytes=4096)

    splitter.start()

    assert created["consumer"].stop_requests == 1
    assert closed_during_stop == [False]
    assert sender.closed
    assert splitter._line_batch_output is None


def _splitter(max_batch_bytes: int) -> tuple[FileSplitter, RecordingSender]:
    sender = RecordingSender()
    splitter = FileSplitter(
        FileSplitterConfig(
            id=5,
            mom_host="localhost",
            input_exchange="file_ingestor_exchange",
            queue_name="file_splitter_5",
            output_queue="line_batch_queue",
            max_line_bytes=1024,
            max_batch_bytes=max_batch_bytes,
            logging_level="INFO",
        )
    )
    splitter._line_batch_output = sender
    return splitter, sender


def _line_batch(message: bytes):
    msg_type, client_id, payload = InternalProtocol.unpack_packet(message)

    assert msg_type == MessageType.DATA
    assert client_id == 7
    return LineBatchSerializer.deserialize(payload)


def _control(message: bytes):
    msg_type, client_id, payload = InternalProtocol.unpack_packet(message)

    assert msg_type == MessageType.EOF
    assert client_id == 7
    return ControlMessageSerializer.deserialize(payload)
