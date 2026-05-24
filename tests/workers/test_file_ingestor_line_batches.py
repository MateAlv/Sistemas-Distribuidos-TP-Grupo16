from common.message_protocol.external.types import (
    FILE_TYPE_ACCOUNTS,
    FILE_TYPE_TRANSACTIONS,
)
from common.message_protocol.internal import (
    ControlMessage,
    ControlMessageSerializer,
    InternalProtocol,
    LineBatch,
    LineBatchSerializer,
    MessageType,
    TransactionSerializer,
)
from workers.file_ingestor.file_ingestor import (
    FileIngestor,
    FileIngestorConfig,
)


HEADER = (
    "Timestamp",
    "From Bank",
    "Account",
    "To Bank",
    "Account",
    "Amount Paid",
    "Payment Currency",
    "Payment Format",
)


class RecordingSender:
    def __init__(self):
        self.messages = []

    def send(self, message: bytes) -> None:
        self.messages.append(message)

    def close(self) -> None:
        pass


class AckNack:
    def __init__(self):
        self.acks = 0
        self.nacks = 0

    def ack(self) -> None:
        self.acks += 1

    def nack(self) -> None:
        self.nacks += 1


def test_file_ingestor_emits_transactions_from_line_batch():
    sender = RecordingSender()
    ingestor = _ingestor(sender)
    calls = AckNack()

    batch = LineBatch(
        file_type=FILE_TYPE_TRANSACTIONS,
        rel_path="LI-Mini_Trans.csv",
        batch_id=7,
        first_line_number=2,
        header=HEADER,
        lines=(
            b"2022/09/01 00:08,1,abc,2,def,12.5,US Dollar,Wire",
            b"2022/09/01 00:09,3,ghi,4,jkl,20.0,US Dollar,ACH",
        ),
    )

    ingestor._on_message(_data_packet(9, batch), calls.ack, calls.nack)

    assert calls.acks == 1
    assert calls.nacks == 0
    assert len(sender.messages) == 2

    msg_type, client_id, payload = InternalProtocol.unpack_packet(sender.messages[0])
    transaction = TransactionSerializer.deserialize(payload)

    assert msg_type == MessageType.DATA
    assert client_id == 9
    assert transaction.date == "2022/09/01 00:08"
    assert transaction.from_bank == "1"
    assert transaction.from_account == "abc"
    assert transaction.to_bank == "2"
    assert transaction.to_account == "def"
    assert transaction.amount == 12.5
    assert transaction.currency == "US Dollar"
    assert transaction.format == "Wire"


def test_file_ingestor_forwards_eof_control():
    sender = RecordingSender()
    ingestor = _ingestor(sender)
    calls = AckNack()
    control_payload = ControlMessageSerializer.serialize(
        ControlMessage(sender_id=5, expected_total=42, processed_count=0)
    )
    message = InternalProtocol.create_packet(
        msg_type=MessageType.EOF,
        client_id_bytes=(9).to_bytes(16, byteorder="big"),
        payload=control_payload,
    )

    ingestor._on_message(message, calls.ack, calls.nack)

    assert calls.acks == 1
    assert calls.nacks == 0
    assert sender.messages == [message]


def test_file_ingestor_nacks_invalid_batch_without_output():
    sender = RecordingSender()
    ingestor = _ingestor(sender)
    calls = AckNack()
    batch = LineBatch(
        file_type=FILE_TYPE_TRANSACTIONS,
        rel_path="LI-Mini_Trans.csv",
        batch_id=8,
        first_line_number=2,
        header=HEADER,
        lines=(b"",),
    )

    ingestor._on_message(_data_packet(9, batch), calls.ack, calls.nack)

    assert calls.acks == 0
    assert calls.nacks == 1
    assert sender.messages == []


def test_file_ingestor_ignores_accounts_batches():
    sender = RecordingSender()
    ingestor = _ingestor(sender)
    calls = AckNack()
    batch = LineBatch(
        file_type=FILE_TYPE_ACCOUNTS,
        rel_path="LI-Mini_accounts.csv",
        batch_id=1,
        first_line_number=1,
        header=(),
        lines=(b"Bank,Account",),
    )

    ingestor._on_message(_data_packet(9, batch), calls.ack, calls.nack)

    assert calls.acks == 1
    assert calls.nacks == 0
    assert sender.messages == []


def _ingestor(sender: RecordingSender) -> FileIngestor:
    ingestor = FileIngestor(
        FileIngestorConfig(
            id=3,
            mom_host="localhost",
            queue_name="line_batch_queue",
            transaction_output_queue="transactions",
            logging_level="INFO",
        )
    )
    ingestor._transaction_output = sender
    return ingestor


def _data_packet(client_id: int, batch: LineBatch) -> bytes:
    return InternalProtocol.create_packet(
        msg_type=MessageType.DATA,
        client_id_bytes=client_id.to_bytes(16, byteorder="big"),
        payload=LineBatchSerializer.serialize(batch),
    )
