from common.message_protocol.external import FileChunk, FileEof
from common.message_protocol.external.types import (
    FILE_TYPE_ACCOUNTS,
    FILE_TYPE_TRANSACTIONS,
)
from common.message_protocol.internal import InternalProtocol
from common.message_protocol.internal.common import MessageType
from common.message_protocol.internal.control_message_serializer import (
    ControlMessageSerializer,
)
from workers.file_ingestor.file_ingestor import FileIngestor, FileIngestorConfig


class RecordingSender:
    def __init__(self):
        self.messages = []

    def send(self, message: bytes) -> None:
        self.messages.append(message)

    def close(self) -> None:
        pass


def test_transaction_file_eof_flushes_without_waiting_for_accounts():
    sender = RecordingSender()
    ingestor = FileIngestor(
        FileIngestorConfig(
            id=3,
            mom_host="localhost",
            file_ingestor_exchange="file_ingestor",
            queue_name="file_ingestor_3",
            transaction_output_queue="transactions",
            max_line_bytes=1024,
            logging_level="INFO",
        )
    )
    ingestor._transaction_output = sender

    ingestor._handle_chunk(
        FileChunk(
            rel_path="LI-Small_Trans.csv",
            client_id=9,
            file_type=FILE_TYPE_TRANSACTIONS,
            offset=0,
            data=(
                b"Timestamp,From Bank,Account,To Bank,Account,Amount Paid,"
                b"Payment Currency,Payment Format\n"
                b"2022/09/01 00:08,1,abc,2,def,12.5,US Dollar,Wire\n"
            ),
        )
    )
    ingestor._handle_file_eof(
        FileEof(
            rel_path="LI-Small_Trans.csv",
            client_id=9,
            file_type=FILE_TYPE_TRANSACTIONS,
        )
    )

    ingestor._handle_chunk(
        FileChunk(
            rel_path="LI-Small_accounts.csv",
            client_id=9,
            file_type=FILE_TYPE_ACCOUNTS,
            offset=0,
            data=b"Bank,Account\n1,abc\n",
        )
    )
    ingestor._handle_file_eof(
        FileEof(
            rel_path="LI-Small_accounts.csv",
            client_id=9,
            file_type=FILE_TYPE_ACCOUNTS,
        )
    )

    assert len(sender.messages) == 2

    protocol = InternalProtocol()
    data_type, data_client_id, _ = protocol.unpack_packet(sender.messages[0])
    eof_type, eof_client_id, eof_payload = protocol.unpack_packet(sender.messages[1])
    eof_control = ControlMessageSerializer.deserialize(eof_payload)

    assert data_type == MessageType.DATA
    assert data_client_id == 9
    assert eof_type == MessageType.EOF
    assert eof_client_id == 9
    assert eof_control.sender_id == 3
    assert eof_control.expected_total == 1
