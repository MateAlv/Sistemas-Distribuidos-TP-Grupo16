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

    ingestor._process_message(_data_packet(9, batch), calls.ack, calls.nack)

    assert calls.acks == 1
    assert calls.nacks == 0
    # Las 2 transactions del LineBatch viajan en un único publish con el
    # payload serializado como batch (serialize_batch).
    assert len(sender.messages) == 1

    msg_type, client_id, payload = InternalProtocol.unpack_packet(sender.messages[0])
    txs = TransactionSerializer.deserialize_batch(payload)

    assert msg_type == MessageType.DATA
    assert client_id == 9
    assert len(txs) == 2
    assert txs[0].date == "2022/09/01 00:08"
    assert txs[0].from_bank == "1"
    assert txs[0].from_account == "abc"
    assert txs[0].to_bank == "2"
    assert txs[0].to_account == "def"
    assert txs[0].amount == 12.5
    assert txs[0].currency == "US Dollar"
    assert txs[0].format == "Wire"
    assert txs[1].date == "2022/09/01 00:09"
    assert txs[1].amount == 20.0
    assert txs[1].format == "ACH"


def test_file_ingestor_broadcasts_eof_on_upstream_eof():
    # The splitter EOF no longer goes straight downstream: the ingestor that
    # grabs it becomes leader and broadcasts EOF_RECEIVED to the pool.
    sender = RecordingSender()
    ingestor = _ingestor(sender)
    control_senders = {
        ingestor._coordinator.control_queue_for(i): RecordingSender()
        for i in range(ingestor._config.total_instances)
    }
    ingestor._main_control_senders = control_senders
    calls = AckNack()
    control_payload = ControlMessageSerializer.serialize(
        ControlMessage(sender_id=5, expected_total=42, processed_count=0)
    )
    message = InternalProtocol.create_packet(
        msg_type=MessageType.EOF,
        client_id_bytes=(9).to_bytes(16, byteorder="big"),
        payload=control_payload,
    )

    ingestor._process_message(message, calls.ack, calls.nack)

    assert calls.acks == 1
    assert calls.nacks == 0
    # Nothing forwarded downstream on EOF arrival.
    assert sender.messages == []
    # Leader recorded the expected total and broadcast EOF_RECEIVED.
    assert ingestor._coordinator._leader_expected[9] == 42
    sent = [message for queue in control_senders.values() for message in queue.messages]
    assert len(sent) == ingestor._config.total_instances
    for message in sent:
        msg_type, client_id, payload = InternalProtocol.unpack_packet(message)
        control = ControlMessageSerializer.deserialize(payload)
        assert msg_type == MessageType.EOF_RECEIVED
        assert client_id == 9
        assert control.sender_id == ingestor._config.id
        assert control.expected_total == 42


def test_file_ingestor_leader_forwards_eof_when_total_reached():
    # Leader forwards one downstream EOF after every non-leader flush ack arrives.
    eof_sender = RecordingSender()
    ingestor = _ingestor(RecordingSender())
    ingestor._coordinator._leader_expected[9] = 4
    ingestor._processed_by_client[9] = 1
    calls = AckNack()

    # First non-leader flush ack: not enough yet.
    ingestor._handle_response(
        _flush_ack_packet(9, sender_id=0, forwarded=2),
        calls.ack,
        calls.nack,
        {},
        eof_sender,
    )
    assert eof_sender.messages == []
    assert calls.acks == 1

    # Second non-leader ack completes the N-1 ack set; leader adds its own count.
    ingestor._handle_response(
        _flush_ack_packet(9, sender_id=2, forwarded=1),
        calls.ack,
        calls.nack,
        {},
        eof_sender,
    )
    assert calls.acks == 2
    assert len(eof_sender.messages) == 1
    msg_type, client_id, payload = InternalProtocol.unpack_packet(eof_sender.messages[0])
    control = ControlMessageSerializer.deserialize(payload)
    assert msg_type == MessageType.EOF
    assert client_id == 9
    assert control.expected_total == 4  # forwarded total
    assert 9 not in ingestor._coordinator._leader_expected


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

    ingestor._process_message(_data_packet(9, batch), calls.ack, calls.nack)

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

    ingestor._process_message(_data_packet(9, batch), calls.ack, calls.nack)

    assert calls.acks == 1
    assert calls.nacks == 0
    assert sender.messages == []


def _ingestor(sender: RecordingSender) -> FileIngestor:
    ingestor = FileIngestor(
        FileIngestorConfig(
            id=1,
            total_instances=3,
            mom_host="localhost",
            queue_name="file_ingestor_1",
            input_exchange="line_batch_exchange",
            input_routing_prefix="file_ingestor",
            transaction_output_exchange="transactions",
            control_queue_prefix="file_ingestor_control",
            response_queue_prefix="file_ingestor_response",
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


def _flush_ack_packet(client_id: int, sender_id: int, forwarded: int) -> bytes:
    return InternalProtocol.create_packet(
        msg_type=MessageType.FLUSH_ACK,
        client_id_bytes=client_id.to_bytes(16, byteorder="big"),
        payload=ControlMessageSerializer.serialize(
            ControlMessage(
                sender_id=sender_id,
                expected_total=0,
                processed_count=forwarded,
            )
        ),
    )
