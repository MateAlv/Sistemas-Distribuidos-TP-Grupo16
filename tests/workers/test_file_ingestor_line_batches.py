from common.fault_tolerance.handler import WorkerRunner
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
    FileIngestorOutputConfig,
)
from workers.file_ingestor.file_ingestor_state import FileIngestorState


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

    def send_to_shard(self, message: bytes, shard: int) -> None:
        self.messages.append(message)

    def close(self) -> None:
        pass


class AckNack:
    def __init__(self):
        self.acks = 0
        self.nacks = 0

    def ack(self) -> None:
        self.acks += 1

    def nack(self, requeue: bool = False) -> None:
        self.nacks += 1


def test_file_ingestor_emits_transactions_from_line_batch(tmp_path):
    outputs = _outputs()
    ingestor = _ingestor(outputs, tmp_path)
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

    ingestor._on_input_message(_data_packet(9, batch), calls.ack, calls.nack)

    assert calls.acks == 1
    assert calls.nacks == 0
    # Both downstream outputs get one publish with the serialized transaction batch.
    for sender in outputs.values():
        assert len(sender.messages) == 1

    msg_type, client_id, sender_id, seq, payload = InternalProtocol.unpack_addressed_packet(
        outputs["filter_usd"].messages[0]
    )
    txs = TransactionSerializer.deserialize_batch(payload)

    assert msg_type == MessageType.DATA
    assert client_id == 9
    assert sender_id == 1
    assert seq == 0
    assert len(txs) == 2
    assert txs[0].date == "2022/09/01 00:08"
    assert txs[0].amount == 12.5
    assert txs[1].format == "ACH"
    assert outputs["filter_q5_format"].messages[0] == outputs["filter_usd"].messages[0]
    # State recorded the forwarded count for the EOF coordinator.
    assert ingestor._state.processed_count(9) == 2


def test_file_ingestor_dedups_redelivered_batch(tmp_path):
    outputs = _outputs()
    ingestor = _ingestor(outputs, tmp_path)
    calls = AckNack()
    packet = _data_packet(9, _two_row_batch(), seq=0)

    ingestor._on_input_message(packet, calls.ack, calls.nack)
    ingestor._on_input_message(packet, calls.ack, calls.nack)  # redelivery

    assert calls.acks == 2
    assert calls.nacks == 0
    # Applied once: outputs and the forwarded count are not doubled.
    for sender in outputs.values():
        assert len(sender.messages) == 1
    assert ingestor._state.processed_count(9) == 2


def test_file_ingestor_broadcasts_eof_on_upstream_eof(tmp_path):
    outputs = _outputs()
    ingestor = _ingestor(outputs, tmp_path)
    control_senders = {
        ingestor._coordinator.control_queue_for(i): RecordingSender()
        for i in range(ingestor._config.total_instances)
    }
    ingestor._main_control_senders = control_senders
    # EOF outputs (the broadcast) ride the outbox, published via _data_publishers.
    ingestor._data_publishers = {**outputs, **control_senders}
    calls = AckNack()

    ingestor._on_input_message(_eof_packet(9, expected_total=42), calls.ack, calls.nack)

    assert calls.acks == 1
    assert calls.nacks == 0
    assert all(sender.messages == [] for sender in outputs.values())
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


def test_file_ingestor_leader_forwards_eof_when_total_reached(tmp_path):
    eof_senders = _outputs()
    ingestor = _ingestor(_outputs(), tmp_path)
    ingestor._coordinator._leader_expected[9] = 4
    ingestor._state.apply_change(FileIngestorState.data_change(9, 1))
    calls = AckNack()

    # First non-leader flush ack: not enough yet.
    ingestor._handle_response(
        _flush_ack_packet(9, sender_id=0, forwarded=2),
        calls.ack, calls.nack, {}, eof_senders,
    )
    assert all(sender.messages == [] for sender in eof_senders.values())
    assert calls.acks == 1

    # Second non-leader ack completes N-1; leader adds its own count and forwards.
    ingestor._handle_response(
        _flush_ack_packet(9, sender_id=2, forwarded=1),
        calls.ack, calls.nack, {}, eof_senders,
    )
    assert calls.acks == 2
    for sender in eof_senders.values():
        assert len(sender.messages) == 1
    msg_type, client_id, sender_id, seq, payload = InternalProtocol.unpack_addressed_packet(
        eof_senders["filter_usd"].messages[0]
    )
    control = ControlMessageSerializer.deserialize(payload)
    assert msg_type == MessageType.EOF
    assert client_id == 9
    assert sender_id == 1
    assert seq == 0
    assert control.expected_total == 4
    assert 9 not in ingestor._coordinator._leader_expected
    assert ingestor._state.processed_count(9) == 0  # dropped on flush


def test_file_ingestor_nacks_invalid_batch_without_output(tmp_path):
    outputs = _outputs()
    ingestor = _ingestor(outputs, tmp_path)
    calls = AckNack()
    batch = LineBatch(
        file_type=FILE_TYPE_TRANSACTIONS,
        rel_path="LI-Mini_Trans.csv",
        batch_id=8,
        first_line_number=2,
        header=HEADER,
        lines=(b"",),
    )

    ingestor._on_input_message(_data_packet(9, batch), calls.ack, calls.nack)

    assert calls.acks == 0
    assert calls.nacks == 1
    assert all(sender.messages == [] for sender in outputs.values())


def test_file_ingestor_ignores_accounts_batches(tmp_path):
    outputs = _outputs()
    ingestor = _ingestor(outputs, tmp_path)
    calls = AckNack()
    batch = LineBatch(
        file_type=FILE_TYPE_ACCOUNTS,
        rel_path="LI-Mini_accounts.csv",
        batch_id=1,
        first_line_number=1,
        header=(),
        lines=(b"Bank,Account",),
    )

    ingestor._on_input_message(_data_packet(9, batch), calls.ack, calls.nack)

    assert calls.acks == 1
    assert calls.nacks == 0
    assert all(sender.messages == [] for sender in outputs.values())


def test_file_ingestor_predeclares_downstream_filter_bindings(tmp_path, monkeypatch):
    calls = []
    ingestor = _ingestor(_outputs(), tmp_path)

    monkeypatch.setattr(
        "workers.file_ingestor.file_ingestor.ensure_exchange_queue_bindings",
        lambda *args: calls.append(args),
    )

    ingestor._ensure_output_bindings()

    assert calls == [
        (
            "localhost",
            "filter_usd_exchange",
            {"filter_usd_0": "filter_usd_0", "filter_usd_1": "filter_usd_1"},
        ),
        (
            "localhost",
            "filter_q5_format_exchange",
            {
                "filter_q5_format_0": "filter_q5_format_0",
                "filter_q5_format_1": "filter_q5_format_1",
                "filter_q5_format_2": "filter_q5_format_2",
            },
        ),
    ]


def _outputs() -> dict[str, RecordingSender]:
    return {"filter_usd": RecordingSender(), "filter_q5_format": RecordingSender()}


def _ingestor(outputs: dict[str, RecordingSender], tmp_path) -> FileIngestor:
    ingestor = FileIngestor(
        FileIngestorConfig(
            id=1,
            total_instances=3,
            mom_host="localhost",
            queue_name="file_ingestor_1",
            input_exchange="line_batch_exchange",
            input_routing_prefix="file_ingestor",
            outputs=(
                FileIngestorOutputConfig(
                    name="filter_usd",
                    exchange="filter_usd_exchange",
                    routing_prefix="filter_usd",
                    shard_count=2,
                ),
                FileIngestorOutputConfig(
                    name="filter_q5_format",
                    exchange="filter_q5_format_exchange",
                    routing_prefix="filter_q5_format",
                    shard_count=3,
                ),
            ),
            control_queue_prefix="file_ingestor_control",
            response_queue_prefix="file_ingestor_response",
            logging_level="INFO",
            state_dir=str(tmp_path),
        )
    )
    ingestor._downstream_outputs = outputs
    ingestor._data_publishers = dict(outputs)
    ingestor._runner = WorkerRunner(
        handler=ingestor._handler,
        publishers=ingestor._data_publishers,
        process_payload=ingestor._data_process_payload,
        lock=ingestor._lock,
    )
    ingestor._runner.recover_and_republish()
    return ingestor


def _two_row_batch() -> LineBatch:
    return LineBatch(
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


def _data_packet(client_id: int, batch: LineBatch, sender_id: int = 5, seq: int = 0) -> bytes:
    return InternalProtocol.create_addressed_packet(
        MessageType.DATA,
        client_id.to_bytes(16, byteorder="big"),
        sender_id,
        seq,
        LineBatchSerializer.serialize(batch),
    )


def _eof_packet(client_id: int, expected_total: int, sender_id: int = 5, seq: int = 0) -> bytes:
    return InternalProtocol.create_addressed_packet(
        MessageType.EOF,
        client_id.to_bytes(16, byteorder="big"),
        sender_id,
        seq,
        ControlMessageSerializer.serialize(
            ControlMessage(sender_id=sender_id, expected_total=expected_total, processed_count=0)
        ),
    )


def _flush_ack_packet(client_id: int, sender_id: int, forwarded: int) -> bytes:
    return InternalProtocol.create_packet(
        msg_type=MessageType.FLUSH_ACK,
        client_id_bytes=client_id.to_bytes(16, byteorder="big"),
        payload=ControlMessageSerializer.serialize(
            ControlMessage(sender_id=sender_id, expected_total=0, processed_count=forwarded)
        ),
    )
