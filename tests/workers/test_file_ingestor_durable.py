"""In-process crash/recovery for the file ingestor data plane.

A "crash" = drop the FileIngestor instance and build a new one over the same
STATE_DIR (the durable disk survives). These prove the two guarantees:
  - recovered state is exact and a redelivered, already-committed input is
    deduplicated (no double count, no re-publish);
  - an input applied-but-not-committed before the crash is re-published from the
    outbox on recovery and then committed without double-applying.
"""

from common.fault_tolerance.handler import WorkerRunner
from common.fault_tolerance.inbox import InboxStatus, MsgKind
from common.message_protocol.external.types import FILE_TYPE_TRANSACTIONS
from common.message_protocol.internal import (
    ControlMessage,
    ControlMessageSerializer,
    InternalProtocol,
    LineBatch,
    LineBatchSerializer,
    MessageType,
)
from workers.file_ingestor.file_ingestor import (
    FileIngestor,
    FileIngestorConfig,
    FileIngestorOutputConfig,
)

HEADER = (
    "Timestamp", "From Bank", "Account", "To Bank", "Account",
    "Amount Paid", "Payment Currency", "Payment Format",
)
CLIENT = 9
SENDER = 5


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

    def ack(self):
        self.acks += 1

    def nack(self, requeue: bool = False):
        self.nacks += 1


def test_recovers_state_and_dedups_committed_redelivery(tmp_path):
    ing1, _ = _ingestor(tmp_path)
    calls = AckNack()
    packet = _data_packet(seq=0)

    ing1._on_input_message(packet, calls.ack, calls.nack)
    assert ing1._state.processed_count(CLIENT) == 2
    ing1._handler.wal.close()  # release the file handle (a real crash just dies)

    ing2, outputs2 = _ingestor(tmp_path)  # recover over the same STATE_DIR
    assert ing2._state.processed_count(CLIENT) == 2  # state restored from the WAL

    calls2 = AckNack()
    ing2._on_input_message(packet, calls2.ack, calls2.nack)  # redelivery
    assert calls2.acks == 1
    assert ing2._state.processed_count(CLIENT) == 2  # not double counted
    assert all(s.messages == [] for s in outputs2.values())  # already done, no re-publish


def test_recovery_republishes_uncommitted_outbox_then_commits(tmp_path):
    # Apply the input through the handler but never commit (crash between publish
    # and commit): its two downstream outputs are left pending in the outbox.
    ing1, _ = _ingestor(tmp_path)
    payload = LineBatchSerializer.serialize(_two_row_batch())
    ing1._handler.handle(
        f"{SENDER}:0", CLIENT, SENDER, 0, payload,
        lambda data: ing1._data_process_payload(CLIENT, data),
    )
    ing1._handler.wal.close()

    ing2, outputs2 = _ingestor(tmp_path)  # recover_and_republish runs in the helper
    assert ing2._state.processed_count(CLIENT) == 2  # applied change replayed
    for sender in outputs2.values():
        assert len(sender.messages) == 1  # pending outbox re-published once

    # Redelivery of the same input: APPLIED -> re-publish + commit, no double apply.
    calls = AckNack()
    ing2._on_input_message(_data_packet(seq=0), calls.ack, calls.nack)
    assert calls.acks == 1
    assert ing2._state.processed_count(CLIENT) == 2

    # A further redelivery is now a no-op ack (committed/DONE).
    calls2 = AckNack()
    before = {name: len(s.messages) for name, s in outputs2.items()}
    ing2._on_input_message(_data_packet(seq=0), calls2.ack, calls2.nack)
    assert calls2.acks == 1
    assert {name: len(s.messages) for name, s in outputs2.items()} == before


def test_upstream_eof_decision_mutates_coordinator_once_via_apply(tmp_path, monkeypatch):
    ingestor, _ = _ingestor(tmp_path, total_instances=3)
    control_senders = {
        ingestor._coordinator.control_queue_for(i): RecordingSender()
        for i in range(ingestor._config.total_instances)
    }
    ingestor._data_publishers = {**ingestor._downstream_outputs, **control_senders}
    calls = AckNack()
    seen = 0
    original = ingestor._coordinator.on_upstream_eof

    def spy(*args, **kwargs):
        nonlocal seen
        seen += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(ingestor._coordinator, "on_upstream_eof", spy)

    ingestor._on_input_message(_eof_packet(expected_total=42), calls.ack, calls.nack)

    assert calls.acks == 1
    assert calls.nacks == 0
    assert seen == 1
    assert ingestor._coordinator.leader_expected(CLIENT) == 42


def test_control_kind_does_not_collide_with_data_sender_and_seq(tmp_path):
    ingestor, outputs = _ingestor(tmp_path, total_instances=3)
    calls = AckNack()
    data = InternalProtocol.create_addressed_packet(
        MessageType.DATA,
        CLIENT.to_bytes(16, byteorder="big"),
        sender_id=1,
        seq=CLIENT,
        payload=LineBatchSerializer.serialize(_two_row_batch()),
    )
    ingestor._on_input_message(data, calls.ack, calls.nack)
    assert calls.acks == 1
    assert all(len(sender.messages) == 1 for sender in outputs.values())

    response_senders = {ingestor._coordinator.response_queue_for(1): RecordingSender()}
    ingestor._handle_flush_order(
        _flush_order_packet(leader_id=1),
        CLIENT,
        ControlMessage(sender_id=1, expected_total=0, processed_count=0),
        calls.ack,
        calls.nack,
        response_senders,
    )

    assert calls.acks == 2
    assert calls.nacks == 0
    assert len(response_senders[ingestor._coordinator.response_queue_for(1)].messages) == 1
    assert (
        ingestor._handler.inbox.classify(CLIENT, 1, CLIENT, MsgKind.DATA)
        is InboxStatus.DONE
    )
    assert (
        ingestor._handler.inbox.classify(CLIENT, 1, CLIENT, MsgKind.CTRL_FLUSH_ORDER)
        is InboxStatus.DONE
    )


def test_late_data_after_close_is_noop(tmp_path):
    ingestor, outputs = _ingestor(tmp_path)
    ingestor._state.apply_change({"type": "close", "client_id": CLIENT})
    calls = AckNack()

    ingestor._on_input_message(_data_packet(seq=0), calls.ack, calls.nack)

    assert calls.acks == 1
    assert calls.nacks == 0
    assert ingestor._state.processed_count(CLIENT) == 0
    assert all(sender.messages == [] for sender in outputs.values())


def test_flush_ack_applied_crash_recovers_and_commits_closed_redelivery(tmp_path):
    ing1, _ = _ingestor(tmp_path, total_instances=3)
    ing1._state.apply_change({"type": "data", "client_id": CLIENT, "transactions_forwarded": 1})
    ing1._coordinator._leader_expected[CLIENT] = 4
    ing1._coordinator._flush_acks[CLIENT] = {0}
    ing1._coordinator._forwarded_from_acks[CLIENT] = 2
    calls = AckNack()

    def crash_before_publish(_entries, _publishers):
        raise RuntimeError("crash before publish")

    ing1._publish_outputs = crash_before_publish
    ing1._handle_flush_ack(
        _flush_ack_packet(sender_id=2, forwarded=1),
        CLIENT,
        ControlMessage(sender_id=2, expected_total=0, processed_count=1),
        calls.ack,
        calls.nack,
        ing1._downstream_outputs,
    )

    assert calls.acks == 0
    assert calls.nacks == 1
    assert ing1._state.is_closed(CLIENT)
    ing1._handler.wal.close()

    ing2, outputs2 = _ingestor(tmp_path, total_instances=3)
    assert ing2._state.is_closed(CLIENT)
    assert all(len(sender.messages) == 1 for sender in outputs2.values())

    redelivery = AckNack()
    ing2._handle_flush_ack(
        _flush_ack_packet(sender_id=2, forwarded=1),
        CLIENT,
        ControlMessage(sender_id=2, expected_total=0, processed_count=1),
        redelivery.ack,
        redelivery.nack,
        ing2._downstream_outputs,
    )

    assert redelivery.acks == 1
    assert redelivery.nacks == 0
    assert (
        ing2._handler.inbox.classify(CLIENT, 2, CLIENT, MsgKind.CTRL_FLUSH_ACK)
        is InboxStatus.DONE
    )
    assert ing2._handler.outbox_to_republish() == []


def test_flush_ack_crash_after_done_before_ack_dedups_redelivery(tmp_path):
    ing1, outputs1 = _ingestor(tmp_path, total_instances=3)
    ing1._state.apply_change({"type": "data", "client_id": CLIENT, "transactions_forwarded": 1})
    ing1._coordinator._leader_expected[CLIENT] = 4
    ing1._coordinator._flush_acks[CLIENT] = {0}
    ing1._coordinator._forwarded_from_acks[CLIENT] = 2
    calls = AckNack()

    def crash_ack():
        raise RuntimeError("crash before ack")

    ing1._handle_flush_ack(
        _flush_ack_packet(sender_id=2, forwarded=1),
        CLIENT,
        ControlMessage(sender_id=2, expected_total=0, processed_count=1),
        crash_ack,
        calls.nack,
        outputs1,
    )

    assert calls.nacks == 1
    assert all(len(sender.messages) == 1 for sender in outputs1.values())
    ing1._handler.wal.close()

    ing2, outputs2 = _ingestor(tmp_path, total_instances=3)
    redelivery = AckNack()
    ing2._handle_flush_ack(
        _flush_ack_packet(sender_id=2, forwarded=1),
        CLIENT,
        ControlMessage(sender_id=2, expected_total=0, processed_count=1),
        redelivery.ack,
        redelivery.nack,
        outputs2,
    )

    assert redelivery.acks == 1
    assert redelivery.nacks == 0
    assert all(sender.messages == [] for sender in outputs2.values())


def _ingestor(tmp_path, total_instances=1):
    output_configs = (
        FileIngestorOutputConfig(
            name="filter_usd", exchange="filter_usd_exchange",
            routing_prefix="filter_usd", shard_count=2,
        ),
        FileIngestorOutputConfig(
            name="filter_q5_format", exchange="filter_q5_format_exchange",
            routing_prefix="filter_q5_format", shard_count=3,
        ),
    )
    ingestor = FileIngestor(
        FileIngestorConfig(
            id=1,
            total_instances=total_instances,
            mom_host="localhost",
            queue_name="file_ingestor_1",
            input_exchange="line_batch_exchange",
            input_routing_prefix="file_ingestor",
            outputs=output_configs,
            control_queue_prefix="file_ingestor_control",
            response_queue_prefix="file_ingestor_response",
            logging_level="INFO",
            state_dir=str(tmp_path),
        )
    )
    publishers = {
        output.name: RecordingSender() for output in output_configs
    }
    ingestor._downstream_outputs = publishers
    ingestor._data_publishers = dict(publishers)
    ingestor._runner = WorkerRunner(
        handler=ingestor._handler,
        publishers=publishers,
        process_payload=ingestor._data_process_payload,
        lock=ingestor._lock,
    )
    ingestor._runner.recover_and_republish()
    return ingestor, publishers


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


def _data_packet(seq: int) -> bytes:
    return InternalProtocol.create_addressed_packet(
        MessageType.DATA,
        CLIENT.to_bytes(16, byteorder="big"),
        SENDER,
        seq,
        LineBatchSerializer.serialize(_two_row_batch()),
    )


def _eof_packet(expected_total: int, seq: int = 0) -> bytes:
    return InternalProtocol.create_addressed_packet(
        MessageType.EOF,
        CLIENT.to_bytes(16, byteorder="big"),
        SENDER,
        seq,
        ControlMessageSerializer.serialize(
            ControlMessage(sender_id=SENDER, expected_total=expected_total, processed_count=0)
        ),
    )


def _flush_order_packet(leader_id: int) -> bytes:
    return InternalProtocol.create_packet(
        MessageType.FLUSH_ORDER,
        CLIENT.to_bytes(16, byteorder="big"),
        ControlMessageSerializer.serialize(
            ControlMessage(sender_id=leader_id, expected_total=0, processed_count=0)
        ),
    )


def _flush_ack_packet(sender_id: int, forwarded: int) -> bytes:
    return InternalProtocol.create_packet(
        MessageType.FLUSH_ACK,
        CLIENT.to_bytes(16, byteorder="big"),
        ControlMessageSerializer.serialize(
            ControlMessage(sender_id=sender_id, expected_total=0, processed_count=forwarded)
        ),
    )
