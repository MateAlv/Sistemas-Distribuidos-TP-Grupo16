"""In-process crash/recovery for the file ingestor data plane.

A "crash" = drop the FileIngestor instance and build a new one over the same
STATE_DIR (the durable disk survives). These prove the two guarantees:
  - recovered state is exact and a redelivered, already-committed input is
    deduplicated (no double count, no re-publish);
  - an input applied-but-not-committed before the crash is re-published from the
    outbox on recovery and then committed without double-applying.
"""

from common.fault_tolerance.handler import WorkerRunner
from common.message_protocol.external.types import FILE_TYPE_TRANSACTIONS
from common.message_protocol.internal import (
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


def _ingestor(tmp_path):
    outputs = {"filter_usd": RecordingSender(), "filter_q5_format": RecordingSender()}
    ingestor = FileIngestor(
        FileIngestorConfig(
            id=1,
            total_instances=1,
            mom_host="localhost",
            queue_name="file_ingestor_1",
            input_exchange="line_batch_exchange",
            input_routing_prefix="file_ingestor",
            outputs=(
                FileIngestorOutputConfig(
                    name="filter_usd", exchange="filter_usd_exchange",
                    routing_prefix="filter_usd", shard_count=2,
                ),
                FileIngestorOutputConfig(
                    name="filter_q5_format", exchange="filter_q5_format_exchange",
                    routing_prefix="filter_q5_format", shard_count=3,
                ),
            ),
            control_queue_prefix="file_ingestor_control",
            response_queue_prefix="file_ingestor_response",
            logging_level="INFO",
            state_dir=str(tmp_path),
        )
    )
    ingestor._downstream_outputs = outputs
    ingestor._runner = WorkerRunner(
        handler=ingestor._handler,
        publishers=outputs,
        process_payload=ingestor._data_process_payload,
        lock=ingestor._lock,
    )
    ingestor._runner.recover_and_republish()
    return ingestor, outputs


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
