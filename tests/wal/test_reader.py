import struct
from pathlib import Path

import pytest

from common.fault_tolerance.outbox.outbox_entry import OutboxEntry
from common.fault_tolerance.wal.input_applied import InputApplied
from common.fault_tolerance.wal.input_done import InputDone
from common.fault_tolerance.wal.reader import (
    Checkpoint,
    ClientCleanupStarted,
    EofSent,
    WALReader,
    decode_record,
)
from common.fault_tolerance.wal.record import HEADER_FORMAT, RecordType
from common.fault_tolerance.wal.writer import WALWriter


def _no_fsync(_fd: int) -> None:
    pass


def _writer(path: Path) -> WALWriter:
    return WALWriter(path, fsync_fn=_no_fsync)


@pytest.fixture
def wal_path(tmp_path: Path) -> Path:
    return tmp_path / "wal.current"


def test_records_returns_empty_for_missing_wal(wal_path: Path) -> None:
    assert list(WALReader(wal_path).records()) == []


def test_reader_round_trips_records_written_by_writer(wal_path: Path) -> None:
    outbox_entry = OutboxEntry(
        output_id="node:client:1#0",
        input_id="node:client:1",
        destination="next.queue",
        body=b"\x00payload",
    )
    input_applied = InputApplied(
        msg_id="node:client:1",
        client_id=7,
        sender_id=3,
        seq=11,
        state_change={"total": 42, "nested": {"ok": True}},
        outputs=[outbox_entry],
    )
    input_done = InputDone("node:client:1", 7, 3, 11)

    writer = _writer(wal_path)
    writer.write_input_applied(input_applied)
    writer.write_input_done(input_done)
    writer.write_client_cleanup_started(7)
    writer.write_eof_sent(client_id=7, fragment=99, node_id=2)
    writer.write_checkpoint(snapshot_lsn=1234)
    writer.close()

    raw_records = list(WALReader(wal_path).records())
    decoded = [decode_record(record) for record in raw_records]

    assert decoded == [
        input_applied,
        input_done,
        ClientCleanupStarted(client_id=7),
        EofSent(client_id=7, fragment=99, node_id=2),
        Checkpoint(snapshot_lsn=1234),
    ]


def test_records_after_filters_by_lsn(wal_path: Path) -> None:
    writer = _writer(wal_path)
    first_lsn = writer.write_input_done(InputDone("first", 1, 0, 1))
    second_lsn = writer.write_input_done(InputDone("second", 1, 0, 2))
    writer.close()

    records = list(WALReader(wal_path).records_after(first_lsn))

    assert [record.lsn for record in records] == [second_lsn]
    assert [decode_record(record).msg_id for record in records] == ["second"]


def test_reader_ignores_truncated_tail_header(wal_path: Path) -> None:
    writer = _writer(wal_path)
    writer.write_input_done(InputDone("complete", 1, 0, 1))
    writer.close()

    with wal_path.open("ab") as wal_file:
        wal_file.write(b"\x01\x00")

    records = list(WALReader(wal_path).records())

    assert [decode_record(record).msg_id for record in records] == ["complete"]


def test_reader_ignores_truncated_tail_payload(wal_path: Path) -> None:
    writer = _writer(wal_path)
    writer.write_input_done(InputDone("complete", 1, 0, 1))
    writer.close()

    with wal_path.open("ab") as wal_file:
        wal_file.write(struct.pack(HEADER_FORMAT, RecordType.INPUT_DONE, 10))
        wal_file.write(b"short")

    records = list(WALReader(wal_path).records())

    assert [decode_record(record).msg_id for record in records] == ["complete"]


def test_reader_rejects_unknown_record_type(wal_path: Path) -> None:
    wal_path.write_bytes(struct.pack(HEADER_FORMAT, 0xFF, 0))

    with pytest.raises(ValueError, match="unknown WAL record type 255"):
        list(WALReader(wal_path).records())


def test_decoder_rejects_payload_trailing_bytes(wal_path: Path) -> None:
    writer = _writer(wal_path)
    writer.write_input_done(InputDone("id", 1, 0, 1))
    writer.close()

    record = list(WALReader(wal_path).records())[0]
    tampered = type(record)(
        lsn=record.lsn,
        record_type=record.record_type,
        payload=record.payload + b"\x00",
    )

    with pytest.raises(ValueError, match="unexpected trailing bytes"):
        decode_record(tampered)
