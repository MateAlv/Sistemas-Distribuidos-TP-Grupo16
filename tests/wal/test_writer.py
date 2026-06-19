"""Tests for WALWriter.

All tests run in-memory: the fsync_fn is replaced with a no-op so no real file
is created. File I/O is exercised via a tmp file to verify LSN tracking and
append-only behaviour.
"""

import os
import struct
from pathlib import Path

import pytest

from common.fault_tolerance._encoding import MAX_UINT16, UINT16_FORMAT, UINT32_FORMAT
from common.fault_tolerance.outbox.outbox_entry import OutboxEntry
from common.fault_tolerance.wal.input_applied import InputApplied
from common.fault_tolerance.wal.input_done import InputDone
from common.fault_tolerance.wal.record import HEADER_FORMAT, HEADER_SIZE, RecordType
from common.fault_tolerance.wal.writer import WALWriter


def _no_fsync(_fd: int) -> None:
    pass


def _make_writer(path: Path) -> WALWriter:
    return WALWriter(path, fsync_fn=_no_fsync)


def _read_record(data: bytes, offset: int = 0) -> tuple[int, bytes, int]:
    """Returns (record_type, payload, next_offset)."""
    record_type, length = struct.unpack_from(HEADER_FORMAT, data, offset)
    offset += HEADER_SIZE
    payload = data[offset : offset + length]
    return record_type, payload, offset + length


@pytest.fixture
def wal_path(tmp_path: Path) -> Path:
    return tmp_path / "wal.current"


class TestWriteInputApplied:
    def test_writes_correct_record_type(self, wal_path: Path) -> None:
        w = _make_writer(wal_path)
        record = InputApplied(
            msg_id="n0:c1:42",
            client_id=1,
            sender_id=2,
            seq=42,
            state_change={"sum": 100},
            outputs=[],
        )
        w.write_input_applied(record)
        w.close()

        data = wal_path.read_bytes()
        record_type, _, _ = _read_record(data)
        assert record_type == RecordType.INPUT_APPLIED

    def test_payload_contains_msg_id(self, wal_path: Path) -> None:
        w = _make_writer(wal_path)
        record = InputApplied(
            msg_id="mynode:client7:99",
            client_id=7,
            sender_id=0,
            seq=99,
            state_change={},
            outputs=[],
        )
        w.write_input_applied(record)
        w.close()

        data = wal_path.read_bytes()
        _, payload, _ = _read_record(data)
        msg_id_len = struct.unpack_from(UINT16_FORMAT, payload, 0)[0]
        msg_id = payload[2 : 2 + msg_id_len].decode()
        assert msg_id == record.msg_id

    def test_outbox_entries_are_serialized(self, wal_path: Path) -> None:
        entry = OutboxEntry(
            output_id="n0:c1:42#0",
            input_id="n0:c1:42",
            destination="next_queue",
            body=b"\xde\xad",
        )
        w = _make_writer(wal_path)
        record = InputApplied(
            msg_id="n0:c1:42",
            client_id=1,
            sender_id=0,
            seq=42,
            state_change={},
            outputs=[entry],
        )
        w.write_input_applied(record)
        w.close()

        # Verify the serialized entry round-trips correctly.
        data = wal_path.read_bytes()
        _, payload, _ = _read_record(data)
        # skip msg_id, client_id, sender_id, seq, state_change_blob
        offset = 0
        msg_id_len = struct.unpack_from(UINT16_FORMAT, payload, offset)[0]
        offset += 2 + msg_id_len
        offset += 4 + 4 + 4  # client_id, sender_id, seq
        sc_len = struct.unpack_from(UINT32_FORMAT, payload, offset)[0]
        offset += 4 + sc_len
        count = struct.unpack_from(UINT16_FORMAT, payload, offset)[0]
        offset += 2
        assert count == 1
        recovered, _ = OutboxEntry.deserialize(payload, offset)
        assert recovered == entry

    def test_returns_lsn_zero_for_first_record(self, wal_path: Path) -> None:
        w = _make_writer(wal_path)
        record = InputApplied("id", 1, 0, 1, {}, [])
        lsn = w.write_input_applied(record)
        w.close()
        assert lsn == 0

    def test_second_record_lsn_equals_first_record_size(self, wal_path: Path) -> None:
        w = _make_writer(wal_path)
        r1 = InputApplied("id1", 1, 0, 1, {}, [])
        r2 = InputDone("id1", 1, 0, 1)
        lsn1 = w.write_input_applied(r1)
        lsn2 = w.write_input_done(r2)
        w.close()

        data = wal_path.read_bytes()
        _, _, end_of_first = _read_record(data, 0)
        assert lsn1 == 0
        assert lsn2 == end_of_first

    def test_lsn_uses_existing_file_size_after_reopen(self, wal_path: Path) -> None:
        first = _make_writer(wal_path)
        first.write_input_done(InputDone("id1", 1, 0, 1))
        first.close()
        existing_size = wal_path.stat().st_size

        second = _make_writer(wal_path)
        lsn = second.write_input_done(InputDone("id2", 1, 0, 2))
        second.close()

        assert lsn == existing_size

    def test_rejects_too_long_msg_id(self, wal_path: Path) -> None:
        w = _make_writer(wal_path)
        record = InputApplied("x" * (MAX_UINT16 + 1), 1, 0, 1, {}, [])
        with pytest.raises(ValueError, match="msg_id too long"):
            w.write_input_applied(record)
        w.close()

    def test_rejects_too_many_outputs(self, wal_path: Path) -> None:
        w = _make_writer(wal_path)
        entry = OutboxEntry("o", "i", "d", b"")
        record = InputApplied("id", 1, 0, 1, {}, [entry] * (MAX_UINT16 + 1))
        with pytest.raises(ValueError, match="too many outbox entries"):
            w.write_input_applied(record)
        w.close()


class TestWriteInputDone:
    def test_writes_correct_record_type(self, wal_path: Path) -> None:
        w = _make_writer(wal_path)
        w.write_input_done(InputDone("id", 1, 0, 1))
        w.close()

        data = wal_path.read_bytes()
        record_type, _, _ = _read_record(data)
        assert record_type == RecordType.INPUT_DONE


class TestFsyncIsCalledPerRecord:
    def test_fsync_called_once_per_write(self, wal_path: Path) -> None:
        calls: list[int] = []
        w = WALWriter(wal_path, fsync_fn=lambda fd: calls.append(fd))
        w.write_input_done(InputDone("id", 1, 0, 1))
        w.write_input_done(InputDone("id2", 1, 0, 2))
        w.close()
        assert len(calls) == 2


class TestWriteAll:
    def test_partial_os_write_is_retried(self, wal_path: Path, monkeypatch) -> None:
        real_write = os.write
        chunks: list[int] = []

        def partial_write(fd: int, data) -> int:
            limit = max(1, len(data) // 2)
            written = real_write(fd, data[:limit])
            chunks.append(written)
            return written

        monkeypatch.setattr(os, "write", partial_write)
        w = _make_writer(wal_path)
        w.write_input_done(InputDone("partial", 1, 0, 1))
        w.close()

        data = wal_path.read_bytes()
        record_type, payload, next_offset = _read_record(data)
        assert record_type == RecordType.INPUT_DONE
        assert next_offset == len(data)
        assert payload
        assert len(chunks) > 1

    def test_zero_byte_os_write_raises(self, wal_path: Path, monkeypatch) -> None:
        monkeypatch.setattr(os, "write", lambda _fd, _data: 0)
        w = _make_writer(wal_path)
        with pytest.raises(OSError, match="0 bytes"):
            w.write_input_done(InputDone("id", 1, 0, 1))
        w.close()
