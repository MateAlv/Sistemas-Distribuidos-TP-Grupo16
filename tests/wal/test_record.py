import struct

import pytest

from common.fault_tolerance._encoding import UINT16_SIZE
from common.fault_tolerance.outbox.outbox_entry import OutboxEntry
from common.fault_tolerance.wal.record import HEADER_FORMAT, HEADER_SIZE, RecordType


def test_header_format_is_big_endian_type_plus_length() -> None:
    data = struct.pack(HEADER_FORMAT, RecordType.INPUT_APPLIED, 42)
    assert data == b"\x01\x00\x00\x00\x2a"
    assert len(data) == HEADER_SIZE


def test_uint16_size_is_two_bytes() -> None:
    assert UINT16_SIZE == 2


@pytest.mark.parametrize("record_type", list(RecordType))
def test_record_type_values_are_stable(record_type: RecordType) -> None:
    expected = {
        RecordType.INPUT_APPLIED: 0x01,
        RecordType.INPUT_DONE: 0x02,
        RecordType.CLIENT_CLEANUP_STARTED: 0x03,
        RecordType.EOF_SENT: 0x04,
        RecordType.CHECKPOINT: 0x05,
    }
    assert record_type == expected[record_type]


class TestOutboxEntry:
    def _entry(self, **kwargs) -> OutboxEntry:
        defaults = dict(
            output_id="filter_usd_0:abc:42#0",
            input_id="filter_usd_0:abc:42",
            destination="sum_q2_1",
            body=b"\x01\x02\x03",
        )
        return OutboxEntry(**{**defaults, **kwargs})

    def test_round_trip(self) -> None:
        entry = self._entry()
        data = entry.serialize()
        recovered, consumed = OutboxEntry.deserialize(data)
        assert recovered == entry
        assert consumed == len(data)

    def test_round_trip_empty_body(self) -> None:
        entry = self._entry(body=b"")
        data = entry.serialize()
        recovered, consumed = OutboxEntry.deserialize(data)
        assert recovered == entry
        assert consumed == len(data)

    def test_round_trip_binary_body(self) -> None:
        entry = self._entry(body=bytes(range(256)))
        data = entry.serialize()
        recovered, _ = OutboxEntry.deserialize(data)
        assert recovered.body == entry.body

    def test_deserialize_with_offset(self) -> None:
        prefix = b"\xff\xff"
        entry = self._entry(output_id="n:m:7#3", destination="sink", body=b"hi")
        entry_bytes = entry.serialize()
        data = prefix + entry_bytes
        recovered, consumed = OutboxEntry.deserialize(data, offset=2)
        assert recovered == entry
        assert consumed == 2 + len(entry_bytes)

    def test_deserialize_multiple_entries(self) -> None:
        entries = [
            self._entry(output_id=f"a:b:{i}#0", input_id=f"a:b:{i}", body=b"data")
            for i in range(3)
        ]
        blob = b"".join(e.serialize() for e in entries)
        recovered = []
        offset = 0
        for _ in range(3):
            entry, offset = OutboxEntry.deserialize(blob, offset)
            recovered.append(entry)
        assert recovered == entries
        assert offset == len(blob)

    def test_unicode_fields_roundtrip(self) -> None:
        entry = self._entry(output_id="nodo_0:café:1#0", input_id="nodo_0:café:1")
        data = entry.serialize()
        recovered, _ = OutboxEntry.deserialize(data)
        assert recovered.output_id == entry.output_id
        assert recovered.input_id == entry.input_id

    def test_truncated_output_id_raises(self) -> None:
        entry = self._entry(output_id="abc")
        data = entry.serialize()
        with pytest.raises(ValueError, match="truncated"):
            OutboxEntry.deserialize(data[: UINT16_SIZE + 1])

    def test_truncated_input_id_raises(self) -> None:
        entry = self._entry(output_id="a", input_id="longid")
        data = entry.serialize()
        output_id_section = UINT16_SIZE + len("a".encode())
        with pytest.raises(ValueError, match="truncated"):
            OutboxEntry.deserialize(data[: output_id_section + UINT16_SIZE + 1])

    def test_truncated_body_raises(self) -> None:
        entry = self._entry(body=b"\x00" * 10)
        data = entry.serialize()
        with pytest.raises(ValueError, match="truncated"):
            OutboxEntry.deserialize(data[:-1])
