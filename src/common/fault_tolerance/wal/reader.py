from __future__ import annotations

import json
import struct
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Union

from common.fault_tolerance._encoding import (
    read_length_prefixed_u16,
    read_length_prefixed_u32,
    read_uint16,
    read_uint32,
)
from common.fault_tolerance.outbox.outbox_entry import OutboxEntry
from common.fault_tolerance.wal.input_applied import InputApplied
from common.fault_tolerance.wal.input_done import InputDone
from common.fault_tolerance.wal.record import HEADER_FORMAT, HEADER_SIZE, RecordType


@dataclass(frozen=True)
class WALRawRecord:
    lsn: int
    record_type: RecordType
    payload: bytes


DecodedRecord = Union[InputApplied, InputDone]


class WALReader:
    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)

    def records(self) -> Iterator[WALRawRecord]:
        if not self._path.exists():
            return

        with self._path.open("rb") as wal_file:
            while True:
                lsn = wal_file.tell()
                header = wal_file.read(HEADER_SIZE)
                if len(header) == 0:
                    return
                if len(header) < HEADER_SIZE:
                    return

                raw_type, length = struct.unpack(HEADER_FORMAT, header)
                payload = wal_file.read(length)
                if len(payload) < length:
                    return

                try:
                    record_type = RecordType(raw_type)
                except ValueError as exc:
                    raise ValueError(
                        f"unknown WAL record type {raw_type} at LSN {lsn}"
                    ) from exc

                yield WALRawRecord(lsn, record_type, payload)

    def records_after(self, checkpoint_lsn: int) -> Iterator[WALRawRecord]:
        return (record for record in self.records() if record.lsn > checkpoint_lsn)

    def decoded_records(self) -> Iterator[tuple[int, DecodedRecord]]:
        return ((record.lsn, decode_record(record)) for record in self.records())


def decode_record(record: WALRawRecord) -> DecodedRecord:
    if record.record_type == RecordType.INPUT_APPLIED:
        return decode_input_applied(record.payload)
    if record.record_type == RecordType.INPUT_DONE:
        return decode_input_done(record.payload)
    raise ValueError(f"unsupported WAL record type {record.record_type!r}")


def decode_input_applied(payload: bytes) -> InputApplied:
    offset = 0
    msg_id_bytes, offset = read_length_prefixed_u16(payload, offset)
    client_id, offset = read_uint32(payload, offset)
    sender_id, offset = read_uint32(payload, offset)
    seq, offset = read_uint32(payload, offset)
    state_change_bytes, offset = read_length_prefixed_u32(payload, offset)
    outbox_count, offset = read_uint16(payload, offset)

    outputs: list[OutboxEntry] = []
    for _ in range(outbox_count):
        entry, offset = OutboxEntry.deserialize(payload, offset)
        outputs.append(entry)

    _ensure_consumed(payload, offset)
    return InputApplied(
        msg_id=_decode_text("msg_id", msg_id_bytes),
        client_id=client_id,
        sender_id=sender_id,
        seq=seq,
        state_change=_decode_json_state_change(state_change_bytes),
        outputs=outputs,
    )


def decode_input_done(payload: bytes) -> InputDone:
    offset = 0
    msg_id_bytes, offset = read_length_prefixed_u16(payload, offset)
    client_id, offset = read_uint32(payload, offset)
    sender_id, offset = read_uint32(payload, offset)
    seq, offset = read_uint32(payload, offset)
    _ensure_consumed(payload, offset)
    return InputDone(
        msg_id=_decode_text("msg_id", msg_id_bytes),
        client_id=client_id,
        sender_id=sender_id,
        seq=seq,
    )


def _decode_text(field: str, data: bytes) -> str:
    try:
        return data.decode()
    except UnicodeDecodeError as exc:
        raise ValueError(f"invalid UTF-8 in WAL {field}") from exc


def _decode_json_state_change(data: bytes) -> dict:
    try:
        value = json.loads(data.decode())
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("invalid JSON state_change in WAL record") from exc
    if not isinstance(value, dict):
        raise ValueError("state_change must decode to a dict")
    return value


def _ensure_consumed(payload: bytes, offset: int) -> None:
    if offset != len(payload):
        raise ValueError(
            f"record payload has {len(payload) - offset} unexpected trailing bytes"
        )
