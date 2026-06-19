"""Append-only WAL writer with per-record fsync.

Each write_* method serializes one record, calls os.write() with the full
buffer (atomic for sizes below PIPE_BUF), then fsyncs. Returns the byte
offset (LSN) of the record just written.

The fsync_fn is injectable so tests run without hitting disk.
"""

import json
import os
import struct
from collections.abc import Callable
from pathlib import Path

from common.fault_tolerance._encoding import (
    MAX_UINT16,
    MAX_UINT32,
    UINT16_FORMAT,
    UINT32_FORMAT,
)
from common.fault_tolerance.wal.input_applied import InputApplied
from common.fault_tolerance.wal.input_done import InputDone
from common.fault_tolerance.wal.record import (
    HEADER_FORMAT,
    RecordType,
)

FsyncFn = Callable[[int], None]

_CLIENT_ID_FORMAT = ">I"
_SENDER_ID_FORMAT = ">I"
_SEQ_FORMAT = ">I"
_OUTBOX_COUNT_FORMAT = ">H"

_MAX_OUTBOX_ENTRIES = MAX_UINT16


class WALWriter:
    def __init__(
        self,
        path: str | Path,
        fsync_fn: FsyncFn = os.fsync,
    ) -> None:
        self._path = Path(path)
        self._fsync = fsync_fn
        self._fd: int = os.open(
            str(self._path),
            os.O_WRONLY | os.O_CREAT | os.O_APPEND,
            0o644,
        )

    def write_input_applied(self, record: InputApplied) -> int:
        state_change_bytes = json.dumps(record.state_change).encode()
        msg_id_bytes = record.msg_id.encode()
        _ensure_u16_len("msg_id", msg_id_bytes)
        _ensure_u32_len("state_change", state_change_bytes)
        if len(record.outputs) > _MAX_OUTBOX_ENTRIES:
            raise ValueError("too many outbox entries")

        outbox_blob = b"".join(e.serialize() for e in record.outputs)

        payload = (
            struct.pack(UINT16_FORMAT, len(msg_id_bytes)) + msg_id_bytes
            + struct.pack(_CLIENT_ID_FORMAT, record.client_id)
            + struct.pack(_SENDER_ID_FORMAT, record.sender_id)
            + struct.pack(_SEQ_FORMAT, record.seq)
            + struct.pack(UINT32_FORMAT, len(state_change_bytes)) + state_change_bytes
            + struct.pack(_OUTBOX_COUNT_FORMAT, len(record.outputs)) + outbox_blob
        )
        return self._write_record(RecordType.INPUT_APPLIED, payload)

    def write_input_done(self, record: InputDone) -> int:
        msg_id_bytes = record.msg_id.encode()
        _ensure_u16_len("msg_id", msg_id_bytes)
        payload = (
            struct.pack(UINT16_FORMAT, len(msg_id_bytes)) + msg_id_bytes
            + struct.pack(_CLIENT_ID_FORMAT, record.client_id)
            + struct.pack(_SENDER_ID_FORMAT, record.sender_id)
            + struct.pack(_SEQ_FORMAT, record.seq)
        )
        return self._write_record(RecordType.INPUT_DONE, payload)

    def close(self) -> None:
        os.close(self._fd)


    def _write_record(self, record_type: RecordType, payload: bytes) -> int:
        _ensure_u32_len("payload", payload)
        header = struct.pack(HEADER_FORMAT, record_type, len(payload))
        data = header + payload
        lsn = os.lseek(self._fd, 0, os.SEEK_END)
        self._write_all(data)
        self._fsync(self._fd)
        return lsn

    def _write_all(self, data: bytes) -> None:
        view = memoryview(data)
        written = 0
        while written < len(data):
            n = os.write(self._fd, view[written:])
            if n == 0:
                raise OSError("os.write returned 0 bytes while writing WAL record")
            written += n


def _ensure_u16_len(field: str, data: bytes) -> None:
    if len(data) > MAX_UINT16:
        raise ValueError(f"{field} too long")


def _ensure_u32_len(field: str, data: bytes) -> None:
    if len(data) > MAX_UINT32:
        raise ValueError(f"{field} too long")
