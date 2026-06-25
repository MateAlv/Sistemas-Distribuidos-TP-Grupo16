"""Append-only WAL facade used by the durable-state handler."""

from __future__ import annotations

import os
from collections.abc import Iterator
from pathlib import Path

from common.fault_tolerance.wal.input_applied import InputApplied
from common.fault_tolerance.wal.input_done import InputDone
from common.fault_tolerance.wal.reader import WALReader, decode_record
from common.fault_tolerance.wal.wal_record import WalRecord
from common.fault_tolerance.wal.writer import FsyncFn, WALWriter


class Wal:
    def __init__(
        self,
        state_dir: str | Path,
        fsync_fn: FsyncFn = os.fsync,
    ) -> None:
        self._dir = Path(state_dir)
        self._dir.mkdir(parents=True, exist_ok=True)
        self._path = self._dir / "wal.current"
        self._rotated_path = self._dir / "wal.previous"
        self._fsync = fsync_fn
        self._writer = WALWriter(self._path, fsync_fn=self._fsync)

    def append(self, record: WalRecord) -> int:
        if isinstance(record, InputApplied):
            return self._writer.write_input_applied(record)
        if isinstance(record, InputDone):
            return self._writer.write_input_done(record)
        raise TypeError(f"unsupported WAL record type: {type(record).__name__}")

    def replay(self, from_record: int) -> Iterator[WalRecord]:
        for raw_record in WALReader(self._path).records_after(from_record):
            record = decode_record(raw_record)
            if isinstance(record, (InputApplied, InputDone)):
                yield record

    def current_record_number(self) -> int:
        if not self._path.exists():
            return 0
        return self._path.stat().st_size

    def rotate(self) -> None:
        self._writer.close()
        if self._path.exists():
            os.replace(self._path, self._rotated_path)
            self._fsync_dir()
        self._writer = WALWriter(self._path, fsync_fn=self._fsync)

    def close(self) -> None:
        self._writer.close()

    def _fsync_dir(self) -> None:
        fd = os.open(str(self._dir), os.O_RDONLY)
        try:
            self._fsync(fd)
        finally:
            os.close(fd)
