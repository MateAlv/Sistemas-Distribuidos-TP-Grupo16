"""Append-only log of INPUT_APPLIED / INPUT_DONE records.

append() serializes a record with a length prefix and checksum, writes it and
fsyncs, returning its record number. replay() yields records past a checkpoint
and silently drops a torn final record left by a crash mid-write. rotate()
starts a fresh log after a snapshot has committed.

The atomic write pattern in src/monitor/election/epoch_store.py can be reused.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

from common.fault_tolerance.wal.wal_record import WalRecord


class Wal:
    def __init__(self, state_dir: str | Path) -> None:
        self._dir = Path(state_dir)
        self._path = self._dir / "wal.current"

    def append(self, record: WalRecord) -> int:
        raise NotImplementedError

    def replay(self, from_record: int) -> Iterator[WalRecord]:
        raise NotImplementedError

    def current_record_number(self) -> int:
        raise NotImplementedError

    def rotate(self) -> None:
        raise NotImplementedError
