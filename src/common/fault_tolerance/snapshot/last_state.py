"""Reads and writes the full-state snapshot on disk.

load() returns last_state.current, falling back to last_state.previous if the
current file is missing or torn, or None if neither exists. commit() writes the
new snapshot atomically (tmp file, fsync, rename current->previous, rename
tmp->current, fsync dir) and must fully succeed before the WAL is rotated.

The atomic write pattern in src/monitor/election/epoch_store.py can be reused.
"""

from __future__ import annotations

from pathlib import Path

from common.fault_tolerance.snapshot.snapshot import Snapshot


class LastState:
    def __init__(self, state_dir: str | Path) -> None:
        self._dir = Path(state_dir)
        self._current = self._dir / "last_state.current"
        self._previous = self._dir / "last_state.previous"
        self._tmp = self._dir / "last_state.tmp"

    def load(self) -> Snapshot | None:
        raise NotImplementedError

    def commit(self, snapshot: Snapshot) -> None:
        raise NotImplementedError
