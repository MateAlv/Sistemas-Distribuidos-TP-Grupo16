"""Reads and writes the full-state snapshot on disk.

commit() writes the snapshot atomically with a double buffer: pickle to a tmp
file, fsync, rename current->previous, rename tmp->current, fsync the directory.
This must fully succeed before the WAL is rotated. load() returns the current
snapshot, falling back to previous if current is missing or corrupt, or None if
neither exists.
"""

from __future__ import annotations

import logging
import os
import pickle
from pathlib import Path

from common.fault_tolerance.snapshot.snapshot import Snapshot


class LastState:
    def __init__(self, state_dir: str | Path) -> None:
        self._dir = Path(state_dir)
        self._dir.mkdir(parents=True, exist_ok=True)
        self._current = self._dir / "last_state.current"
        self._previous = self._dir / "last_state.previous"
        self._tmp = self._dir / "last_state.tmp"

    def load(self) -> Snapshot | None:
        for path in (self._current, self._previous):
            if not path.exists():
                continue
            try:
                with path.open("rb") as snapshot_file:
                    return pickle.load(snapshot_file)
            except Exception:
                logging.warning("last_state_load_error | file=%s", path, exc_info=True)
        return None

    def commit(self, snapshot: Snapshot) -> None:
        with self._tmp.open("wb") as snapshot_file:
            pickle.dump(snapshot, snapshot_file, protocol=pickle.HIGHEST_PROTOCOL)
            snapshot_file.flush()
            os.fsync(snapshot_file.fileno())

        if self._current.exists():
            os.replace(self._current, self._previous)
        os.replace(self._tmp, self._current)
        self._fsync_dir()

    def _fsync_dir(self) -> None:
        fd = os.open(str(self._dir), os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
