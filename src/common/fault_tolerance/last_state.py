"""LastState: full-state snapshot to disk + recovery.

Owns last_state.{current,previous,tmp}. load() returns current, falling back to
previous if current is missing/torn/corrupt; None if neither exists. commit()
does the atomic double-buffer rename (PDF "Protocolo Para Hacer Snapshot"):
write tmp (+fsync) -> rename current->previous -> rename tmp->current -> fsync dir.

Reuse the atomic write pattern already in
src/monitor/election/epoch_store.py (tmp + fsync + os.replace + dir fsync).
"""

from __future__ import annotations

from pathlib import Path

from common.fault_tolerance.records import Snapshot


class LastState:
    def __init__(self, state_dir: str | Path) -> None:
        self._dir = Path(state_dir)
        self._current = self._dir / "last_state.current"
        self._previous = self._dir / "last_state.previous"
        self._tmp = self._dir / "last_state.tmp"

    def load(self) -> Snapshot | None:
        """current -> previous -> None. Validates checksum; torn current falls back."""
        raise NotImplementedError

    def commit(self, snapshot: Snapshot) -> None:
        """Atomic double-buffer write. Must fully succeed before WAL.rotate()."""
        raise NotImplementedError
