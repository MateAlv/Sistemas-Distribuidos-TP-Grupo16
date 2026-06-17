"""The worker's own state, that knows how to save, load and replay itself.

Each worker supplies an implementation (e.g. the sum worker wraps its
max-by-bank table) so the durable-state engine can snapshot, restore and replay
state without knowing anything about the business logic.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class WorkerState(Protocol):
    def snapshot(self) -> dict:
        """Return the full state as a serializable dict."""
        ...

    def restore(self, data: dict) -> None:
        """Load state from a snapshot dict (empty dict means a fresh start)."""
        ...

    def change_for(self, payload, outputs) -> dict:
        """Build the state change for a freshly processed input.

        The result is written into the WAL and later replayed via apply_change().
        """
        ...

    def apply_change(self, change: dict) -> None:
        """Apply a state change. The single code path that mutates state, used
        both live and during recovery replay."""
        ...
