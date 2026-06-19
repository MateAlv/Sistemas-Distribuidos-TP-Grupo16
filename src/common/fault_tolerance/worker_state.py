"""The worker's own state, that knows how to save, load and replay itself.

Each worker supplies an implementation (e.g. the sum worker wraps its
max-by-bank table and its EOF coordinator) so the durable-state engine can
snapshot, restore and replay state without knowing the business logic. All
payloads are plain picklable data (dicts), serialized as part of the snapshot.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class WorkerState(Protocol):
    def snapshot(self) -> dict:
        """Return the full state as a picklable dict."""
        ...

    def restore(self, data: dict) -> None:
        """Load state from a snapshot dict (empty dict means a fresh start)."""
        ...

    def apply_change(self, change: dict) -> None:
        """Apply one state change. The single code path that mutates state, used
        both live and during recovery replay."""
        ...
