"""The worker's own state, that knows how to save, load and replay itself.

Each worker supplies an implementation (e.g. the sum worker wraps its
max-by-bank table) so the durable-state engine can snapshot, restore and replay
state without knowing anything about the business logic. All payloads are bytes
encoded by the worker itself (no JSON), like the rest of the wire protocol.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class WorkerState(Protocol):
    def snapshot(self) -> bytes:
        """Return the full state as bytes."""
        ...

    def restore(self, data: bytes) -> None:
        """Load state from snapshot bytes (empty bytes means a fresh start)."""
        ...

    def apply_change(self, change: bytes) -> None:
        """Apply one state change. The single code path that mutates state, used
        both live and during recovery replay."""
        ...
