"""Business-state adapter each worker implements so PersistentStateHandler can
persist and replay state without knowing the business logic.

The change dict passed to apply_change is JSON-serialized in the WAL, so it must
hold only JSON-safe values (base64-encode raw bytes, no sets or tuples). The
snapshot dict goes through pickle instead, so it may hold any picklable object.

this file is NOT really used, just a template to set WorkerStates for each worker
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class WorkerState(Protocol):
    def snapshot(self) -> dict:
        """Return the business state as a picklable dict, copying every
        collection so it shares no mutable objects with the live state."""
        ...

    def restore(self, data: dict) -> None:
        """Load state from a snapshot dict. An empty dict means a fresh start:
        reset everything to empty."""
        ...

    def apply_change(self, change: dict) -> None:
        """Apply one state change. Runs both live and during WAL replay, so it
        must do no I/O and stay idempotent if the same change replays twice."""
        ...
