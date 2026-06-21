"""Protocol that the business-state adapter of every worker must implement.

The durable-state engine (PersistentStateHandler) uses this interface to
snapshot, restore and replay state without knowing the business logic.

Contract
--------
snapshot() -> dict
    Return the complete business state as a **picklable** dict.  The dict is
    passed to pickle, not JSON, so Python sets, tuples and arbitrary objects are
    allowed.  Called periodically (every N inputs) while the worker's lock is
    held; the returned dict must not share mutable objects with the live state.

restore(data: dict) -> None
    Load state from a snapshot dict.  ``data == {}`` means the worker has never
    been snapshotted (fresh start) — restore must reset to a clean empty state
    rather than raising.  Called once at startup, before the WAL is replayed.
    Must be idempotent: calling it twice with the same dict must leave the
    adapter in the same state.

apply_change(change: dict) -> None
    Apply **one** logical state change.  This is the *single* code path that
    mutates business state.  It runs:

      · **Live**: immediately after PersistentStateHandler writes the change to
        the WAL and before it acks the RabbitMQ message.
      · **On WAL replay**: during recover(), for every INPUT_APPLIED record
        after the last snapshot, in append order.

    Because the same method runs in both contexts, it must be free of I/O and
    side effects.  It must also be idempotent at the WAL-record level: if the
    same WAL record is replayed twice (possible when the WAL overlaps with the
    snapshot after a crash mid-snapshot), the second call must leave the state
    unchanged.

Change dict format
------------------
The change dict is serialized as **JSON** inside the WAL, so:
  - All values must be JSON-safe: strings, ints, floats, bools, None, lists,
    dicts.  No bytes, sets, or tuples.
  - Raw ``bytes`` payloads must be base64-encoded to a str before storing in
    the change dict (use ``base64.b64encode(b).decode('ascii')``).
  - Enum values must be stored as their underlying int/str value.
  - Integer dict keys are coerced to strings by JSON; avoid them in change
    dicts (use them only in snapshot/restore which go through pickle).

One change per input
--------------------
PersistentStateHandler.handle() calls business_fn(payload) once per RabbitMQ
message and writes exactly one INPUT_APPLIED WAL record carrying the returned
change dict.  If a single message triggers multiple logical state mutations
(e.g. coordinator cleanup + business close), bundle them into one compound
change dict so that a single apply_change call can replay them atomically.

Thread safety
-------------
The adapter itself is not thread-safe.  The worker must hold its own lock
around every call to apply_change(), snapshot(), and restore().
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class WorkerState(Protocol):
    def snapshot(self) -> dict:
        """Return the full business state as a picklable dict.

        The returned dict must not share mutable objects with live state —
        make shallow (or deep) copies of all collections before returning.
        Called while the worker's lock is held.
        """
        ...

    def restore(self, data: dict) -> None:
        """Load state from a snapshot dict.

        ``data == {}`` means fresh start: reset every collection to empty.
        Called once at startup before WAL replay; must be idempotent.
        """
        ...

    def apply_change(self, change: dict) -> None:
        """Apply one state change (live path and WAL replay path).

        Must be free of I/O, idempotent within a single WAL replay, and
        handle unknown future change types gracefully (raise ValueError).
        """
        ...
