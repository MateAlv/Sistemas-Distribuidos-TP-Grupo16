"""In-memory stand-ins for Wal, LastState and WorkerState so the handler can be
tested (including crash injection) without touching disk or RabbitMQ.

A "crash" in these tests = drop the handler instance but keep the shared FakeWal
and FakeLastState (they are the durable disk), then build a new handler over them
and call recover().
"""

from __future__ import annotations

import copy
import struct

from common.fault_tolerance.snapshot import Snapshot
from common.fault_tolerance.wal import WalRecord

_INT = ">q"


class CrashError(RuntimeError):
    """Raised by a fake to simulate a process crash at a durability boundary."""


class FakeWal:
    def __init__(self) -> None:
        self.records: list[tuple[int, WalRecord]] = []
        self.previous: list[tuple[int, WalRecord]] = []
        self._next = 0
        self.raise_on_append: int | None = None
        self.raise_on_rotate = False
        self._appends = 0

    def append(self, record: WalRecord) -> int:
        self._appends += 1
        if self.raise_on_append == self._appends:
            raise CrashError("crash during WAL append")
        lsn = self._next
        self._next += 1
        self.records.append((lsn, record))
        return lsn

    def replay(self, from_record: int):
        for lsn, record in self.records:
            if lsn > from_record:
                yield record

    def current_record_number(self) -> int:
        return self._next

    def rotate(self) -> None:
        if self.raise_on_rotate:
            raise CrashError("crash during WAL rotate")
        self.previous = self.records
        self.records = []
        self._next = 0  # mirror the real WAL resetting the file offset on rotation


class FakeLastState:
    def __init__(self) -> None:
        self._snapshot: Snapshot | None = None
        self.raise_on_commit = False

    def load(self) -> Snapshot | None:
        return copy.deepcopy(self._snapshot)

    def commit(self, snapshot: Snapshot) -> None:
        if self.raise_on_commit:
            raise CrashError("crash during snapshot commit")
        self._snapshot = copy.deepcopy(snapshot)


class FakeWorkerState:
    """A running sum: apply_change is NOT idempotent, so any accidental
    double-apply during recovery shows up as a wrong total."""

    def __init__(self) -> None:
        self.total = 0

    def snapshot(self) -> bytes:
        return struct.pack(_INT, self.total)

    def restore(self, data: bytes) -> None:
        self.total = struct.unpack(_INT, data)[0] if data else 0

    def apply_change(self, change: bytes) -> None:
        self.total += struct.unpack(_INT, change)[0]


def change(value: int) -> bytes:
    return struct.pack(_INT, value)
