from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class Snapshot:
    """Full worker state at a point in time: one file per worker, all clients
    inside. Sections are plain picklable data. wal_checkpoint_record marks where
    recovery resumes replaying the WAL.

    inbox/outbox are handler-owned; worker_state is the worker adapter's own
    state (business state, counters and its EOF coordinator)."""

    wal_checkpoint_record: int
    inbox: dict = field(default_factory=dict)
    outbox: dict = field(default_factory=dict)
    worker_state: dict = field(default_factory=dict)
