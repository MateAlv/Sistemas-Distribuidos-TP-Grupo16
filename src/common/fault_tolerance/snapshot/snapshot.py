from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class Snapshot:
    """Full worker state at a point in time: one file per worker, all clients
    inside. wal_checkpoint_record is the last WAL record already folded in, so
    recovery replays only the records after it."""

    wal_checkpoint_record: int
    worker_state: dict = field(default_factory=dict)
    inbox: dict = field(default_factory=dict)
    outbox: dict = field(default_factory=dict)
    eof_state: dict = field(default_factory=dict)
    counters: dict = field(default_factory=dict)
    closed_clients: set = field(default_factory=set)
