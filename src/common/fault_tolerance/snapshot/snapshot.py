from __future__ import annotations

from dataclasses import dataclass


@dataclass
class Snapshot:
    """Full worker state at a point in time: one file per worker, all clients
    inside. Each section is bytes produced by its component's serialize().
    wal_checkpoint_record marks where recovery resumes replaying the WAL."""

    wal_checkpoint_record: int
    worker_state: bytes = b""
    inbox: bytes = b""
    outbox: bytes = b""
    eof_state: bytes = b""
    counters: bytes = b""
    closed_clients: bytes = b""
