"""Orchestrates the durable-state protocol; the only object the worker loop
talks to.

It owns the snapshot counter and the ordering of every disk write, memory
mutation and RabbitMQ ack. The ordering it must guarantee:
  1. INPUT_APPLIED is fsynced before any output is published.
  2. All outputs are publisher-confirmed before INPUT_DONE.
  3. INPUT_DONE is fsynced before the RabbitMQ ack.
  4. A snapshot fully commits before the WAL rotates.
  5. Every memory mutation happens after its WAL append.
  6. On recovery, the outbox is republished before consuming new messages.
"""

from __future__ import annotations

from collections.abc import Callable

from common.fault_tolerance.handler.worker_loop_instruction import (
    WorkerLoopInstruction,
)
from common.fault_tolerance.inbox import Inbox
from common.fault_tolerance.outbox import Outbox, OutboxEntry
from common.fault_tolerance.snapshot import LastState
from common.fault_tolerance.wal import Wal, WalRecord
from common.fault_tolerance.worker_state import WorkerState


class PersistentStateHandler:
    def __init__(
        self,
        state_dir: str,
        node_id: str,
        worker_state: WorkerState,
        snapshot_every: int,
    ) -> None:
        self.node_id = node_id
        self.snapshot_every = snapshot_every
        self.worker_state = worker_state
        self.last_state = LastState(state_dir)
        self.wal = Wal(state_dir)
        self.inbox = Inbox()
        self.outbox = Outbox()
        self.applied_since_snapshot = 0

    def recover(self) -> None:
        """Load the snapshot and replay the WAL into memory, without publishing."""
        raise NotImplementedError

    def outbox_to_republish(self) -> list[OutboxEntry]:
        """Pending outbox to resend before consuming new messages."""
        raise NotImplementedError

    def handle(
        self,
        msg_id: str,
        client_id: int,
        sender_id: int,
        seq: int,
        payload: bytes,
        business_fn: Callable[[bytes], list[OutboxEntry]],
    ) -> WorkerLoopInstruction:
        raise NotImplementedError

    def commit_done(
        self, msg_id: str, client_id: int, sender_id: int, seq: int
    ) -> WorkerLoopInstruction:
        """Write INPUT_DONE, update memory and maybe snapshot, after confirms."""
        raise NotImplementedError

    def maybe_snapshot(self) -> None:
        raise NotImplementedError

    def _apply_record(self, record: WalRecord) -> None:
        """Mutate inbox/outbox/worker_state from a WAL record; shared by the live
        path and recovery replay."""
        raise NotImplementedError
