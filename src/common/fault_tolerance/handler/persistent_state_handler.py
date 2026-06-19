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

Recovery is idempotent: _apply_record consults the inbox and skips any record
already reflected in the loaded snapshot, so replaying records that a crash left
in an un-rotated WAL never double-applies.
"""

from __future__ import annotations

from collections.abc import Callable

from common.fault_tolerance.handler.action import Action
from common.fault_tolerance.handler.worker_loop_instruction import (
    WorkerLoopInstruction,
)
from common.fault_tolerance.inbox import Inbox, InboxStatus
from common.fault_tolerance.outbox import Outbox, OutboxEntry
from common.fault_tolerance.snapshot import LastState, Snapshot
from common.fault_tolerance.wal import InputApplied, InputDone, Wal, WalRecord
from common.fault_tolerance.worker_state import WorkerState

# Replay the whole current WAL segment. We rotate after every snapshot, so the
# segment only holds records since the last snapshot; precise LSN filtering is an
# optimization, not a correctness requirement (the inbox guards double-apply).
REPLAY_ALL = -1

BusinessFn = Callable[[bytes], "tuple[bytes, list[OutboxEntry]]"]


class PersistentStateHandler:
    def __init__(
        self,
        state_dir: str,
        node_id: str,
        worker_state: WorkerState,
        snapshot_every: int,
        *,
        last_state: LastState | None = None,
        wal: Wal | None = None,
    ) -> None:
        self.node_id = node_id
        self.snapshot_every = snapshot_every
        self.worker_state = worker_state
        self.last_state = last_state if last_state is not None else LastState(state_dir)
        self.wal = wal if wal is not None else Wal(state_dir)
        self.inbox = Inbox()
        self.outbox = Outbox()
        self.applied_since_snapshot = 0

    def recover(self) -> None:
        snapshot = self.last_state.load()
        from_record = REPLAY_ALL
        if snapshot is not None:
            self.worker_state.restore(snapshot.worker_state)
            self.inbox = Inbox.from_dict(snapshot.inbox)
            self.outbox = Outbox.from_dict(snapshot.outbox)
            from_record = snapshot.wal_checkpoint_record

        applied = 0
        for record in self.wal.replay(from_record):
            if self._apply_record(record):
                applied += 1
        self.applied_since_snapshot = applied

    def outbox_to_republish(self) -> list[OutboxEntry]:
        return self.outbox.all_pending()

    def handle(
        self,
        msg_id: str,
        client_id: int,
        sender_id: int,
        seq: int,
        payload: bytes,
        business_fn: BusinessFn,
    ) -> WorkerLoopInstruction:
        status = self.inbox.classify(client_id, sender_id, seq)
        if status is InboxStatus.DONE:
            return WorkerLoopInstruction(Action.ACK)

        if status is InboxStatus.NEW:
            state_change, outputs = business_fn(payload)
            self.wal.append(
                InputApplied(msg_id, client_id, sender_id, seq, state_change, outputs)
            )
            self.worker_state.apply_change(state_change)
            self.inbox.mark_applied(client_id, sender_id, seq)
            self.outbox.add(client_id, msg_id, outputs)
            self.applied_since_snapshot += 1

        return WorkerLoopInstruction(
            Action.PUBLISH_THEN_COMMIT,
            outputs=self.outbox.entries_for_input(client_id, msg_id),
            ctx=(msg_id, client_id, sender_id, seq),
        )

    def commit_done(
        self, msg_id: str, client_id: int, sender_id: int, seq: int
    ) -> WorkerLoopInstruction:
        self.wal.append(InputDone(msg_id, client_id, sender_id, seq))
        self.inbox.mark_done(client_id, sender_id, seq)
        self.outbox.remove_input(client_id, msg_id)
        self.maybe_snapshot()
        return WorkerLoopInstruction(Action.ACK)

    def maybe_snapshot(self) -> None:
        if self.applied_since_snapshot < self.snapshot_every:
            return
        snapshot = Snapshot(
            wal_checkpoint_record=REPLAY_ALL,
            worker_state=self.worker_state.snapshot(),
            inbox=self.inbox.to_dict(),
            outbox=self.outbox.to_dict(),
        )
        self.last_state.commit(snapshot)
        self.wal.rotate()
        self.applied_since_snapshot = 0

    def _apply_record(self, record: WalRecord) -> bool:
        """Replay one record into memory, skipping anything already reflected in
        the loaded snapshot. Returns whether an applied input was (re)counted."""
        if isinstance(record, InputApplied):
            if self.inbox.classify(record.client_id, record.sender_id, record.seq) is (
                InboxStatus.NEW
            ):
                self.worker_state.apply_change(record.state_change)
                self.inbox.mark_applied(record.client_id, record.sender_id, record.seq)
                self.outbox.add(record.client_id, record.msg_id, record.outputs)
                return True
            return False

        if isinstance(record, InputDone):
            if self.inbox.classify(record.client_id, record.sender_id, record.seq) is not (
                InboxStatus.DONE
            ):
                self.inbox.mark_done(record.client_id, record.sender_id, record.seq)
                self.outbox.remove_input(record.client_id, record.msg_id)
            return False

        raise TypeError(f"unsupported WAL record type: {type(record).__name__}")
