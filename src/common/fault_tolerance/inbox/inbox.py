"""Per-client dedup brain: classifies every incoming message as NEW, APPLIED or
DONE, and tracks the transitions.

`applied` holds the (sender_id, seq) pairs whose state change was applied but
whose outputs are not yet published+committed. `done` is a tracker per
(client_id, sender_id), the bounded-memory replacement for storing every
finished id.

Holds no files: persistence is via to_dict()/from_dict() (plain picklable data);
transitions are driven by PersistentStateHandler after each WAL write.
"""

from __future__ import annotations

from common.fault_tolerance.inbox.inbox_status import InboxStatus
from common.fault_tolerance.inbox.deduplication_tracker import DeduplicationTracker


class Inbox:
    def __init__(self) -> None:
        self._applied: dict[int, set[tuple[int, int]]] = {}
        self._done: dict[int, dict[int, DeduplicationTracker]] = {}

    def classify(self, client_id: int, sender_id: int, seq: int) -> InboxStatus:
        if (sender_id, seq) in self._applied.get(client_id, ()):
            return InboxStatus.APPLIED
        tracker = self._done.get(client_id, {}).get(sender_id)
        if tracker is not None and tracker.is_duplicate(seq):
            return InboxStatus.DONE
        return InboxStatus.NEW

    def mark_applied(self, client_id: int, sender_id: int, seq: int) -> None:
        self._applied.setdefault(client_id, set()).add((sender_id, seq))

    def mark_done(self, client_id: int, sender_id: int, seq: int) -> None:
        applied = self._applied.get(client_id)
        if applied is not None:
            applied.discard((sender_id, seq))
            if not applied:
                del self._applied[client_id]
        tracker = self._done.setdefault(client_id, {}).setdefault(
            sender_id, DeduplicationTracker()
        )
        tracker.observe(seq)

    def drop_client(self, client_id: int) -> None:
        self._applied.pop(client_id, None)
        self._done.pop(client_id, None)

    def to_dict(self) -> dict:
        return {
            "applied": {
                client_id: sorted(pairs)
                for client_id, pairs in self._applied.items()
            },
            "done": {
                client_id: {
                    sender_id: tracker.to_dict()
                    for sender_id, tracker in senders.items()
                }
                for client_id, senders in self._done.items()
            },
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Inbox":
        inbox = cls()
        inbox._applied = {
            client_id: {(sender_id, seq) for sender_id, seq in pairs}
            for client_id, pairs in data.get("applied", {}).items()
        }
        inbox._done = {
            client_id: {
                sender_id: DeduplicationTracker.from_dict(wm)
                for sender_id, wm in senders.items()
            }
            for client_id, senders in data.get("done", {}).items()
        }
        return inbox
