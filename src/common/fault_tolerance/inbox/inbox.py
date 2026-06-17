"""Per-client dedup brain: classifies every incoming message as NEW, APPLIED or
DONE, and tracks the transitions.

`applied` holds the (sender_id, seq) pairs whose state change was applied but
whose outputs are not yet published+committed. `done` is a watermark per
(client_id, sender_id), the bounded-memory replacement for storing every
finished id.

Holds no files: persistence is via to_dict()/from_dict() (JSON-safe shapes, so
any serializer works); transitions are driven by PersistentStateHandler after
each WAL write.
"""

from __future__ import annotations

from common.fault_tolerance.inbox.inbox_status import InboxStatus
from common.fault_tolerance.inbox.watermark import Watermark


class Inbox:
    def __init__(self) -> None:
        self._applied: dict[int, set[tuple[int, int]]] = {}
        self._done: dict[int, dict[int, Watermark]] = {}

    def classify(self, client_id: int, sender_id: int, seq: int) -> InboxStatus:
        if (sender_id, seq) in self._applied.get(client_id, ()):
            return InboxStatus.APPLIED
        watermark = self._done.get(client_id, {}).get(sender_id)
        if watermark is not None and watermark.is_duplicate(seq):
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
        watermark = self._done.setdefault(client_id, {}).setdefault(
            sender_id, Watermark()
        )
        watermark.observe(seq)

    def drop_client(self, client_id: int) -> None:
        self._applied.pop(client_id, None)
        self._done.pop(client_id, None)

    def to_dict(self) -> dict:
        return {
            "applied": {
                str(client_id): sorted(pairs)
                for client_id, pairs in self._applied.items()
            },
            "done": {
                str(client_id): {
                    str(sender_id): watermark.to_dict()
                    for sender_id, watermark in senders.items()
                }
                for client_id, senders in self._done.items()
            },
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Inbox":
        inbox = cls()
        inbox._applied = {
            int(client_id): {(sender_id, seq) for sender_id, seq in pairs}
            for client_id, pairs in data.get("applied", {}).items()
        }
        inbox._done = {
            int(client_id): {
                int(sender_id): Watermark.from_dict(wm)
                for sender_id, wm in senders.items()
            }
            for client_id, senders in data.get("done", {}).items()
        }
        return inbox
