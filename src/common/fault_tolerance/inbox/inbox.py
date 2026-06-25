"""Per-client dedup: classifies each incoming message as NEW, APPLIED or DONE.

The dedup key is (client_id, kind, sender_id, seq). kind separates DATA from
control messages so they never share a bucket when sender_id and seq collide.
applied holds triples whose change was applied but whose outputs are not yet
committed; done is a bounded tracker per (client_id, kind, sender_id) that
replaces storing every finished id. Persists via to_dict()/from_dict().
"""

from __future__ import annotations

from common.fault_tolerance.inbox.inbox_status import InboxStatus
from common.fault_tolerance.inbox.deduplication_tracker import DeduplicationTracker
from common.fault_tolerance.inbox.msg_kind import MsgKind


class Inbox:
    def __init__(self) -> None:
        # client_id -> set of (kind, sender_id, seq)
        self._applied: dict[int, set[tuple[int, int, int]]] = {}
        # client_id -> (kind, sender_id) -> DeduplicationTracker
        self._done: dict[int, dict[tuple[int, int], DeduplicationTracker]] = {}

    def classify(
        self, client_id: int, sender_id: int, seq: int, kind: MsgKind = MsgKind.DATA
    ) -> InboxStatus:
        if (int(kind), sender_id, seq) in self._applied.get(client_id, ()):
            return InboxStatus.APPLIED
        tracker = self._done.get(client_id, {}).get((int(kind), sender_id))
        if tracker is not None and tracker.is_duplicate(seq):
            return InboxStatus.DONE
        return InboxStatus.NEW

    def mark_applied(
        self, client_id: int, sender_id: int, seq: int, kind: MsgKind = MsgKind.DATA
    ) -> None:
        self._applied.setdefault(client_id, set()).add((int(kind), sender_id, seq))

    def mark_done(
        self, client_id: int, sender_id: int, seq: int, kind: MsgKind = MsgKind.DATA
    ) -> None:
        applied = self._applied.get(client_id)
        if applied is not None:
            applied.discard((int(kind), sender_id, seq))
            if not applied:
                del self._applied[client_id]
        tracker = self._done.setdefault(client_id, {}).setdefault(
            (int(kind), sender_id), DeduplicationTracker()
        )
        tracker.observe(seq)

    def drop_client(self, client_id: int) -> None:
        self._applied.pop(client_id, None)
        self._done.pop(client_id, None)

    def to_dict(self) -> dict:
        return {
            "applied": {
                client_id: sorted(triples)
                for client_id, triples in self._applied.items()
            },
            "done": {
                client_id: {
                    f"{kind_val}:{sender_id}": tracker.to_dict()
                    for (kind_val, sender_id), tracker in senders.items()
                }
                for client_id, senders in self._done.items()
            },
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Inbox":
        inbox = cls()
        inbox._applied = {
            client_id: {(kind_val, sender_id, seq) for kind_val, sender_id, seq in triples}
            for client_id, triples in data.get("applied", {}).items()
        }
        inbox._done = {
            client_id: {
                _parse_done_key(key): DeduplicationTracker.from_dict(wm)
                for key, wm in senders.items()
            }
            for client_id, senders in data.get("done", {}).items()
        }
        return inbox


def _parse_done_key(key) -> tuple[int, int]:
    """Parse a (kind, sender_id) key, serialized as a "kind:sender_id" string."""
    if isinstance(key, str):
        kind_val, sender_id = key.split(":", 1)
        return int(kind_val), int(sender_id)
    # A bare int key carries no kind, so default it to DATA.
    return int(MsgKind.DATA), int(key)
