"""Per-client dedup brain: classifies every incoming message as NEW, APPLIED or
DONE, and tracks the transitions.

`applied` holds the input ids whose state change was applied but whose outputs
are not yet published+committed. `done` is a watermark per (client_id,
sender_id), the bounded-memory replacement for storing every finished id.

Holds no files: persistence is via to_dict()/from_dict(); transitions are driven
by PersistentStateHandler after each WAL write.
"""

from __future__ import annotations

from common.fault_tolerance.inbox.inbox_status import InboxStatus
from common.fault_tolerance.inbox.watermark import Watermark


class Inbox:
    def __init__(self) -> None:
        self._applied: dict[int, set[tuple[int, int]]] = {}
        self._done: dict[int, dict[int, Watermark]] = {}

    def classify(self, client_id: int, sender_id: int, seq: int) -> InboxStatus:
        raise NotImplementedError

    def mark_applied(self, client_id: int, sender_id: int, seq: int) -> None:
        raise NotImplementedError

    def mark_done(self, client_id: int, sender_id: int, seq: int) -> None:
        raise NotImplementedError

    def drop_client(self, client_id: int) -> None:
        raise NotImplementedError

    def to_dict(self) -> dict:
        raise NotImplementedError

    @classmethod
    def from_dict(cls, data: dict) -> "Inbox":
        raise NotImplementedError
