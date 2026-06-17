"""Inbox: per-client dedup brain.

Answers NEW / APPLIED / DONE for every incoming message (data AND control,
decision D5). inbox_done is implemented as the watermark tracker (decision D1):
per (client_id, sender_id) keep `biggest` + `pending` holes. inbox_applied is a
small set of (sender_id, seq) currently applied-but-not-committed (decision D2).

Does NO file IO: to_dict()/from_dict() only. LastState serializes it; WAL differences
are fed back via mark_applied/mark_done by PersistentStateHandler.
"""

from __future__ import annotations

from common.fault_tolerance.records import InboxStatus


class _Watermark:
    """biggest + pending messages  for one (client_id, sender_id). DONE tracker."""

    def __init__(self, biggest: int = 0, pending: set[int] | None = None) -> None:
        self.biggest = biggest
        self.pending = pending or set()

    def is_duplicate(self, seq: int) -> bool:
        raise NotImplementedError  # seq <= biggest and seq not in pending

    def observe(self, seq: int) -> None:
        raise NotImplementedError  # advance biggest (filling pending) or discard hole


class Inbox:
    def __init__(self) -> None:
        # client_id -> set[(sender_id, seq)]
        self._applied: dict[int, set[tuple[int, int]]] = {}
        # client_id -> sender_id -> _Watermark
        self._done: dict[int, dict[int, _Watermark]] = {}

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
