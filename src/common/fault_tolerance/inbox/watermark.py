"""Bounded dedup tracker for one (client_id, sender_id) pair.

Keeps the highest seq seen (`biggest`) plus the gaps below it that have not
arrived yet (`pending`). A seq is a duplicate when it is at or below `biggest`
and not one of the pending gaps. This bounds memory: gaps fill in as messages
arrive, and the whole tracker is dropped when the client closes.
"""

from __future__ import annotations


class Watermark:
    def __init__(self, biggest: int = 0, pending: set[int] | None = None) -> None:
        self.biggest = biggest
        self.pending = pending or set()

    def is_duplicate(self, seq: int) -> bool:
        raise NotImplementedError

    def observe(self, seq: int) -> None:
        raise NotImplementedError
