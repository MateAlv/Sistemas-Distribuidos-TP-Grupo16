"""Bounded dedup tracker for one (client_id, sender_id) pair.

Keeps the highest seq seen (`biggest`) plus the gaps below it that have not
arrived yet (`pending`). A seq is a duplicate when it is at or below `biggest`
and not one of the pending gaps. This bounds memory: gaps fill in as messages
arrive, and the whole tracker is dropped when the client closes.
"""

from __future__ import annotations


class DeduplicationTracker:
    def __init__(self, biggest: int = 0, pending: set[int] | None = None) -> None:
        self.biggest = biggest
        self.pending = pending if pending is not None else set()

    def is_duplicate(self, seq: int) -> bool:
        return seq <= self.biggest and seq not in self.pending

    def observe(self, seq: int) -> None:
        if seq > self.biggest:
            self.pending.update(range(self.biggest + 1, seq))
            self.biggest = seq
        else:
            self.pending.discard(seq)

    def to_dict(self) -> dict:
        return {"biggest": self.biggest, "pending": sorted(self.pending)}

    @classmethod
    def from_dict(cls, data: dict) -> "DeduplicationTracker":
        return cls(biggest=data["biggest"], pending=set(data["pending"]))
