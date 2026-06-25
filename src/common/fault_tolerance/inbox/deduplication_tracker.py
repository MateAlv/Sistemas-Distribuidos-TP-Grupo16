"""Bounded dedup tracker for one (client_id, sender_id) pair.

Keeps the highest seq seen (biggest) plus the gaps below it not yet arrived
(pending). A seq is a duplicate when it is at or below biggest and not pending.

_MAX_SEQUENTIAL_GAP caps how many gap entries observe() pre-fills. Hash-derived
dedup keys give each message a fresh tracker whose first seq can be near 2**32;
filling that gap would allocate billions of ints and OOM. Past the cap we just
advance biggest without filling, which can later mark a skipped seq as a
duplicate, but for hash-derived keys a colliding sender_id is negligibly rare.
"""

from __future__ import annotations

_MAX_SEQUENTIAL_GAP = 65_536


class DeduplicationTracker:
    def __init__(self, biggest: int = 0, pending: set[int] | None = None) -> None:
        self.biggest = biggest
        self.pending = pending if pending is not None else set()

    def is_duplicate(self, seq: int) -> bool:
        return seq <= self.biggest and seq not in self.pending

    def observe(self, seq: int) -> None:
        if seq > self.biggest:
            gap = seq - self.biggest - 1
            if gap <= _MAX_SEQUENTIAL_GAP:
                self.pending.update(range(self.biggest + 1, seq))
            self.biggest = seq
        else:
            self.pending.discard(seq)

    def to_dict(self) -> dict:
        return {"biggest": self.biggest, "pending": sorted(self.pending)}

    @classmethod
    def from_dict(cls, data: dict) -> "DeduplicationTracker":
        return cls(biggest=data["biggest"], pending=set(data["pending"]))
