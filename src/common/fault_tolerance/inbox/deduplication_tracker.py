"""Bounded dedup tracker for one (client_id, sender_id) pair.

Keeps the highest seq seen (`biggest`) plus the gaps below it that have not
arrived yet (`pending`). A seq is a duplicate when it is at or below `biggest`
and not one of the pending gaps. This bounds memory: gaps fill in as messages
arrive, and the whole tracker is dropped when the client closes.

For near-sequential message IDs (e.g. worker-assigned 0, 1, 2, …) the gap
between consecutive calls to observe() is typically 0 or 1 and pending stays
tiny. For workers that derive the dedup key from a hash (e.g. sha256 of the
raw message) the sender_id varies per message, so each message gets its own
fresh tracker and the first observe() call sees a gap of up to 2**32.
Populating pending for such a huge gap would allocate billions of integers and
cause an OOM kill. _MAX_SEQUENTIAL_GAP caps the fill: if the gap is larger
than the cap we only advance `biggest` without pre-populating pending. Any seq
value in the skipped gap that later arrives will be mis-classified as DONE
(seq ≤ biggest, not in pending) — this is safe for hash-derived keys because
the probability of two different messages sharing the same sender_id prefix is
negligible (~1/2**32 per pair).
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
