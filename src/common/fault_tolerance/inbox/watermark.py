"""Bounded dedup tracker for one (client_id, sender_id) pair.

Keeps the highest seq seen (`biggest`) plus the gaps below it that have not
arrived yet (`pending`). A seq is a duplicate when it is at or below `biggest`
and not one of the pending gaps. This bounds memory: gaps fill in as messages
arrive, and the whole tracker is dropped when the client closes.
"""

from __future__ import annotations

import struct

from common.fault_tolerance._encoding import UINT32_FORMAT, read_uint32


class Watermark:
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

    def serialize(self) -> bytes:
        chunks = [
            struct.pack(UINT32_FORMAT, self.biggest),
            struct.pack(UINT32_FORMAT, len(self.pending)),
        ]
        chunks.extend(struct.pack(UINT32_FORMAT, seq) for seq in sorted(self.pending))
        return b"".join(chunks)

    @classmethod
    def deserialize(cls, data: bytes, offset: int = 0) -> tuple["Watermark", int]:
        biggest, offset = read_uint32(data, offset)
        pending_count, offset = read_uint32(data, offset)
        pending = set()
        for _ in range(pending_count):
            seq, offset = read_uint32(data, offset)
            pending.add(seq)
        return cls(biggest=biggest, pending=pending), offset
