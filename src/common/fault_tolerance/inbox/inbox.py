"""Per-client dedup brain: classifies every incoming message as NEW, APPLIED or
DONE, and tracks the transitions.

`applied` holds the (sender_id, seq) pairs whose state change was applied but
whose outputs are not yet published+committed. `done` is a watermark per
(client_id, sender_id), the bounded-memory replacement for storing every
finished id.

Holds no files: persistence is via binary serialize()/deserialize(); transitions
are driven by PersistentStateHandler after each WAL write.

Binary layout produced by serialize() is two sections, applied then done:
    applied: u32 client_count, per client:
        u32 client_id, u32 pair_count, per pair: u32 sender_id, u32 seq
    done: u32 client_count, per client:
        u32 client_id, u32 sender_count, per sender: u32 sender_id, Watermark
"""

from __future__ import annotations

import struct

from common.fault_tolerance._encoding import UINT32_FORMAT, read_uint32
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

    def serialize(self) -> bytes:
        chunks = [struct.pack(UINT32_FORMAT, len(self._applied))]
        for client_id, pairs in self._applied.items():
            chunks.append(struct.pack(UINT32_FORMAT, client_id))
            chunks.append(struct.pack(UINT32_FORMAT, len(pairs)))
            for sender_id, seq in sorted(pairs):
                chunks.append(struct.pack(UINT32_FORMAT, sender_id))
                chunks.append(struct.pack(UINT32_FORMAT, seq))

        chunks.append(struct.pack(UINT32_FORMAT, len(self._done)))
        for client_id, senders in self._done.items():
            chunks.append(struct.pack(UINT32_FORMAT, client_id))
            chunks.append(struct.pack(UINT32_FORMAT, len(senders)))
            for sender_id, watermark in senders.items():
                chunks.append(struct.pack(UINT32_FORMAT, sender_id))
                chunks.append(watermark.serialize())
        return b"".join(chunks)

    @classmethod
    def deserialize(cls, data: bytes) -> "Inbox":
        inbox = cls()
        offset = 0

        applied_clients, offset = read_uint32(data, offset)
        for _ in range(applied_clients):
            client_id, offset = read_uint32(data, offset)
            pair_count, offset = read_uint32(data, offset)
            pairs = set()
            for _ in range(pair_count):
                sender_id, offset = read_uint32(data, offset)
                seq, offset = read_uint32(data, offset)
                pairs.add((sender_id, seq))
            inbox._applied[client_id] = pairs

        done_clients, offset = read_uint32(data, offset)
        for _ in range(done_clients):
            client_id, offset = read_uint32(data, offset)
            sender_count, offset = read_uint32(data, offset)
            senders: dict[int, Watermark] = {}
            for _ in range(sender_count):
                sender_id, offset = read_uint32(data, offset)
                watermark, offset = Watermark.deserialize(data, offset)
                senders[sender_id] = watermark
            inbox._done[client_id] = senders
        return inbox
