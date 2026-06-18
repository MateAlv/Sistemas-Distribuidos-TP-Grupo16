"""Per-client pending downstream messages, kept until their input is committed.

Keyed as outbox[client_id][input_id] -> [OutboxEntry], so a whole input's
outputs are added and removed together. Entries are stored ready to publish.

Binary layout produced by serialize():
    u32 client_count
    per client:
        u32 client_id
        u32 input_count
        per input:
            u16 len + input_id
            u16 entry_count
            per entry: OutboxEntry.serialize()
"""

from __future__ import annotations

import struct

from common.fault_tolerance._encoding import (
    UINT16_FORMAT,
    UINT32_FORMAT,
    read_length_prefixed_u16,
    read_uint16,
    read_uint32,
)
from common.fault_tolerance.outbox.outbox_entry import OutboxEntry


class Outbox:
    def __init__(self) -> None:
        self._pending: dict[int, dict[str, list[OutboxEntry]]] = {}

    def add(self, client_id: int, input_id: str, entries: list[OutboxEntry]) -> None:
        self._pending.setdefault(client_id, {})[input_id] = list(entries)

    def entries_for_input(self, client_id: int, input_id: str) -> list[OutboxEntry]:
        return self._pending.get(client_id, {}).get(input_id, [])

    def remove_input(self, client_id: int, input_id: str) -> None:
        inputs = self._pending.get(client_id)
        if inputs is None:
            return
        inputs.pop(input_id, None)
        if not inputs:
            del self._pending[client_id]

    def all_pending(self) -> list[OutboxEntry]:
        return [
            entry
            for inputs in self._pending.values()
            for entries in inputs.values()
            for entry in entries
        ]

    def drop_client(self, client_id: int) -> None:
        self._pending.pop(client_id, None)

    def serialize(self) -> bytes:
        chunks = [struct.pack(UINT32_FORMAT, len(self._pending))]
        for client_id, inputs in self._pending.items():
            chunks.append(struct.pack(UINT32_FORMAT, client_id))
            chunks.append(struct.pack(UINT32_FORMAT, len(inputs)))
            for input_id, entries in inputs.items():
                input_id_bytes = input_id.encode()
                chunks.append(struct.pack(UINT16_FORMAT, len(input_id_bytes)))
                chunks.append(input_id_bytes)
                chunks.append(struct.pack(UINT16_FORMAT, len(entries)))
                chunks.extend(entry.serialize() for entry in entries)
        return b"".join(chunks)

    @classmethod
    def deserialize(cls, data: bytes) -> "Outbox":
        outbox = cls()
        offset = 0
        client_count, offset = read_uint32(data, offset)
        for _ in range(client_count):
            client_id, offset = read_uint32(data, offset)
            input_count, offset = read_uint32(data, offset)
            inputs: dict[str, list[OutboxEntry]] = {}
            for _ in range(input_count):
                input_id_bytes, offset = read_length_prefixed_u16(data, offset)
                entry_count, offset = read_uint16(data, offset)
                entries = []
                for _ in range(entry_count):
                    entry, offset = OutboxEntry.deserialize(data, offset)
                    entries.append(entry)
                inputs[input_id_bytes.decode()] = entries
            outbox._pending[client_id] = inputs
        return outbox
