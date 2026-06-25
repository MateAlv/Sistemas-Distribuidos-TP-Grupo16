from __future__ import annotations

import struct
from dataclasses import dataclass

from common.fault_tolerance._encoding import (
    MAX_UINT16,
    MAX_UINT32,
    UINT16_FORMAT,
    UINT32_FORMAT,
    read_length_prefixed_u16,
    read_length_prefixed_u32,
    read_uint32,
)


@dataclass
class OutboxEntry:
    output_id: str    # deterministic id: "{node_id}:{client_id}:{input_seq}#{index}"
    input_id: str     # the input that produced it
    destination: str  # publisher-registry key (logical edge / queue name)
    body: bytes       # fully serialized message, resent byte-for-byte on recovery
    shard: int | None = None  # explicit destination partition; None = let the publisher route

    def serialize(self) -> bytes:
        output_id_bytes = self.output_id.encode()
        input_id_bytes = self.input_id.encode()
        destination_bytes = self.destination.encode()
        if len(output_id_bytes) > MAX_UINT16:
            raise ValueError("output_id too long")
        if len(input_id_bytes) > MAX_UINT16:
            raise ValueError("input_id too long")
        if len(destination_bytes) > MAX_UINT16:
            raise ValueError("destination too long")
        if len(self.body) > MAX_UINT32:
            raise ValueError("body too long")
        return (
            struct.pack(UINT16_FORMAT, len(output_id_bytes)) + output_id_bytes
            + struct.pack(UINT16_FORMAT, len(input_id_bytes)) + input_id_bytes
            + struct.pack(UINT16_FORMAT, len(destination_bytes)) + destination_bytes
            + struct.pack(UINT32_FORMAT, len(self.body)) + self.body
            + struct.pack(UINT32_FORMAT, _encode_shard(self.shard))
        )

    @classmethod
    def deserialize(cls, data: bytes, offset: int = 0) -> tuple[OutboxEntry, int]:
        output_id_bytes, offset = read_length_prefixed_u16(data, offset)
        input_id_bytes, offset = read_length_prefixed_u16(data, offset)
        destination_bytes, offset = read_length_prefixed_u16(data, offset)
        body, offset = read_length_prefixed_u32(data, offset)
        shard_encoded, offset = read_uint32(data, offset)
        return (
            cls(
                output_id=output_id_bytes.decode(),
                input_id=input_id_bytes.decode(),
                destination=destination_bytes.decode(),
                body=body,
                shard=_decode_shard(shard_encoded),
            ),
            offset,
        )


# shard is stored as shard+1 so 0 can mean "no explicit shard" (None).
def _encode_shard(shard: int | None) -> int:
    return 0 if shard is None else int(shard) + 1


def _decode_shard(encoded: int) -> int | None:
    return None if encoded == 0 else encoded - 1
