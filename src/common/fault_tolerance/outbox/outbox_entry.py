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
)


@dataclass
class OutboxEntry:
    output_id: str    # deterministic id: "{node_id}:{client_id}:{input_seq}#{index}"
    input_id: str     # the input that produced it
    destination: str  # queue name or routing key
    body: bytes       # fully serialized message, resent byte-for-byte on recovery

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
        )

    @classmethod
    def deserialize(cls, data: bytes, offset: int = 0) -> tuple[OutboxEntry, int]:
        output_id_bytes, offset = read_length_prefixed_u16(data, offset)
        input_id_bytes, offset = read_length_prefixed_u16(data, offset)
        destination_bytes, offset = read_length_prefixed_u16(data, offset)
        body, offset = read_length_prefixed_u32(data, offset)
        return (
            cls(
                output_id=output_id_bytes.decode(),
                input_id=input_id_bytes.decode(),
                destination=destination_bytes.decode(),
                body=body,
            ),
            offset,
        )
