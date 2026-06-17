from __future__ import annotations

from dataclasses import dataclass


@dataclass
class OutboxEntry:
    output_id: str    # deterministic id: "{node_id}:{client_id}:{input_seq}#{index}"
    input_id: str     # the input that produced it
    destination: str  # queue name or routing key
    body: bytes       # fully serialized message, resent byte-for-byte on recovery
