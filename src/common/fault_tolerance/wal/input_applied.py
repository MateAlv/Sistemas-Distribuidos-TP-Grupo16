from __future__ import annotations

from dataclasses import dataclass

from common.fault_tolerance.outbox.outbox_entry import OutboxEntry


@dataclass
class InputApplied:
    msg_id: str
    client_id: int
    sender_id: int
    seq: int
    state_change: dict
    outputs: list[OutboxEntry]
