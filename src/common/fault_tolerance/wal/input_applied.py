from __future__ import annotations

from dataclasses import dataclass, field

from common.fault_tolerance.inbox.msg_kind import MsgKind
from common.fault_tolerance.outbox.outbox_entry import OutboxEntry


@dataclass
class InputApplied:
    msg_id: str
    client_id: int
    sender_id: int
    seq: int
    state_change: dict
    outputs: list[OutboxEntry]
    kind: MsgKind = field(default=MsgKind.DATA)
