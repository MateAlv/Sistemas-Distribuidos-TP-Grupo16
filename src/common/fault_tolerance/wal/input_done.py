from __future__ import annotations

from dataclasses import dataclass, field

from common.fault_tolerance.inbox.msg_kind import MsgKind


@dataclass
class InputDone:
    msg_id: str
    client_id: int
    sender_id: int
    seq: int
    kind: MsgKind = field(default=MsgKind.DATA)
