from __future__ import annotations

from dataclasses import dataclass


@dataclass
class InputDone:
    msg_id: str
    client_id: int
    sender_id: int
    seq: int
