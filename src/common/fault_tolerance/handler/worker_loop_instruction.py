"""What handle() returns to the consumer loop, telling it the next action.

The handler owns disk and memory but not the RabbitMQ channel, so it hands the
loop an instruction: either ACK, or publish `outputs` and then call commit_done()
with `ctx` (the message's (msg_id, client_id, sender_id, seq)).
"""

from __future__ import annotations

from dataclasses import dataclass, field

from common.fault_tolerance.handler.action import Action
from common.fault_tolerance.outbox.outbox_entry import OutboxEntry


@dataclass
class WorkerLoopInstruction:
    action: Action
    outputs: list[OutboxEntry] = field(default_factory=list)
    ctx: tuple | None = None
