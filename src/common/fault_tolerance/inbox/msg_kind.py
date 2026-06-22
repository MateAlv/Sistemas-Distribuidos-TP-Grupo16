from enum import IntEnum


class MsgKind(IntEnum):
    DATA = 0
    CTRL_FLUSH_ORDER = 1
    CTRL_FLUSH_ACK = 2
    # Broadcast-mode coordination: an upstream replica's EOF_RECEIVED fan-out.
    # Separates these from DATA in the inbox so a control sender_id that happens
    # to equal an upstream worker's id does not collide with data seqs.
    CTRL_EOF_RECEIVED = 3
