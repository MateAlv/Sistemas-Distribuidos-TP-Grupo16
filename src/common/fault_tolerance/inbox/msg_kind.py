from enum import IntEnum


class MsgKind(IntEnum):
    DATA = 0
    CTRL_FLUSH_ORDER = 1
    CTRL_FLUSH_ACK = 2
    CTRL_UPSTREAM_EOF = 3
    CTRL_EOF_RECEIVED = 4
