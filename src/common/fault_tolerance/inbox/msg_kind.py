from enum import IntEnum


class MsgKind(IntEnum):
    DATA = 0
    CTRL_FLUSH_ORDER = 1
    CTRL_FLUSH_ACK = 2
    CTRL_UPSTREAM_EOF = 3
    CTRL_EOF_RECEIVED = 4
    CTRL_PROCESSED_ANSWER = 5
    # q3_barrier joins two independent streams in one inbox; these separate them so
    # an averages and a candidates message can't collide on (client, sender, seq).
    Q3_AVERAGES = 6
    Q3_CANDIDATES = 7
