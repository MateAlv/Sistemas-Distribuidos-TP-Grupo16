from enum import IntEnum

class MessageType(IntEnum):
    DATA = 0
    EOF = 1
    PROCESSED_REQUEST = 2
    PROCESSED_ANSWER = 3
    FLUSH_ORDER = 4
    FLUSH_ACK = 5
    AGGREGATED_COUNT = 6
