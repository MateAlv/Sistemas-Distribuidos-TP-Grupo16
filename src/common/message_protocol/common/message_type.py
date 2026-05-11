from enum import IntEnum

class MessageType(IntEnum):
    DATA = 0
    EOF = 1
    EOF_RECEIVED = 2
    PROCESSED_REQUEST = 3
    PROCESSED_ANSWER = 4
    FLUSH_ORDER = 5
    FLUSH_ACK = 6
