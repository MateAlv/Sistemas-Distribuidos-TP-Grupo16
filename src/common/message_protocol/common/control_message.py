
from enum import IntEnum

class ControlMessageType(IntEnum):
    EOF_RECEIVED = 1
    PROCESSED_REQUEST = 2
    PROCESSED_ANSWER = 3
    FLUSH_ORDER = 4
    FLUSH_ACK = 5

class ControlMessage:
    def __init__(
            self, message_type: ControlMessageType, client_id: int, sender_id: int, expected_total: int, processed_count: int
    ):
        self.message_type = message_type
        self.client_id = client_id
        self.sender_id = sender_id
        self.expected_total = expected_total
        self.processed_count = processed_count
    