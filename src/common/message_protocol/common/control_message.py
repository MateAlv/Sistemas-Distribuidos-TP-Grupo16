
class ControlMessage:
    def __init__(
            self, sender_id: int, expected_total: int, processed_count: int
    ):
        self.sender_id = sender_id
        self.expected_total = expected_total
        self.processed_count = processed_count
    