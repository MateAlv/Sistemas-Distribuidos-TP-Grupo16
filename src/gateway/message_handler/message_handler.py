import uuid
from common import message_protocol


class MessageHandler:

    def __init__(self):
        self.client_id = str(uuid.uuid4())
        self.message_count = 0

    def serialize_data_message(self, message):
        [fruit, amount] = message
        self.message_count += 1
        return message_protocol.internal.serialize([self.client_id, fruit, amount])

    def serialize_eof_message(self, message):
        return message_protocol.internal.serialize([self.client_id, self.message_count])

    def deserialize_result_message(self, message):
        fields = message_protocol.internal.deserialize(message)
        if fields[0] != self.client_id:
            return None
        return fields[1]
