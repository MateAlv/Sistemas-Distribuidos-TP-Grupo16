import struct

from common.message_protocol.common.control_message import ControlMessage

class ControlMessageSerializer:
    FORMAT = "!5I"
    SIZE = struct.calcsize(FORMAT)

    @classmethod
    def serialize(cls, cm: ControlMessage) -> bytes:
        return struct.pack(
            cls.FORMAT,
            int(cm.message_type),
            int(cm.client_id),
            int(cm.sender_id),
            int(cm.expected_total),
            int(cm.processed_count)
        )

    @classmethod
    def deserialize(cls, data: bytes) -> ControlMessage:
        vals = struct.unpack(cls.FORMAT, data)
        return ControlMessage(
            message_type=vals[0],
            client_id=vals[1],
            sender_id=vals[2],
            expected_total=vals[3],
            processed_count=vals[4]
        )
    