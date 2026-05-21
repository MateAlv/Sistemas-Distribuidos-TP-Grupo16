import struct

from common.message_protocol.internal.common.control_message import ControlMessage


class ControlMessageSerializer:
    FORMAT = "!3I"
    SIZE = struct.calcsize(FORMAT)

    @classmethod
    def serialize(cls, cm: ControlMessage) -> bytes:
        return struct.pack(
            cls.FORMAT,
            int(cm.sender_id),
            int(cm.expected_total),
            int(cm.processed_count)
        )

    @classmethod
    def deserialize(cls, data: bytes) -> ControlMessage:
        vals = struct.unpack(cls.FORMAT, data)
        return ControlMessage(
            sender_id=vals[0],
            expected_total=vals[1],
            processed_count=vals[2]
        )
    
