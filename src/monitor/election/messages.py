import struct
from dataclasses import dataclass
from enum import IntEnum


class ElectionMessageType(IntEnum):
    ELECTION = 0x00
    OK = 0x01
    COORDINATOR = 0x02


@dataclass(frozen=True)
class ElectionMessage:
    message_type: ElectionMessageType
    epoch: int
    sender_id: int

    FORMAT = "!BHB"
    SIZE = struct.calcsize(FORMAT)
    MAX_EPOCH = 2**16 - 1
    MAX_SENDER_ID = 2**8 - 1

    def __post_init__(self) -> None:
        if not isinstance(self.message_type, ElectionMessageType):
            raise TypeError("message_type must be an ElectionMessageType")
        if not isinstance(self.epoch, int):
            raise TypeError("epoch must be an int")
        if not 0 <= self.epoch <= self.MAX_EPOCH:
            raise ValueError(f"epoch must be in range [0, {self.MAX_EPOCH}]")
        if not isinstance(self.sender_id, int):
            raise TypeError("sender_id must be an int")
        if not 0 <= self.sender_id <= self.MAX_SENDER_ID:
            raise ValueError(
                f"sender_id must be in range [0, {self.MAX_SENDER_ID}]"
            )

    def serialize(self) -> bytes:
        return struct.pack(
            self.FORMAT,
            self.message_type,
            self.epoch,
            self.sender_id,
        )

    @classmethod
    def deserialize(cls, data: bytes) -> "ElectionMessage":
        if len(data) != cls.SIZE:
            raise ValueError(
                f"election message must be exactly {cls.SIZE} bytes, got {len(data)}"
            )

        raw_type, epoch, sender_id = struct.unpack(cls.FORMAT, data)
        try:
            message_type = ElectionMessageType(raw_type)
        except ValueError as exc:
            raise ValueError(f"unknown election message type: {raw_type}") from exc

        return cls(
            message_type=message_type,
            epoch=epoch,
            sender_id=sender_id,
        )
