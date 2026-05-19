import struct


class AggregationSerializer:
    FORMAT = "!Q"
    SIZE = struct.calcsize(FORMAT)

    @classmethod
    def serialize(cls, count: int) -> bytes:
        return struct.pack(cls.FORMAT, int(count))

    @classmethod
    def deserialize(cls, data: bytes) -> int:
        (count,) = struct.unpack(cls.FORMAT, data)
        return count