import struct

from common.message_protocol.internal.common.message_type import MessageType
from common.routing import shard_for_key_parts, stable_digest_shard


def partition_for_key(key: str, partitions: int) -> int:
    if partitions <= 0:
        raise ValueError("partitions must be greater than 0")
    return stable_digest_shard(str(key), partitions)


def partition_for_pair(left: str, right: str, partitions: int) -> int:
    left = str(left)
    right = str(right)
    return partition_for_parts((left, right), partitions)


def partition_for_parts(parts, partitions: int) -> int:
    if partitions <= 0:
        raise ValueError("partitions must be greater than 0")
    return shard_for_key_parts(parts, partitions)


class InternalProtocol:
    HEADER_FORMAT = "!B 16s"
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)
    ADDRESSED_HEADER_FORMAT = "!B 16s I I"
    ADDRESSED_HEADER_SIZE = struct.calcsize(ADDRESSED_HEADER_FORMAT)

    @classmethod
    def create_packet(cls, msg_type: MessageType, client_id_bytes: bytes, payload: bytes) -> bytes:
        header = struct.pack(cls.HEADER_FORMAT, msg_type, client_id_bytes)
        return header + payload

    @classmethod
    def create_addressed_packet(
        cls,
        msg_type: MessageType,
        client_id_bytes: bytes,
        sender_id: int,
        seq: int,
        payload: bytes,
    ) -> bytes:
        header = struct.pack(
            cls.ADDRESSED_HEADER_FORMAT,
            msg_type,
            client_id_bytes,
            sender_id,
            seq,
        )
        return header + payload

    @classmethod
    def unpack_packet(cls, packet: bytes):
        header_data = packet[:cls.HEADER_SIZE]
        payload = packet[cls.HEADER_SIZE:]
        msg_type, client_id_bytes = struct.unpack(cls.HEADER_FORMAT, header_data)
        client_id = int.from_bytes(client_id_bytes, byteorder="big")
        return msg_type, client_id, payload

    @classmethod
    def unpack_addressed_packet(cls, packet: bytes):
        header_data = packet[:cls.ADDRESSED_HEADER_SIZE]
        payload = packet[cls.ADDRESSED_HEADER_SIZE:]
        msg_type, client_id_bytes, sender_id, seq = struct.unpack(
            cls.ADDRESSED_HEADER_FORMAT,
            header_data,
        )
        client_id = int.from_bytes(client_id_bytes, byteorder="big")
        return msg_type, client_id, sender_id, seq, payload
