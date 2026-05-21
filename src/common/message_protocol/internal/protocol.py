import hashlib
import struct

from common.message_protocol.internal.common.message_type import MessageType


def partition_for_key(key: str, partitions: int) -> int:
    if partitions <= 0:
        raise ValueError("partitions must be greater than 0")

    digest = hashlib.sha256(str(key).encode("utf-8")).digest()
    return int.from_bytes(digest, "big") % partitions


def partition_for_pair(left: str, right: str, partitions: int) -> int:
    left = str(left)
    right = str(right)
    return partition_for_key(f"{len(left)}:{left}{len(right)}:{right}", partitions)


class InternalProtocol:
    HEADER_FORMAT = "!B 16s"
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

    @classmethod
    def create_packet(cls, msg_type: MessageType, client_id_bytes: bytes, payload: bytes) -> bytes:
        header = struct.pack(cls.HEADER_FORMAT, msg_type, client_id_bytes)
        return header + payload

    @classmethod
    def unpack_packet(cls, packet: bytes):
        header_data = packet[:cls.HEADER_SIZE]
        payload = packet[cls.HEADER_SIZE:]
        msg_type, client_id_bytes = struct.unpack(cls.HEADER_FORMAT, header_data)
        client_id = int.from_bytes(client_id_bytes, byteorder="big")
        return msg_type, client_id, payload
