import json
import struct
from common.domain.transaction import Transaction


class InternalProtocol:
    HEADER_FORMAT = "!B 16s" 
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

    @classmethod
    def create_packet(cls, msg_type: int, client_id_bytes: bytes, payload: bytes) -> bytes:
        header = struct.pack(cls.HEADER_FORMAT, msg_type, client_id_bytes)
        return header + payload

    @classmethod
    def unpack_packet(cls, packet: bytes):
        header_data = packet[:cls.HEADER_SIZE]
        payload = packet[cls.HEADER_SIZE:]
        msg_type, client_id = struct.unpack(cls.HEADER_FORMAT, header_data)
        return msg_type, client_id, payload
