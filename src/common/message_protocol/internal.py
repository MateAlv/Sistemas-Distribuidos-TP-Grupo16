import json
import struct
from common.message_protocol.common.message_type import MessageType
from common.domain.transaction import Transaction
from common.message_protocol.control_message_serializer import ControlMessageSerializer
from common.message_protocol.transaction_serializer import TransactionSerializer


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
