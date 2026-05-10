import struct
from src.common.message_protocol.transaction_serializer import TransactionSerializer
from asyncio import IncompleteReadError


class ExternalMsgType:
    DATA_BATCH = 1
    EOF = 2
    ACK = 3


def send_batch(socket, transactions: list):
    payload = TransactionSerializer.serialize_batch(transactions)
    header = struct.pack("!B I", ExternalMsgType.DATA_BATCH, len(payload))
    socket.sendall(header + payload)

    
def recv_msg(socket):
    header = _recv_sized(socket, 5)
    msg_type, length = struct.unpack("!B I", header)
    payload = _recv_sized(socket, length)
    
    if msg_type == ExternalMsgType.DATA_BATCH:
        return msg_type, TransactionSerializer.deserialize_batch(payload)
    return msg_type, None


def _recv_sized(socket, size):
    buf = bytearray(size)
    pos = 0
    while pos < size:
        n = socket.recv_into(memoryview(buf)[pos:])
        if n == 0:
            raise IncompleteReadError(bytes(buf[:pos]), size)
        pos += n
    return bytes(buf)


def recv_msg(socket):
    try:
        header = _recv_sized(socket, 5)
        msg_type, length = struct.unpack("!B I", header)
        payload = _recv_sized(socket, length)
        
        if msg_type == ExternalMsgType.DATA_BATCH:
            return msg_type, TransactionSerializer.deserialize_batch(payload)
        
        return msg_type, None
    except IncompleteReadError:
        return None, None


def _recv_sized(socket, size):
    buf = bytearray(size)
    pos = 0
    while pos < size:
        n = socket.recv_into(memoryview(buf)[pos:])
        if n == 0:
            raise IncompleteReadError(bytes(buf[:pos]), size)
        pos += n
    return bytes(buf)