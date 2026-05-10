import socket

from common.message_protocol.external.socket_utils import recv_exact


MAX_UINT32 = 2**32 - 1
MAX_UINT64 = 2**64 - 1


def _validate_uint(name: str, value: int, max_value: int) -> int:
    value = int(value)
    if value < 0:
        raise ValueError(f"{name} must be greater than or equal to 0")
    if value > max_value:
        raise ValueError(f"{name} must be less than or equal to {max_value}")
    return value


class FileChunkHeader:
    # Wire layout: client_id(4) | payload_size(4) | path_size(4) | offset(8) | rel_path(N)
    HEADER_SIZE = 20

    def __init__(
        self,
        rel_path: str,
        client_id: int,
        offset: int,
        payload_size: int,
    ) -> None:
        self.rel_path = rel_path
        self.client_id = _validate_uint("client_id", client_id, MAX_UINT32)
        self.offset = _validate_uint("offset", offset, MAX_UINT64)
        self.payload_size = _validate_uint("payload_size", payload_size, MAX_UINT32)
        self.path_size = _validate_uint(
            "path_size",
            self.path_size_for(rel_path),
            MAX_UINT32,
        )

    @staticmethod
    def path_size_for(rel_path: str) -> int:
        return len(rel_path.encode("utf-8"))

    @classmethod
    def serialized_size_for_path(cls, rel_path: str) -> int:
        return cls.HEADER_SIZE + cls.path_size_for(rel_path)

    def serialized_size(self) -> int:
        return self.serialized_size_for_path(self.rel_path)

    def serialize(self) -> bytes:
        return b"".join(
            [
                self.client_id.to_bytes(4, byteorder="big"),
                self.payload_size.to_bytes(4, byteorder="big"),
                self.path_size.to_bytes(4, byteorder="big"),
                self.offset.to_bytes(8, byteorder="big"),
                self.rel_path.encode("utf-8"),
            ]
        )

    @classmethod
    def deserialize(cls, data: bytes) -> "FileChunkHeader":
        if len(data) < cls.HEADER_SIZE:
            raise ValueError("not enough bytes for FileChunkHeader")

        client_id = int.from_bytes(data[0:4], byteorder="big")
        payload_size = int.from_bytes(data[4:8], byteorder="big")
        path_size = int.from_bytes(data[8:12], byteorder="big")
        offset = int.from_bytes(data[12:20], byteorder="big")

        end = cls.HEADER_SIZE + path_size
        if len(data) < end:
            raise ValueError("not enough bytes for FileChunkHeader path")

        rel_path = data[cls.HEADER_SIZE:end].decode("utf-8")
        return cls(
            rel_path=rel_path,
            client_id=client_id,
            offset=offset,
            payload_size=payload_size,
        )

    @classmethod
    def recv(cls, sock: socket.socket) -> "FileChunkHeader":
        fixed = recv_exact(sock, cls.HEADER_SIZE)
        path_size = int.from_bytes(fixed[8:12], byteorder="big")
        path_bytes = recv_exact(sock, path_size)
        return cls.deserialize(fixed + path_bytes)


class FileChunk:
    def __init__(self, rel_path: str, client_id: int, offset: int, data: bytes) -> None:
        self.header = FileChunkHeader(
            rel_path=rel_path,
            client_id=client_id,
            offset=offset,
            payload_size=len(data),
        )
        self.data = data

    def path(self) -> str:
        return self.header.rel_path

    def client_id(self) -> int:
        return self.header.client_id

    def payload_size(self) -> int:
        return self.header.payload_size

    def offset(self) -> int:
        return self.header.offset

    def payload(self) -> bytes:
        return self.data

    def serialized_size(self) -> int:
        return self.header.serialized_size() + self.payload_size()

    def message_size(self, message_type_size: int) -> int:
        return int(message_type_size) + self.serialized_size()

    def serialize(self) -> bytes:
        return self.header.serialize() + self.data

    @classmethod
    def serialized_size_for(cls, rel_path: str, payload_size: int) -> int:
        return FileChunkHeader.serialized_size_for_path(rel_path) + int(payload_size)

    @classmethod
    def message_overhead_for(cls, rel_path: str, message_type_size: int) -> int:
        return int(message_type_size) + FileChunkHeader.serialized_size_for_path(rel_path)

    @classmethod
    def max_payload_size_for_message(
        cls,
        rel_path: str,
        max_message_size: int,
        message_type_size: int,
    ) -> int:
        max_message_size = int(max_message_size)
        message_type_size = int(message_type_size)

        if max_message_size <= 0:
            raise ValueError("max_message_size must be greater than 0")
        if message_type_size <= 0:
            raise ValueError("message_type_size must be greater than 0")

        overhead = cls.message_overhead_for(rel_path, message_type_size)
        max_payload_size = max_message_size - overhead
        if max_payload_size <= 0:
            raise ValueError(
                "max_message_size leaves no room for payload "
                f"(max_message_size={max_message_size}, overhead={overhead}, "
                f"rel_path={rel_path!r})"
            )

        return max_payload_size

    @classmethod
    def deserialize(cls, data: bytes) -> "FileChunk":
        header = FileChunkHeader.deserialize(data)
        start = FileChunkHeader.HEADER_SIZE + header.path_size
        end = start + header.payload_size

        if len(data) < end:
            raise ValueError("not enough bytes for FileChunk payload")

        return cls(
            rel_path=header.rel_path,
            client_id=header.client_id,
            offset=header.offset,
            data=data[start:end],
        )

    @classmethod
    def recv(cls, sock: socket.socket) -> "FileChunk":
        header = FileChunkHeader.recv(sock)
        payload = recv_exact(sock, header.payload_size)
        return cls(
            rel_path=header.rel_path,
            client_id=header.client_id,
            offset=header.offset,
            data=payload,
        )
