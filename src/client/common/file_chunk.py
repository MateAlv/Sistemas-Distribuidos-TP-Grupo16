import socket

from common.socket_utils import recv_exact


class FileChunkHeader:
    HEADER_SIZE = 20

    def __init__(
        self,
        rel_path: str,
        client_id: int,
        offset: int,
        payload_size: int,
    ) -> None:
        self.rel_path = rel_path
        self.client_id = int(client_id)
        self.offset = int(offset)
        self.payload_size = int(payload_size)
        self.path_size = len(rel_path.encode("utf-8"))

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
        fixed_header = recv_exact(sock, cls.HEADER_SIZE)

        client_id = int.from_bytes(fixed_header[0:4], byteorder="big")
        payload_size = int.from_bytes(fixed_header[4:8], byteorder="big")
        path_size = int.from_bytes(fixed_header[8:12], byteorder="big")
        offset = int.from_bytes(fixed_header[12:20], byteorder="big")
        rel_path = recv_exact(sock, path_size).decode("utf-8")

        return cls(
            rel_path=rel_path,
            client_id=client_id,
            offset=offset,
            payload_size=payload_size,
        )


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

    def serialize(self) -> bytes:
        return self.header.serialize() + self.data

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
