from common.message_protocol.external.types import FILE_TYPE_NAMES


MAX_UINT8 = 2**8 - 1
MAX_UINT32 = 2**32 - 1


def _validate_uint(name: str, value: int, max_value: int) -> int:
    value = int(value)
    if value < 0:
        raise ValueError(f"{name} must be greater than or equal to 0")
    if value > max_value:
        raise ValueError(f"{name} must be less than or equal to {max_value}")
    return value


def _validate_file_type(file_type: int) -> int:
    file_type = _validate_uint("file_type", file_type, MAX_UINT8)
    if file_type not in FILE_TYPE_NAMES:
        raise ValueError(f"unknown file_type: {file_type}")
    return file_type


class FileEof:
    # Wire layout: client_id(4) | file_type(1) | path_size(4) | rel_path(N)
    HEADER_SIZE = 9

    def __init__(self, rel_path: str, client_id: int, file_type: int) -> None:
        self.rel_path = rel_path
        self._client_id = _validate_uint("client_id", client_id, MAX_UINT32)
        self._file_type = _validate_file_type(file_type)
        self._path_size = _validate_uint(
            "path_size",
            self.path_size_for(rel_path),
            MAX_UINT32,
        )

    @staticmethod
    def path_size_for(rel_path: str) -> int:
        return len(rel_path.encode("utf-8"))

    def path(self) -> str:
        return self.rel_path

    def client_id(self) -> int:
        return self._client_id

    def file_type(self) -> int:
        return self._file_type

    def serialize(self) -> bytes:
        return b"".join(
            [
                self._client_id.to_bytes(4, byteorder="big"),
                self._file_type.to_bytes(1, byteorder="big"),
                self._path_size.to_bytes(4, byteorder="big"),
                self.rel_path.encode("utf-8"),
            ]
        )

    @classmethod
    def deserialize(cls, data: bytes) -> "FileEof":
        if len(data) < cls.HEADER_SIZE:
            raise ValueError("not enough bytes for FileEof")

        client_id = int.from_bytes(data[0:4], byteorder="big")
        file_type = int.from_bytes(data[4:5], byteorder="big")
        path_size = int.from_bytes(data[5:9], byteorder="big")

        end = cls.HEADER_SIZE + path_size
        if len(data) < end:
            raise ValueError("not enough bytes for FileEof path")
        if len(data) != end:
            raise ValueError("too many bytes for FileEof")

        rel_path = data[cls.HEADER_SIZE:end].decode("utf-8")
        return cls(rel_path=rel_path, client_id=client_id, file_type=file_type)
