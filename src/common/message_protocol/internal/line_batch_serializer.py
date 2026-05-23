from dataclasses import dataclass


MAX_UINT8 = 2**8 - 1
MAX_UINT32 = 2**32 - 1
MAX_UINT64 = 2**64 - 1


@dataclass(frozen=True)
class LineBatch:
    file_type: int
    rel_path: str
    batch_id: int
    first_line_number: int
    header: tuple[str, ...]
    lines: tuple[bytes, ...]


class LineBatchSerializer:
    # Wire layout:
    # file_type(1) | batch_id(8) | first_line_number(8) | path_size(4)
    # | header_count(4) | line_count(4) | rel_path(N)
    # | repeated header_size(4) + header(N)
    # | repeated line_size(4) + line(N)
    FIXED_HEADER_SIZE = 29

    @classmethod
    def serialize(cls, batch: LineBatch) -> bytes:
        file_type = _validate_uint("file_type", batch.file_type, MAX_UINT8)
        batch_id = _validate_uint("batch_id", batch.batch_id, MAX_UINT64)
        first_line_number = _validate_uint(
            "first_line_number",
            batch.first_line_number,
            MAX_UINT64,
        )
        path_bytes = _encode_sized(batch.rel_path, "rel_path")
        header = tuple(batch.header)
        lines = tuple(batch.lines)

        return b"".join(
            [
                file_type.to_bytes(1, byteorder="big"),
                batch_id.to_bytes(8, byteorder="big"),
                first_line_number.to_bytes(8, byteorder="big"),
                len(path_bytes).to_bytes(4, byteorder="big"),
                _validate_uint("header_count", len(header), MAX_UINT32).to_bytes(
                    4,
                    byteorder="big",
                ),
                _validate_uint("line_count", len(lines), MAX_UINT32).to_bytes(
                    4,
                    byteorder="big",
                ),
                path_bytes,
                _serialize_string_items(header, "header"),
                _serialize_byte_items(lines, "line"),
            ]
        )

    @classmethod
    def deserialize(cls, data: bytes) -> LineBatch:
        if len(data) < cls.FIXED_HEADER_SIZE:
            raise ValueError("not enough bytes for LineBatch")

        file_type = int.from_bytes(data[0:1], byteorder="big")
        batch_id = int.from_bytes(data[1:9], byteorder="big")
        first_line_number = int.from_bytes(data[9:17], byteorder="big")
        path_size = int.from_bytes(data[17:21], byteorder="big")
        header_count = int.from_bytes(data[21:25], byteorder="big")
        line_count = int.from_bytes(data[25:29], byteorder="big")

        offset = cls.FIXED_HEADER_SIZE
        rel_path_bytes, offset = _read_sized(data, offset, path_size, "rel_path")
        header, offset = _read_string_items(data, offset, header_count, "header")
        lines, offset = _read_byte_items(data, offset, line_count, "line")

        if offset != len(data):
            raise ValueError("too many bytes for LineBatch")

        return LineBatch(
            file_type=file_type,
            rel_path=rel_path_bytes.decode("utf-8"),
            batch_id=batch_id,
            first_line_number=first_line_number,
            header=tuple(header),
            lines=tuple(lines),
        )


def _validate_uint(name: str, value: int, max_value: int) -> int:
    value = int(value)
    if value < 0:
        raise ValueError(f"{name} must be greater than or equal to 0")
    if value > max_value:
        raise ValueError(f"{name} must be less than or equal to {max_value}")
    return value


def _encode_sized(value: str, field_name: str) -> bytes:
    encoded = str(value).encode("utf-8")
    _validate_uint(f"{field_name}_size", len(encoded), MAX_UINT32)
    return encoded


def _serialize_string_items(items: tuple[str, ...], field_name: str) -> bytes:
    return b"".join(
        len(encoded).to_bytes(4, byteorder="big") + encoded
        for encoded in (_encode_sized(item, field_name) for item in items)
    )


def _serialize_byte_items(items: tuple[bytes, ...], field_name: str) -> bytes:
    parts = []
    for item in items:
        if not isinstance(item, bytes):
            raise TypeError(f"{field_name} items must be bytes")
        _validate_uint(f"{field_name}_size", len(item), MAX_UINT32)
        parts.append(len(item).to_bytes(4, byteorder="big") + item)
    return b"".join(parts)


def _read_sized(
    data: bytes,
    offset: int,
    size: int,
    field_name: str,
) -> tuple[bytes, int]:
    end = offset + size
    if end > len(data):
        raise ValueError(f"not enough bytes for LineBatch {field_name}")
    return data[offset:end], end


def _read_string_items(
    data: bytes,
    offset: int,
    count: int,
    field_name: str,
) -> tuple[list[str], int]:
    raw_items, offset = _read_byte_items(data, offset, count, field_name)
    return [item.decode("utf-8") for item in raw_items], offset


def _read_byte_items(
    data: bytes,
    offset: int,
    count: int,
    field_name: str,
) -> tuple[list[bytes], int]:
    items = []
    for _ in range(count):
        size_bytes, offset = _read_sized(data, offset, 4, f"{field_name}_size")
        size = int.from_bytes(size_bytes, byteorder="big")
        item, offset = _read_sized(data, offset, size, field_name)
        items.append(item)
    return items, offset
