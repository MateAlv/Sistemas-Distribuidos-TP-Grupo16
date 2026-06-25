"""
Low-level binary encoding helpers shared by WAL and outbox serialization.
"""

import struct

UINT8_FORMAT  = ">B"
UINT16_FORMAT = ">H"
UINT32_FORMAT = ">I"
UINT8_SIZE  = struct.calcsize(UINT8_FORMAT)
UINT16_SIZE = struct.calcsize(UINT16_FORMAT)
UINT32_SIZE = struct.calcsize(UINT32_FORMAT)

MAX_UINT16 = 2**16 - 1
MAX_UINT32 = 2**32 - 1


def read_exact(data: bytes, offset: int, n: int) -> tuple[bytes, int]:
    end = offset + n
    if end > len(data):
        raise ValueError(
            f"truncated data: need {n} bytes at offset {offset}, have {len(data) - offset}"
        )
    return data[offset:end], end


def read_uint8(data: bytes, offset: int) -> tuple[int, int]:
    raw, new_offset = read_exact(data, offset, UINT8_SIZE)
    return struct.unpack(UINT8_FORMAT, raw)[0], new_offset


def read_uint16(data: bytes, offset: int) -> tuple[int, int]:
    raw, new_offset = read_exact(data, offset, UINT16_SIZE)
    return struct.unpack(UINT16_FORMAT, raw)[0], new_offset


def read_uint32(data: bytes, offset: int) -> tuple[int, int]:
    raw, new_offset = read_exact(data, offset, UINT32_SIZE)
    return struct.unpack(UINT32_FORMAT, raw)[0], new_offset


def read_length_prefixed_u16(data: bytes, offset: int) -> tuple[bytes, int]:
    length, offset = read_uint16(data, offset)
    return read_exact(data, offset, length)


def read_length_prefixed_u32(data: bytes, offset: int) -> tuple[bytes, int]:
    length, offset = read_uint32(data, offset)
    return read_exact(data, offset, length)
