import struct
from enum import IntEnum

from common.fault_tolerance._encoding import (
    UINT16_FORMAT,
    UINT16_SIZE,
    UINT32_FORMAT,
    UINT32_SIZE,
)

HEADER_FORMAT = ">BI"
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

__all__ = [
    "HEADER_FORMAT",
    "HEADER_SIZE",
    "RecordType",
    "UINT16_FORMAT",
    "UINT16_SIZE",
    "UINT32_FORMAT",
    "UINT32_SIZE",
]


class RecordType(IntEnum):
    INPUT_APPLIED = 0x01
    INPUT_DONE = 0x02
    CLIENT_CLEANUP_STARTED = 0x03
    EOF_SENT = 0x04
    CHECKPOINT = 0x05
