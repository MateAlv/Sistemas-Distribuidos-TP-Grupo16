import struct
from dataclasses import dataclass

from common.message_protocol.internal.transaction_serializer import TransactionSerializer


@dataclass(frozen=True)
class ScatterGatherRelation:
    from_account: str
    intermediate_account: str
    to_account: str


@dataclass(frozen=True)
class ScatterGatherResult:
    from_account: str
    to_account: str


class ScatterGatherRelationSerializer:
    ACCOUNT_SIZE = TransactionSerializer.ACCOUNT_SIZE
    FORMAT = f"!{ACCOUNT_SIZE}s{ACCOUNT_SIZE}s{ACCOUNT_SIZE}s"
    SIZE = struct.calcsize(FORMAT)

    @classmethod
    def serialize(cls, relation: ScatterGatherRelation) -> bytes:
        return struct.pack(
            cls.FORMAT,
            _encode_fixed(relation.from_account, cls.ACCOUNT_SIZE, "from_account"),
            _encode_fixed(
                relation.intermediate_account,
                cls.ACCOUNT_SIZE,
                "intermediate_account",
            ),
            _encode_fixed(relation.to_account, cls.ACCOUNT_SIZE, "to_account"),
        )

    @classmethod
    def deserialize(cls, data: bytes) -> ScatterGatherRelation:
        if len(data) != cls.SIZE:
            raise ValueError(f"invalid scatter-gather relation size: {len(data)}")
        from_account, intermediate_account, to_account = struct.unpack(cls.FORMAT, data)
        return ScatterGatherRelation(
            from_account=_decode_fixed(from_account),
            intermediate_account=_decode_fixed(intermediate_account),
            to_account=_decode_fixed(to_account),
        )


class ScatterGatherResultSerializer:
    ACCOUNT_SIZE = TransactionSerializer.ACCOUNT_SIZE
    FORMAT = f"!{ACCOUNT_SIZE}s{ACCOUNT_SIZE}s"
    SIZE = struct.calcsize(FORMAT)

    @classmethod
    def serialize(cls, result: ScatterGatherResult) -> bytes:
        return struct.pack(
            cls.FORMAT,
            _encode_fixed(result.from_account, cls.ACCOUNT_SIZE, "from_account"),
            _encode_fixed(result.to_account, cls.ACCOUNT_SIZE, "to_account"),
        )

    @classmethod
    def deserialize(cls, data: bytes) -> ScatterGatherResult:
        if len(data) != cls.SIZE:
            raise ValueError(f"invalid scatter-gather result size: {len(data)}")
        from_account, to_account = struct.unpack(cls.FORMAT, data)
        return ScatterGatherResult(
            from_account=_decode_fixed(from_account),
            to_account=_decode_fixed(to_account),
        )


def _encode_fixed(value, size: int, field_name: str) -> bytes:
    encoded = str(value).encode("utf-8")
    if len(encoded) > size:
        raise ValueError(f"{field_name} exceeds {size} bytes: {value!r}")
    return encoded


def _decode_fixed(value: bytes) -> str:
    return value.decode("utf-8").rstrip("\x00")
