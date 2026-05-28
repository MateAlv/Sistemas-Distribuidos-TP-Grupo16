import struct
from dataclasses import dataclass

from common.message_protocol.internal.transaction_serializer import TransactionSerializer

Q4_EDGE_INCOMING = 1
Q4_EDGE_OUTGOING = 2
Q4_QUALIFY_THRESHOLD = 6


@dataclass(frozen=True)
class ScatterGatherRelation:
    from_account: str
    intermediate_account: str
    to_account: str


@dataclass(frozen=True)
class ScatterGatherResult:
    from_account: str
    to_account: str


@dataclass(frozen=True)
class Q4AccountId:
    bank_id: str
    account: str


@dataclass(frozen=True)
class Q4TransactionEdge:
    source: Q4AccountId
    target: Q4AccountId


@dataclass(frozen=True)
class Q4CountedEdge:
    role: int
    intermediate: Q4AccountId
    endpoint: Q4AccountId
    count: int


@dataclass(frozen=True)
class Q4BlockJoinEdge:
    role: int
    intermediate: Q4AccountId
    endpoint: Q4AccountId
    a_bucket: int
    b_bucket: int
    count: int


@dataclass(frozen=True)
class Q4PairPaths:
    source: Q4AccountId
    target: Q4AccountId
    path_count: int


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

    @classmethod
    def serialize_batch(cls, relations: list[ScatterGatherRelation]) -> bytes:
        return b"".join(cls.serialize(relation) for relation in relations)

    @classmethod
    def deserialize_batch(cls, data: bytes) -> list[ScatterGatherRelation]:
        if len(data) % cls.SIZE != 0:
            raise ValueError(f"invalid scatter-gather relation batch size: {len(data)}")
        return [
            cls.deserialize(data[offset:offset + cls.SIZE])
            for offset in range(0, len(data), cls.SIZE)
        ]


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


class Q4AccountIdSerializer:
    BANK_SIZE = TransactionSerializer.BANK_SIZE
    ACCOUNT_SIZE = TransactionSerializer.ACCOUNT_SIZE
    FORMAT = f"!{BANK_SIZE}s{ACCOUNT_SIZE}s"
    SIZE = struct.calcsize(FORMAT)

    @classmethod
    def serialize(cls, account_id: Q4AccountId) -> bytes:
        return struct.pack(
            cls.FORMAT,
            _encode_fixed(account_id.bank_id, cls.BANK_SIZE, "bank_id"),
            _encode_fixed(account_id.account, cls.ACCOUNT_SIZE, "account"),
        )

    @classmethod
    def deserialize(cls, data: bytes) -> Q4AccountId:
        if len(data) != cls.SIZE:
            raise ValueError(f"invalid Q4 account id size: {len(data)}")
        bank_id, account = struct.unpack(cls.FORMAT, data)
        return Q4AccountId(
            bank_id=_decode_fixed(bank_id),
            account=_decode_fixed(account),
        )

    @classmethod
    def serialize_batch(cls, account_ids: list[Q4AccountId]) -> bytes:
        return b"".join(cls.serialize(account_id) for account_id in account_ids)

    @classmethod
    def deserialize_batch(cls, data: bytes) -> list[Q4AccountId]:
        return _deserialize_batch(cls, data, "Q4 account id")


class Q4TransactionEdgeSerializer:
    SIZE = Q4AccountIdSerializer.SIZE * 2

    @classmethod
    def serialize(cls, edge: Q4TransactionEdge) -> bytes:
        return (
            Q4AccountIdSerializer.serialize(edge.source)
            + Q4AccountIdSerializer.serialize(edge.target)
        )

    @classmethod
    def deserialize(cls, data: bytes) -> Q4TransactionEdge:
        if len(data) != cls.SIZE:
            raise ValueError(f"invalid Q4 transaction edge size: {len(data)}")
        account_size = Q4AccountIdSerializer.SIZE
        return Q4TransactionEdge(
            source=Q4AccountIdSerializer.deserialize(data[:account_size]),
            target=Q4AccountIdSerializer.deserialize(data[account_size:]),
        )

    @classmethod
    def serialize_batch(cls, edges: list[Q4TransactionEdge]) -> bytes:
        return b"".join(cls.serialize(edge) for edge in edges)

    @classmethod
    def deserialize_batch(cls, data: bytes) -> list[Q4TransactionEdge]:
        return _deserialize_batch(cls, data, "Q4 transaction edge")


class Q4CountedEdgeSerializer:
    FORMAT = f"!B{Q4AccountIdSerializer.SIZE}s{Q4AccountIdSerializer.SIZE}sQ"
    SIZE = struct.calcsize(FORMAT)

    @classmethod
    def serialize(cls, edge: Q4CountedEdge) -> bytes:
        _validate_q4_role(edge.role)
        return struct.pack(
            cls.FORMAT,
            int(edge.role),
            Q4AccountIdSerializer.serialize(edge.intermediate),
            Q4AccountIdSerializer.serialize(edge.endpoint),
            int(edge.count),
        )

    @classmethod
    def deserialize(cls, data: bytes) -> Q4CountedEdge:
        if len(data) != cls.SIZE:
            raise ValueError(f"invalid Q4 counted edge size: {len(data)}")
        role, intermediate, endpoint, count = struct.unpack(cls.FORMAT, data)
        _validate_q4_role(role)
        return Q4CountedEdge(
            role=role,
            intermediate=Q4AccountIdSerializer.deserialize(intermediate),
            endpoint=Q4AccountIdSerializer.deserialize(endpoint),
            count=count,
        )

    @classmethod
    def serialize_batch(cls, edges: list[Q4CountedEdge]) -> bytes:
        return b"".join(cls.serialize(edge) for edge in edges)

    @classmethod
    def deserialize_batch(cls, data: bytes) -> list[Q4CountedEdge]:
        return _deserialize_batch(cls, data, "Q4 counted edge")


class Q4BlockJoinEdgeSerializer:
    FORMAT = f"!B{Q4AccountIdSerializer.SIZE}s{Q4AccountIdSerializer.SIZE}sIIQ"
    SIZE = struct.calcsize(FORMAT)

    @classmethod
    def serialize(cls, edge: Q4BlockJoinEdge) -> bytes:
        _validate_q4_role(edge.role)
        return struct.pack(
            cls.FORMAT,
            int(edge.role),
            Q4AccountIdSerializer.serialize(edge.intermediate),
            Q4AccountIdSerializer.serialize(edge.endpoint),
            int(edge.a_bucket),
            int(edge.b_bucket),
            int(edge.count),
        )

    @classmethod
    def deserialize(cls, data: bytes) -> Q4BlockJoinEdge:
        if len(data) != cls.SIZE:
            raise ValueError(f"invalid Q4 block join edge size: {len(data)}")
        role, intermediate, endpoint, a_bucket, b_bucket, count = struct.unpack(
            cls.FORMAT, data
        )
        _validate_q4_role(role)
        return Q4BlockJoinEdge(
            role=role,
            intermediate=Q4AccountIdSerializer.deserialize(intermediate),
            endpoint=Q4AccountIdSerializer.deserialize(endpoint),
            a_bucket=a_bucket,
            b_bucket=b_bucket,
            count=count,
        )

    @classmethod
    def serialize_batch(cls, edges: list[Q4BlockJoinEdge]) -> bytes:
        return b"".join(cls.serialize(edge) for edge in edges)

    @classmethod
    def deserialize_batch(cls, data: bytes) -> list[Q4BlockJoinEdge]:
        return _deserialize_batch(cls, data, "Q4 block join edge")


class Q4PairPathsSerializer:
    FORMAT = f"!{Q4AccountIdSerializer.SIZE}s{Q4AccountIdSerializer.SIZE}sQ"
    SIZE = struct.calcsize(FORMAT)

    @classmethod
    def serialize(cls, pair_paths: Q4PairPaths) -> bytes:
        return struct.pack(
            cls.FORMAT,
            Q4AccountIdSerializer.serialize(pair_paths.source),
            Q4AccountIdSerializer.serialize(pair_paths.target),
            min(int(pair_paths.path_count), Q4_QUALIFY_THRESHOLD),
        )

    @classmethod
    def deserialize(cls, data: bytes) -> Q4PairPaths:
        if len(data) != cls.SIZE:
            raise ValueError(f"invalid Q4 pair paths size: {len(data)}")
        source, target, path_count = struct.unpack(cls.FORMAT, data)
        return Q4PairPaths(
            source=Q4AccountIdSerializer.deserialize(source),
            target=Q4AccountIdSerializer.deserialize(target),
            path_count=path_count,
        )

    @classmethod
    def serialize_batch(cls, items: list[Q4PairPaths]) -> bytes:
        return b"".join(cls.serialize(item) for item in items)

    @classmethod
    def deserialize_batch(cls, data: bytes) -> list[Q4PairPaths]:
        return _deserialize_batch(cls, data, "Q4 pair paths")


def _encode_fixed(value, size: int, field_name: str) -> bytes:
    encoded = str(value).encode("utf-8")
    if len(encoded) > size:
        raise ValueError(f"{field_name} exceeds {size} bytes: {value!r}")
    return encoded


def _decode_fixed(value: bytes) -> str:
    return value.decode("utf-8").rstrip("\x00")


def _deserialize_batch(serializer, data: bytes, label: str):
    if len(data) % serializer.SIZE != 0:
        raise ValueError(f"invalid {label} batch size: {len(data)}")
    return [
        serializer.deserialize(data[offset:offset + serializer.SIZE])
        for offset in range(0, len(data), serializer.SIZE)
    ]


def _validate_q4_role(role: int) -> None:
    if role not in (Q4_EDGE_INCOMING, Q4_EDGE_OUTGOING):
        raise ValueError(f"invalid Q4 edge role: {role}")
