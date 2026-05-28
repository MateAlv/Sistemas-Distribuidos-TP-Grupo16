import pytest

from common.message_protocol.internal.protocol import (
    partition_for_pair,
    partition_for_parts,
)
from common.message_protocol.internal.scatter_gather_serializer import (
    Q4_EDGE_INCOMING,
    Q4_EDGE_OUTGOING,
    Q4_QUALIFY_THRESHOLD,
    Q4AccountId,
    Q4AccountIdSerializer,
    Q4BlockJoinEdge,
    Q4BlockJoinEdgeSerializer,
    Q4CountedEdge,
    Q4CountedEdgeSerializer,
    Q4PairDelta,
    Q4PairDeltaSerializer,
    Q4TransactionEdge,
    Q4TransactionEdgeSerializer,
)


def account(bank="001", value="ACC") -> Q4AccountId:
    return Q4AccountId(bank_id=bank, account=value)


def test_q4_account_id_roundtrip():
    original = account("991001", "Q4NBA")

    recovered = Q4AccountIdSerializer.deserialize(
        Q4AccountIdSerializer.serialize(original)
    )

    assert recovered == original


def test_q4_transaction_edge_roundtrip_batch():
    edges = [
        Q4TransactionEdge(source=account("1", "A"), target=account("2", "M")),
        Q4TransactionEdge(source=account("2", "M"), target=account("3", "B")),
    ]

    recovered = Q4TransactionEdgeSerializer.deserialize_batch(
        Q4TransactionEdgeSerializer.serialize_batch(edges)
    )

    assert recovered == edges


def test_q4_counted_edge_roundtrip_and_rejects_invalid_role():
    original = Q4CountedEdge(
        role=Q4_EDGE_INCOMING,
        intermediate=account("2", "M"),
        endpoint=account("1", "A"),
        count=7,
    )

    recovered = Q4CountedEdgeSerializer.deserialize(
        Q4CountedEdgeSerializer.serialize(original)
    )

    assert recovered == original

    with pytest.raises(ValueError, match="invalid Q4 edge role"):
        Q4CountedEdgeSerializer.serialize(
            Q4CountedEdge(
                role=99,
                intermediate=account("2", "M"),
                endpoint=account("1", "A"),
                count=1,
            )
        )


def test_q4_block_join_edge_roundtrip():
    original = Q4BlockJoinEdge(
        role=Q4_EDGE_OUTGOING,
        intermediate=account("2", "M"),
        endpoint=account("3", "B"),
        a_bucket=4,
        b_bucket=9,
        count=11,
    )

    recovered = Q4BlockJoinEdgeSerializer.deserialize(
        Q4BlockJoinEdgeSerializer.serialize(original)
    )

    assert recovered == original


def test_q4_pair_delta_caps_weight_at_threshold():
    original = Q4PairDelta(
        source=account("1", "A"),
        target=account("3", "B"),
        weight=99,
    )

    recovered = Q4PairDeltaSerializer.deserialize(
        Q4PairDeltaSerializer.serialize(original)
    )

    assert recovered == Q4PairDelta(
        source=account("1", "A"),
        target=account("3", "B"),
        weight=Q4_QUALIFY_THRESHOLD,
    )


def test_q4_batch_deserializer_rejects_partial_record():
    with pytest.raises(ValueError, match="invalid Q4 account id batch size"):
        Q4AccountIdSerializer.deserialize_batch(b"x")


def test_partition_for_parts_matches_existing_pair_partition():
    assert partition_for_parts(("A", "B"), 13) == partition_for_pair("A", "B", 13)

    with pytest.raises(ValueError, match="partitions must be greater than 0"):
        partition_for_parts(("A",), 0)
