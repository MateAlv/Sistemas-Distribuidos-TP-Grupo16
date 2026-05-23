import pytest

from common.message_protocol.external.types import FILE_TYPE_TRANSACTIONS
from common.message_protocol.internal import LineBatch, LineBatchSerializer


def test_line_batch_serialization_integrity():
    batch = LineBatch(
        file_type=FILE_TYPE_TRANSACTIONS,
        rel_path="input/LI-Small_Trans.csv",
        batch_id=42,
        first_line_number=2,
        header=(
            "Timestamp",
            "From Bank",
            "Account",
            "To Bank",
            "Account",
            "Amount Paid",
            "Payment Currency",
            "Payment Format",
        ),
        lines=(
            b"2022/09/01 00:08,1,abc,2,def,12.5,US Dollar,Wire\r",
            b"",
        ),
    )

    recovered = LineBatchSerializer.deserialize(LineBatchSerializer.serialize(batch))

    assert recovered == batch


def test_line_batch_serialization_accepts_empty_lines():
    batch = LineBatch(
        file_type=FILE_TYPE_TRANSACTIONS,
        rel_path="transactions.csv",
        batch_id=0,
        first_line_number=2,
        header=("Timestamp",),
        lines=(),
    )

    recovered = LineBatchSerializer.deserialize(LineBatchSerializer.serialize(batch))

    assert recovered.lines == ()


def test_line_batch_serialization_rejects_non_bytes_lines():
    batch = LineBatch(
        file_type=FILE_TYPE_TRANSACTIONS,
        rel_path="transactions.csv",
        batch_id=0,
        first_line_number=2,
        header=("Timestamp",),
        lines=("not-bytes",),
    )

    with pytest.raises(TypeError, match="line items must be bytes"):
        LineBatchSerializer.serialize(batch)


def test_line_batch_deserialize_rejects_truncated_payload():
    batch = LineBatch(
        file_type=FILE_TYPE_TRANSACTIONS,
        rel_path="transactions.csv",
        batch_id=0,
        first_line_number=2,
        header=("Timestamp",),
        lines=(b"line",),
    )
    payload = LineBatchSerializer.serialize(batch)

    with pytest.raises(ValueError, match="not enough bytes"):
        LineBatchSerializer.deserialize(payload[:-1])


def test_line_batch_deserialize_rejects_trailing_bytes():
    batch = LineBatch(
        file_type=FILE_TYPE_TRANSACTIONS,
        rel_path="transactions.csv",
        batch_id=0,
        first_line_number=2,
        header=("Timestamp",),
        lines=(b"line",),
    )
    payload = LineBatchSerializer.serialize(batch)

    with pytest.raises(ValueError, match="too many bytes"):
        LineBatchSerializer.deserialize(payload + b"x")
