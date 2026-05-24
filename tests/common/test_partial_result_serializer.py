import pytest

from common.constants import C_Q2, C_Q3
from common.domain.account import Q2BankMaxResult
from common.domain.partial_result import Q2BankMaxPartial, Q3PaymentFormatPartial
from common.message_protocol.internal.partial_result_serializer import (
    Q2BankMaxPartialSerializer,
    Q2BankMaxResultSerializer,
    Q3PaymentFormatPartialSerializer,
    partial_serializer_for,
)


def test_q2_bank_max_partial_roundtrip():
    partial = Q2BankMaxPartial(
        bank_id="001120",
        from_account="8006AA910",
        amount=592571.0,
    )

    recovered = Q2BankMaxPartialSerializer.deserialize(
        Q2BankMaxPartialSerializer.serialize(partial)
    )

    assert recovered == partial


def test_q2_bank_max_result_roundtrip():
    result = Q2BankMaxResult(
        bank_id="001120",
        from_account="8006AA910",
        bank_name="First Bank of Portland",
        amount=592571.0,
    )

    recovered = Q2BankMaxResultSerializer.deserialize(
        Q2BankMaxResultSerializer.serialize(result)
    )

    assert recovered == result


def test_q3_payment_format_partial_roundtrip():
    partial = Q3PaymentFormatPartial(
        payment_format="Credit Card",
        amount_sum=15.75,
        count=3,
    )

    recovered = Q3PaymentFormatPartialSerializer.deserialize(
        Q3PaymentFormatPartialSerializer.serialize(partial)
    )

    assert recovered == partial


def test_partial_serializer_dispatches_by_configuration():
    assert partial_serializer_for(C_Q2) is Q2BankMaxPartialSerializer
    assert partial_serializer_for(C_Q3) is Q3PaymentFormatPartialSerializer

    with pytest.raises(ValueError):
        partial_serializer_for("Q1")


def test_partial_serializer_rejects_oversized_fields():
    partial = Q2BankMaxPartial(
        bank_id="x" * (Q2BankMaxPartialSerializer.BANK_SIZE + 1),
        from_account="8006AA910",
        amount=1.0,
    )

    with pytest.raises(ValueError):
        Q2BankMaxPartialSerializer.serialize(partial)


def test_q2_bank_max_result_serializer_rejects_oversized_bank_name():
    result = Q2BankMaxResult(
        bank_id="001120",
        from_account="8006AA910",
        bank_name="x" * (Q2BankMaxResultSerializer.BANK_NAME_SIZE + 1),
        amount=1.0,
    )

    with pytest.raises(ValueError):
        Q2BankMaxResultSerializer.serialize(result)
