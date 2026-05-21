import pytest

from common.constants import C_Q2, C_Q3
from common.domain.transaction import Transaction
from common.message_protocol.internal.partial_result_serializer import (
    Q2BankMaxPartialSerializer,
    Q3PaymentFormatPartialSerializer,
)
from workers.sum.processors import (
    Q2SumProcessor,
    Q3SumProcessor,
    create_sum_processor,
)


def transaction(
    from_bank="001",
    from_account="ACC1",
    amount=10.0,
    payment_format="ACH",
) -> Transaction:
    return Transaction(
        date="2022/09/01 00:00",
        from_bank=from_bank,
        from_account=from_account,
        to_bank="999",
        to_account="ACC9",
        amount=amount,
        currency="US Dollar",
        format=payment_format,
    )


def test_q2_processor_keeps_max_transaction_per_bank():
    processor = Q2SumProcessor()

    processor.process(transaction(from_bank="001", from_account="LOW", amount=10.0))
    processor.process(transaction(from_bank="001", from_account="HIGH", amount=25.0))
    processor.process(transaction(from_bank="002", from_account="OTHER", amount=7.0))
    processor.process(transaction(from_bank="001", from_account="TIE", amount=25.0))

    assert processor.max_by_bank["001"] == {
        "bank_id": "001",
        "from_account": "HIGH",
        "amount": 25.0,
    }
    assert processor.max_by_bank["002"] == {
        "bank_id": "002",
        "from_account": "OTHER",
        "amount": 7.0,
    }


def test_q3_processor_accumulates_amount_and_count_by_payment_format():
    processor = Q3SumProcessor()

    processor.process(transaction(amount=10.0, payment_format="ACH"))
    processor.process(transaction(amount=5.0, payment_format="ACH"))
    processor.process(transaction(amount=7.0, payment_format="Wire"))

    assert processor.amount_sum_by_payment_format == {
        "ACH": 15.0,
        "Wire": 7.0,
    }
    assert processor.count_by_payment_format == {
        "ACH": 2,
        "Wire": 1,
    }


def test_create_sum_processor_dispatches_by_configuration():
    assert isinstance(create_sum_processor(C_Q2), Q2SumProcessor)
    assert isinstance(create_sum_processor(C_Q3), Q3SumProcessor)

    with pytest.raises(ValueError):
        create_sum_processor("Q1")


def test_processors_emit_no_partials_when_empty():
    assert Q2SumProcessor().partials() == []
    assert Q3SumProcessor().partials() == []


def test_q2_processor_emits_serialized_partials_by_bank():
    processor = Q2SumProcessor()

    processor.process(transaction(from_bank="001", from_account="LOW", amount=10.0))
    processor.process(transaction(from_bank="001", from_account="HIGH", amount=25.0))
    processor.process(transaction(from_bank="002", from_account="OTHER", amount=7.0))

    partials = processor.partials()
    decoded_by_key = {
        key: Q2BankMaxPartialSerializer.deserialize(payload)
        for key, payload in partials
    }

    assert set(decoded_by_key) == {"001", "002"}
    assert decoded_by_key["001"].bank_id == "001"
    assert decoded_by_key["001"].from_account == "HIGH"
    assert decoded_by_key["001"].amount == 25.0
    assert decoded_by_key["002"].bank_id == "002"
    assert decoded_by_key["002"].from_account == "OTHER"
    assert decoded_by_key["002"].amount == 7.0


def test_q2_processor_emits_single_late_transaction_partial():
    processor = Q2SumProcessor()

    partials = processor.partials_for_transaction(
        transaction(from_bank="001", from_account="ACC1", amount=8.5)
    )

    assert len(partials) == 1
    key, payload = partials[0]
    decoded = Q2BankMaxPartialSerializer.deserialize(payload)
    assert key == "001"
    assert decoded.bank_id == "001"
    assert decoded.from_account == "ACC1"
    assert decoded.amount == 8.5


def test_q3_processor_emits_serialized_partials_by_payment_format():
    processor = Q3SumProcessor()

    processor.process(transaction(amount=10.0, payment_format="ACH"))
    processor.process(transaction(amount=5.0, payment_format="ACH"))
    processor.process(transaction(amount=7.0, payment_format="Wire"))

    partials = processor.partials()
    decoded_by_key = {
        key: Q3PaymentFormatPartialSerializer.deserialize(payload)
        for key, payload in partials
    }

    assert set(decoded_by_key) == {"ACH", "Wire"}
    assert decoded_by_key["ACH"].payment_format == "ACH"
    assert decoded_by_key["ACH"].amount_sum == 15.0
    assert decoded_by_key["ACH"].count == 2
    assert decoded_by_key["Wire"].payment_format == "Wire"
    assert decoded_by_key["Wire"].amount_sum == 7.0
    assert decoded_by_key["Wire"].count == 1


def test_q3_processor_emits_single_late_transaction_partial():
    processor = Q3SumProcessor()

    partials = processor.partials_for_transaction(
        transaction(amount=3.25, payment_format="Cheque")
    )

    assert len(partials) == 1
    key, payload = partials[0]
    decoded = Q3PaymentFormatPartialSerializer.deserialize(payload)
    assert key == "Cheque"
    assert decoded.payment_format == "Cheque"
    assert decoded.amount_sum == 3.25
    assert decoded.count == 1
