from common.domain.transaction import Transaction
from workers.sum.processors import Q2SumProcessor, Q3SumProcessor


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
