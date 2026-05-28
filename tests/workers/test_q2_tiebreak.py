from common.domain.partial_result import Q2BankMaxPartial
from common.message_protocol.internal.partial_result_serializer import (
    Q2BankMaxPartialSerializer,
)
from workers.aggregator.processors import Q2AggregatorProcessor
from workers.joiner.processors import Q2JoinerProcessor
from workers.sum.processors import Q2SumProcessor


def _serialized(partial: Q2BankMaxPartial) -> bytes:
    return Q2BankMaxPartialSerializer.serialize(partial)


def _result(processor):
    [payload] = processor.results()
    return Q2BankMaxPartialSerializer.deserialize(payload)


def test_q2_sum_keeps_earliest_row_on_equal_amounts():
    processor = Q2SumProcessor()

    processor.process(
        _transaction(from_account="later", amount=100.0, row_number=20)
    )
    processor.process(
        _transaction(from_account="earlier", amount=100.0, row_number=10)
    )

    assert processor.max_by_bank["1"]["from_account"] == "earlier"
    assert processor.max_by_bank["1"]["row_number"] == 10


def test_q2_aggregator_keeps_earliest_row_on_equal_amounts():
    processor = Q2AggregatorProcessor()

    processor.accept(
        _serialized(
            Q2BankMaxPartial(
                bank_id="1", from_account="later", amount=100.0, row_number=20
            )
        )
    )
    processor.accept(
        _serialized(
            Q2BankMaxPartial(
                bank_id="1", from_account="earlier", amount=100.0, row_number=10
            )
        )
    )

    result = _result(processor)
    assert result.from_account == "earlier"
    assert result.row_number == 10


def test_q2_joiner_keeps_earliest_row_on_equal_amounts():
    processor = Q2JoinerProcessor()

    processor.accept(
        _serialized(
            Q2BankMaxPartial(
                bank_id="1", from_account="later", amount=100.0, row_number=20
            )
        )
    )
    processor.accept(
        _serialized(
            Q2BankMaxPartial(
                bank_id="1", from_account="earlier", amount=100.0, row_number=10
            )
        )
    )

    result = _result(processor)
    assert result.from_account == "earlier"
    assert result.row_number == 10


def _transaction(from_account: str, amount: float, row_number: int):
    from common.domain.transaction import Transaction

    return Transaction(
        date="2022/09/01 00:00",
        from_bank="001",
        from_account=from_account,
        to_bank="999",
        to_account="target",
        amount=amount,
        currency="US Dollar",
        format="ACH",
        row_number=row_number,
    )
