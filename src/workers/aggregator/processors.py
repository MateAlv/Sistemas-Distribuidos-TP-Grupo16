from common.constants import C_Q2, C_Q3, C_Q5
from common.bank_ids import notebook_bank_id
from common.domain.partial_result import Q2BankMaxPartial, Q3AverageResult
from common.message_protocol.internal.aggregation_serializer import AggregationSerializer
from common.message_protocol.internal.partial_result_serializer import (
    Q2BankMaxPartialSerializer,
    Q3AverageResultSerializer,
    Q3PaymentFormatPartialSerializer,
)


class AggregatorProcessor:
    """Reducción final por cliente. `accept` consume un DATA del upstream;
    `results` devuelve los payloads a emitir al recibir el EOF."""

    def accept(self, payload: bytes) -> None:
        raise NotImplementedError

    def results(self) -> list[bytes]:
        raise NotImplementedError


class Q2AggregatorProcessor(AggregatorProcessor):
    """Máximo monto por banco emisor (parciales del Sum Q2)."""

    def __init__(self) -> None:
        self.max_by_bank: dict[str, Q2BankMaxPartial] = {}

    def accept(self, payload: bytes) -> None:
        partial = Q2BankMaxPartialSerializer.deserialize(payload)
        bank_id = notebook_bank_id(partial.bank_id)
        if bank_id != partial.bank_id:
            partial = Q2BankMaxPartial(
                bank_id=bank_id,
                from_account=partial.from_account,
                amount=partial.amount,
                row_number=partial.row_number,
            )
        current = self.max_by_bank.get(partial.bank_id)
        if _is_better_q2_partial(partial, current):
            self.max_by_bank[partial.bank_id] = partial

    def results(self) -> list[bytes]:
        return [
            Q2BankMaxPartialSerializer.serialize(partial)
            for partial in self.max_by_bank.values()
        ]


class Q3AggregatorProcessor(AggregatorProcessor):
    """Promedio (sum / count) por payment_format (parciales del Sum Q3)."""

    def __init__(self) -> None:
        self.amount_sum_by_format: dict[str, float] = {}
        self.count_by_format: dict[str, int] = {}

    def accept(self, payload: bytes) -> None:
        partial = Q3PaymentFormatPartialSerializer.deserialize(payload)
        pf = partial.payment_format
        self.amount_sum_by_format[pf] = (
            self.amount_sum_by_format.get(pf, 0.0) + partial.amount_sum
        )
        self.count_by_format[pf] = (
            self.count_by_format.get(pf, 0) + partial.count
        )

    def results(self) -> list[bytes]:
        payloads = []
        for pf, amount_sum in self.amount_sum_by_format.items():
            count = self.count_by_format[pf]
            average = amount_sum / count if count else 0.0
            payloads.append(
                Q3AverageResultSerializer.serialize(
                    Q3AverageResult(payment_format=pf, average=average)
                )
            )
        return payloads


def _is_better_q2_partial(
    candidate: Q2BankMaxPartial,
    current: Q2BankMaxPartial | None,
) -> bool:
    if current is None:
        return True
    if candidate.amount != current.amount:
        return candidate.amount > current.amount
    if candidate.row_number and current.row_number:
        return candidate.row_number < current.row_number
    return False


class Q5AggregatorProcessor(AggregatorProcessor):
    """Conteo de transacciones. Q5 no tiene Sum: el Filter Q5 USD<1 envía
    cada Transaction que pasó, así que cada DATA suma 1."""

    def __init__(self) -> None:
        self.count = 0

    def accept(self, payload: bytes) -> None:
        self.count += 1

    def results(self) -> list[bytes]:
        return [AggregationSerializer.serialize(self.count)]


def create_aggregator_processor(configuration: str) -> AggregatorProcessor:
    if configuration == C_Q2:
        return Q2AggregatorProcessor()
    if configuration == C_Q3:
        return Q3AggregatorProcessor()
    if configuration == C_Q5:
        return Q5AggregatorProcessor()
    raise ValueError(f"Invalid aggregator processor configuration: {configuration}")
