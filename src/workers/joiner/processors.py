from common.constants import C_Q2, C_Q3, C_Q5
from common.domain.partial_result import Q2BankMaxPartial
from common.message_protocol.internal.aggregation_serializer import AggregationSerializer
from common.message_protocol.internal.partial_result_serializer import (
    Q2BankMaxPartialSerializer,
    Q3AverageResultSerializer,
)


class JoinerProcessor:
    def accept(self, payload: bytes) -> None:
        raise NotImplementedError

    def results(self) -> list[bytes]:
        raise NotImplementedError


class Q2JoinerProcessor(JoinerProcessor):
    """Reduce global de máximos por banco a partir de los parciales de cada shard."""

    def __init__(self) -> None:
        self.max_by_bank: dict[str, Q2BankMaxPartial] = {}

    def accept(self, payload: bytes) -> None:
        partial = Q2BankMaxPartialSerializer.deserialize(payload)
        current = self.max_by_bank.get(partial.bank_id)
        if current is None or partial.amount > current.amount:
            self.max_by_bank[partial.bank_id] = partial

    def results(self) -> list[bytes]:
        return [
            Q2BankMaxPartialSerializer.serialize(p)
            for p in self.max_by_bank.values()
        ]


class Q5JoinerProcessor(JoinerProcessor):
    """Suma los counts parciales de cada shard del Aggregator Q5."""

    def __init__(self) -> None:
        self.total = 0

    def accept(self, payload: bytes) -> None:
        self.total += AggregationSerializer.deserialize(payload)

    def results(self) -> list[bytes]:
        return [AggregationSerializer.serialize(self.total)]


class Q3JoinerProcessor(JoinerProcessor):
    """Colecciona Q3AverageResult de cada shard y los reenvía al BarrierFilter.
    """

    def __init__(self) -> None:
        self.payloads: list[bytes] = []

    def accept(self, payload: bytes) -> None:
        self.payloads.append(payload)

    def results(self) -> list[bytes]:
        return list(self.payloads)


def create_joiner_processor(configuration: str) -> JoinerProcessor:
    if configuration == C_Q2:
        return Q2JoinerProcessor()
    if configuration == C_Q3:
        return Q3JoinerProcessor()
    if configuration == C_Q5:
        return Q5JoinerProcessor()
    raise ValueError(f"Invalid joiner processor configuration: {configuration}")
