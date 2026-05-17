from common.constants import C_Q2, C_Q3


class SumProcessor:
    def process(self, transaction) -> None:
        pass

    def partials(self):
        return []

    def partials_for_transaction(self, transaction):
        return []


class Q2SumProcessor(SumProcessor):
    def __init__(self) -> None:
        self.max_by_bank = {}


class Q3SumProcessor(SumProcessor):
    def __init__(self) -> None:
        self.amount_sum_by_payment_format = {}
        self.count_by_payment_format = {}


def create_sum_processor(configuration: str) -> SumProcessor:
    if configuration == C_Q2:
        return Q2SumProcessor()
    if configuration == C_Q3:
        return Q3SumProcessor()
    raise ValueError(f"Invalid sum processor configuration: {configuration}")
