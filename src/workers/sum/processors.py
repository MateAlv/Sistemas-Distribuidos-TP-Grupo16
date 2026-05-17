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

    def process(self, transaction) -> None:
        bank_id = transaction.from_bank
        current = self.max_by_bank.get(bank_id)
        candidate = {
            "bank_id": bank_id,
            "from_account": transaction.from_account,
            "amount": transaction.amount,
        }

        if current is None or candidate["amount"] > current["amount"]:
            self.max_by_bank[bank_id] = candidate


class Q3SumProcessor(SumProcessor):
    def __init__(self) -> None:
        self.amount_sum_by_payment_format = {}
        self.count_by_payment_format = {}

    def process(self, transaction) -> None:
        payment_format = transaction.format
        self.amount_sum_by_payment_format[payment_format] = (
            self.amount_sum_by_payment_format.get(payment_format, 0.0)
            + transaction.amount
        )
        self.count_by_payment_format[payment_format] = (
            self.count_by_payment_format.get(payment_format, 0) + 1
        )


def create_sum_processor(configuration: str) -> SumProcessor:
    if configuration == C_Q2:
        return Q2SumProcessor()
    if configuration == C_Q3:
        return Q3SumProcessor()
    raise ValueError(f"Invalid sum processor configuration: {configuration}")
