from common.constants import C_Q2, C_Q3
from common.domain.partial_result import Q2BankMaxPartial, Q3PaymentFormatPartial
from common.message_protocol.internal.partial_result_serializer import (
    Q2BankMaxPartialSerializer,
    Q3PaymentFormatPartialSerializer,
)


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

    def partials(self):
        return [
            self._partial_tuple(candidate)
            for candidate in self.max_by_bank.values()
        ]

    def partials_for_transaction(self, transaction):
        return [
            self._partial_tuple(
                {
                    "bank_id": transaction.from_bank,
                    "from_account": transaction.from_account,
                    "amount": transaction.amount,
                }
            )
        ]

    def _partial_tuple(self, candidate):
        partial = Q2BankMaxPartial(
            bank_id=candidate["bank_id"],
            from_account=candidate["from_account"],
            amount=candidate["amount"],
        )
        return (
            partial.bank_id,
            Q2BankMaxPartialSerializer.serialize(partial),
        )


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

    def partials(self):
        return [
            self._partial_tuple(
                payment_format,
                amount_sum,
                self.count_by_payment_format[payment_format],
            )
            for payment_format, amount_sum in (
                self.amount_sum_by_payment_format.items()
            )
        ]

    def partials_for_transaction(self, transaction):
        return [
            self._partial_tuple(
                transaction.format,
                transaction.amount,
                1,
            )
        ]

    def _partial_tuple(self, payment_format, amount_sum, count):
        partial = Q3PaymentFormatPartial(
            payment_format=payment_format,
            amount_sum=amount_sum,
            count=count,
        )
        return (
            partial.payment_format,
            Q3PaymentFormatPartialSerializer.serialize(partial),
        )


def create_sum_processor(configuration: str) -> SumProcessor:
    if configuration == C_Q2:
        return Q2SumProcessor()
    if configuration == C_Q3:
        return Q3SumProcessor()
    raise ValueError(f"Invalid sum processor configuration: {configuration}")
