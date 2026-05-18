from dataclasses import dataclass


@dataclass(frozen=True)
class Q2BankMaxPartial:
    bank_id: str
    from_account: str
    amount: float


@dataclass(frozen=True)
class Q3PaymentFormatPartial:
    payment_format: str
    amount_sum: float
    count: int
