from dataclasses import dataclass


@dataclass(frozen=True)
class Q2BankMaxPartial:
    bank_id: str
    from_account: str
    amount: float
    row_number: int = 0


@dataclass(frozen=True)
class Q3PaymentFormatPartial:
    payment_format: str
    amount_sum: float
    count: int


@dataclass(frozen=True)
class Q3AverageResult:
    payment_format: str
    average: float
