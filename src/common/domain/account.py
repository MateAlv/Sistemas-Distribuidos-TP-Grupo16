from dataclasses import dataclass


@dataclass(frozen=True)
class BankAccountMapping:
    bank_id: str
    bank_name: str


@dataclass(frozen=True)
class Q2BankMaxResult:
    bank_id: str
    from_account: str
    bank_name: str
    amount: float
