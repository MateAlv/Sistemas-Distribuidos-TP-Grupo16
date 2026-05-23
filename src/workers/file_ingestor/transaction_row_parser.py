import csv
from dataclasses import dataclass

from common.domain.transaction import Transaction


@dataclass(frozen=True)
class TransactionColumnIndexes:
    date: int
    from_bank: int
    from_account: int
    to_bank: int
    to_account: int
    amount: int
    currency: int
    format: int


class TransactionRowParser:
    def __init__(self, header: tuple[str, ...]) -> None:
        self.header = tuple(header)
        self.columns = _resolve_transaction_columns(self.header)

    def parse_line(self, line: bytes) -> Transaction:
        clean_line = line[:-1] if line.endswith(b"\r") else line
        if not clean_line:
            raise ValueError("empty transaction line")
        return self.parse_fields(parse_csv_line(clean_line))

    def parse_fields(self, fields: list[str]) -> Transaction:
        if len(fields) != len(self.header):
            raise ValueError(
                f"transaction row has {len(fields)} fields, expected {len(self.header)}"
            )

        columns = self.columns
        return Transaction(
            date=_required(fields, columns.date),
            from_bank=_required(fields, columns.from_bank),
            from_account=_required(fields, columns.from_account),
            to_bank=_required(fields, columns.to_bank),
            to_account=_required(fields, columns.to_account),
            amount=float(_required(fields, columns.amount)),
            currency=_required(fields, columns.currency),
            format=_required(fields, columns.format),
        )


def parse_csv_line(line: bytes) -> list[str]:
    rows = list(csv.reader([line.decode("utf-8")]))
    if len(rows) != 1:
        raise ValueError("expected exactly one CSV row")
    return rows[0]


def _resolve_transaction_columns(header: tuple[str, ...]) -> TransactionColumnIndexes:
    from_bank_idx = _column_index(header, "From Bank")
    to_bank_idx = _column_index(header, "To Bank")
    return TransactionColumnIndexes(
        date=_column_index(header, "Timestamp"),
        from_bank=from_bank_idx,
        from_account=_column_index_after(header, "Account", from_bank_idx),
        to_bank=to_bank_idx,
        to_account=_column_index_after(header, "Account", to_bank_idx),
        amount=_column_index(header, "Amount Paid"),
        currency=_column_index(header, "Payment Currency"),
        format=_column_index(header, "Payment Format"),
    )


def _column_index(header: tuple[str, ...], name: str) -> int:
    try:
        return header.index(name)
    except ValueError as exc:
        raise ValueError(f"missing required transaction column: {name}") from exc


def _column_index_after(header: tuple[str, ...], name: str, start_index: int) -> int:
    for index in range(start_index + 1, len(header)):
        if header[index] == name:
            return index
    raise ValueError(f"missing required transaction column after {start_index}: {name}")


def _required(fields: list[str], index: int) -> str:
    value = fields[index].strip()
    if not value:
        raise ValueError(f"empty required transaction field at index {index}")
    return value
