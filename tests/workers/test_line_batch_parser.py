import pytest

from common.message_protocol.external.types import (
    FILE_TYPE_ACCOUNTS,
    FILE_TYPE_TRANSACTIONS,
)
from common.message_protocol.internal import LineBatch
from workers.file_ingestor.line_batch_parser import LineBatchParser


HEADER = (
    "Timestamp",
    "From Bank",
    "Account",
    "To Bank",
    "Account",
    "Amount Paid",
    "Payment Currency",
    "Payment Format",
)


def _batch(lines, header=HEADER, file_type=FILE_TYPE_TRANSACTIONS):
    return LineBatch(
        file_type=file_type,
        rel_path="LI-Small_Trans.csv",
        batch_id=4,
        first_line_number=2,
        header=tuple(header),
        lines=tuple(lines),
    )


def test_line_batch_parser_uses_batch_header_order():
    header = (
        "Payment Format",
        "Timestamp",
        "From Bank",
        "Account",
        "Amount Paid",
        "To Bank",
        "Account",
        "Payment Currency",
    )
    batch = _batch(
        [b"Wire,2022/09/01 00:08,1,from-acc,12.5,2,to-acc,US Dollar"],
        header=header,
    )

    transactions = LineBatchParser.parse(batch)

    assert len(transactions) == 1
    assert transactions[0].date == "2022/09/01 00:08"
    assert transactions[0].from_bank == "1"
    assert transactions[0].from_account == "from-acc"
    assert transactions[0].to_bank == "2"
    assert transactions[0].to_account == "to-acc"
    assert transactions[0].amount == 12.5
    assert transactions[0].currency == "US Dollar"
    assert transactions[0].format == "Wire"


def test_line_batch_parser_strips_trailing_cr():
    batch = _batch(
        [b"2022/09/01 00:08,1,from-acc,2,to-acc,12.5,US Dollar,Wire\r"]
    )

    transactions = LineBatchParser.parse(batch)

    assert transactions[0].format == "Wire"


def test_line_batch_parser_accounts_batch_emits_no_transactions():
    batch = _batch(
        [b"anything", b""],
        header=("Bank", "Account"),
        file_type=FILE_TYPE_ACCOUNTS,
    )

    assert LineBatchParser.parse(batch) == []


def test_line_batch_parser_blank_line_raises():
    batch = _batch([b""])

    with pytest.raises(ValueError, match="invalid transaction line batch"):
        LineBatchParser.parse(batch)


def test_line_batch_parser_malformed_line_raises():
    batch = _batch([b"not,enough,fields"])

    with pytest.raises(ValueError, match="invalid transaction line batch"):
        LineBatchParser.parse(batch)


def test_line_batch_parser_emits_one_transaction_per_line():
    batch = _batch(
        [
            b"2022/09/01 00:08,1,from-1,2,to-1,12.5,US Dollar,Wire",
            b"2022/09/01 00:09,3,from-2,4,to-2,22.0,US Dollar,ACH",
        ]
    )

    transactions = LineBatchParser.parse(batch)

    assert len(transactions) == len(batch.lines)
    assert transactions[0].from_account == "from-1"
    assert transactions[1].from_account == "from-2"


def test_line_batch_parser_resolves_duplicate_account_columns():
    batch = _batch(
        [b"2022/09/01 00:08,1,from-acc,2,to-acc,12.5,US Dollar,Wire"]
    )

    transaction = LineBatchParser.parse(batch)[0]

    assert transaction.from_account == "from-acc"
    assert transaction.to_account == "to-acc"
