import csv
import sys
from pathlib import Path


SCRIPTS_ROOT = Path(__file__).resolve().parents[2] / "scripts"
if str(SCRIPTS_ROOT) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_ROOT))

import reference_results as ref


TRANS_HEADER = [
    "Timestamp",
    "From Bank",
    "Account",
    "To Bank",
    "Account",
    "Amount Paid",
    "Payment Currency",
    "Payment Format",
]


def _write_csv(path, header, rows):
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)


def test_compute_q2_uses_notebook_raw_inner_join(tmp_path):
    trans_file = tmp_path / "sample_Trans.csv"
    accounts_file = tmp_path / "sample_accounts.csv"
    _write_csv(
        trans_file,
        TRANS_HEADER,
        [
            ["2022/09/01 00:00", "001", "leading-low", "9", "dst", "100", "US Dollar", "Wire"],
            ["2022/09/02 00:00", "001", "leading-high", "9", "dst", "200", "US Dollar", "Wire"],
            ["2022/09/01 00:00", "1", "raw-match", "9", "dst", "50", "US Dollar", "Wire"],
            ["2022/09/01 00:00", "02", "unmatched", "9", "dst", "300", "US Dollar", "Wire"],
            ["2022/09/01 00:00", "3", "non-usd", "9", "dst", "400", "Euro", "Wire"],
        ],
    )
    _write_csv(
        accounts_file,
        ["Bank ID", "Bank Name"],
        [
            ["1", "Raw One"],
            ["3", "Skipped Non USD"],
        ],
    )

    assert ref.compute_q2(trans_file, accounts_file) == [
        ("1", "raw-match", "Raw One", "50.00")
    ]


def test_q2_normalization_compares_account_column():
    assert ref.normalize_row("q2", ["1", "raw-match", "Raw One", "50"]) == (
        "1",
        "raw-match",
        "Raw One",
        "50.00",
    )


def test_compute_q3_uses_notebook_timestamp_bounds_and_format_column(tmp_path):
    trans_file = tmp_path / "sample_Trans.csv"
    _write_csv(
        trans_file,
        TRANS_HEADER,
        [
            ["2022/09/01 00:00", "1", "baseline-wire", "9", "dst", "100", "US Dollar", "Wire"],
            ["2022/09/06 00:00", "2", "candidate-a", "9", "dst", "0.50", "US Dollar", "Wire"],
            ["2022/09/14 23:59", "3", "candidate-b", "9", "dst", "0.70", "US Dollar", "Wire"],
            ["2022/09/15 00:00", "4", "excluded-15th", "9", "dst", "0.10", "US Dollar", "Wire"],
            ["2022/09/14 23:59", "5", "too-large", "9", "dst", "1.00", "US Dollar", "Wire"],
            ["2022/09/14 23:59", "6", "non-usd", "9", "dst", "0.10", "Euro", "Wire"],
        ],
    )

    assert ref.compute_q3(trans_file) == [
        ("2", "candidate-a", "Wire", "0.50"),
        ("3", "candidate-b", "Wire", "0.70"),
    ]


def test_q3_normalization_compares_payment_format_column():
    assert ref.OUTPUT_COLUMNS["q3"] == [
        "From Bank",
        "Account",
        "Payment Format",
        "Amount Paid",
    ]
    assert ref.normalize_row("q3", ["2", "candidate-a", "Wire", "0.5"]) == (
        "2",
        "candidate-a",
        "Wire",
        "0.50",
    )
