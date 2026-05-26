#!/usr/bin/env python3
"""
Validate Q3 output.

Q3: source account and amount for USD transactions in [2022-09-06, 2022-09-15]
whose amount is less than one hundredth of the average amount for the same
payment format in [2022-09-01, 2022-09-05].
"""
import csv
import os
import sys
from collections import Counter, defaultdict
from pathlib import Path


USD_CURRENCY = "US Dollar"
BASELINE_START = os.environ.get("Q3_BASELINE_START", "2022-09-01")
BASELINE_END = os.environ.get("Q3_BASELINE_END", "2022-09-05")
CANDIDATE_START = os.environ.get("Q3_CANDIDATE_START", "2022-09-06")
CANDIDATE_END = os.environ.get("Q3_CANDIDATE_END", "2022-09-15")
DATASET_DIR = os.environ.get("Q3_DATASET_DIR", "data/datasets/client-1/LI-Mini")
DATASET_TRANS = os.environ.get("Q3_DATASET_TRANS", "LI-Mini_Trans.csv")
Q3_OUTPUT_COLUMNS = ["From Bank", "Account", "Amount Paid"]


def validate_q3_results() -> bool:
    dataset_file = Path(DATASET_DIR) / DATASET_TRANS
    output_dir = Path("data/output")

    if not dataset_file.exists():
        print(f"ERROR: Dataset not found: {dataset_file}")
        return False
    if not output_dir.exists():
        print("ERROR: No output directory found. Test may have failed.")
        return False

    output_files = sorted(output_dir.glob("results_q3_*.csv"))
    if not output_files:
        print("ERROR: No Q3 output files found in data/output")
        return False

    try:
        expected_rows = _expected_rows(dataset_file)
    except Exception as e:
        print(f"ERROR computing expected Q3 rows: {e}")
        return False

    print(f"Found {len(output_files)} output file(s)")
    print(f"Expected Q3 rows: {sum(expected_rows.values())}")

    for output_file in output_files:
        print(f"\n  Reading: {output_file.name}")
        try:
            actual_rows = _read_output(output_file)
        except Exception as e:
            print(f"ERROR reading {output_file}: {e}")
            return False

        actual_count = sum(actual_rows.values())
        print(f"    count = {actual_count}")

        if actual_rows != expected_rows:
            missing = expected_rows - actual_rows
            unexpected = actual_rows - expected_rows
            print(
                f"ERROR: Q3 rows differ from expected "
                f"(missing={sum(missing.values())}, unexpected={sum(unexpected.values())})"
            )
            for row in list(missing)[:5]:
                print(f"  missing: {row}")
            for row in list(unexpected)[:5]:
                print(f"  unexpected: {row}")
            return False

        print("    ✓ matches expected Q3 rows")

    print(f"\nAll {len(output_files)} client outputs match expected Q3 rows")
    return True


def _expected_rows(dataset_file: Path) -> Counter:
    sums: dict[str, float] = defaultdict(float)
    counts: dict[str, int] = defaultdict(int)
    rows = []

    with open(dataset_file, "r") as f:
        reader = csv.reader(f)
        header = next(reader)
        col = _transaction_columns(header)
        for row in reader:
            if row[col["currency"]].strip() != USD_CURRENCY:
                continue
            date = _normalize_date(row[col["date"]])
            fmt = row[col["payment_format"]].strip()
            amount = float(row[col["amount"]])
            if BASELINE_START <= date <= BASELINE_END:
                sums[fmt] += amount
                counts[fmt] += 1
            elif CANDIDATE_START <= date <= CANDIDATE_END:
                rows.append(
                    (
                        fmt,
                        row[col["from_bank"]].strip(),
                        row[col["from_account"]].strip(),
                        amount,
                    )
                )

    averages = {fmt: sums[fmt] / counts[fmt] for fmt in counts}
    expected = Counter()
    for fmt, from_bank, from_account, amount in rows:
        avg = averages.get(fmt)
        if avg is not None and amount < (avg / 100):
            expected[(from_bank, from_account, f"{amount:.2f}")] += 1
    return expected


def _read_output(output_file: Path) -> Counter:
    with open(output_file, "r") as f:
        reader = csv.DictReader(f)
        if list(reader.fieldnames or []) != Q3_OUTPUT_COLUMNS:
            raise ValueError(
                f"invalid header {reader.fieldnames}, expected {Q3_OUTPUT_COLUMNS}"
            )
        return Counter(
            (
                row["From Bank"].strip(),
                row["Account"].strip(),
                f"{float(row['Amount Paid']):.2f}",
            )
            for row in reader
        )


def _transaction_columns(header: list[str]) -> dict[str, int]:
    from_bank_idx = header.index("From Bank")
    return {
        "from_bank": from_bank_idx,
        "from_account": _column_index_after(header, "Account", from_bank_idx),
        "date": header.index("Timestamp"),
        "amount": header.index("Amount Paid"),
        "currency": header.index("Payment Currency"),
        "payment_format": header.index("Payment Format"),
    }


def _column_index_after(header: list[str], name: str, start_index: int) -> int:
    for index in range(start_index + 1, len(header)):
        if header[index] == name:
            return index
    raise ValueError(f"missing required column {name!r} after index {start_index}")


def _normalize_date(value: str) -> str:
    return value[:10].replace("/", "-")


if __name__ == "__main__":
    print("=" * 60)
    print("Q3 FLOW VALIDATION")
    print("=" * 60)
    success = validate_q3_results()
    print("=" * 60)
    if success:
        print("✓ Q3 test PASSED")
        sys.exit(0)
    print("✗ Q3 test FAILED")
    sys.exit(1)
