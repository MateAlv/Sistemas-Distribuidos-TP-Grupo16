#!/usr/bin/env python3
"""
Valida que el flujo Q2 funcionó correctamente.
Verifica que el max amount por banco coincide con el dataset original y
que el nombre del banco corresponde al dataset de cuentas.
"""
import sys
import csv
import os
from collections import Counter
from pathlib import Path

USD_CURRENCY = "US Dollar"
DATASET_DIR = os.environ.get("Q2_DATASET_DIR", "data/datasets/client-1/LI-Mini")
DATASET_TRANS = os.environ.get("Q2_DATASET_TRANS", "LI-Mini_Trans.csv")
DATASET_ACCOUNTS = os.environ.get("Q2_DATASET_ACCOUNTS")
Q2_OUTPUT_COLUMNS = ["From Bank", "Account", "Bank Name", "Amount Paid"]


def _normalize_bank_id(value: str) -> str:
    value = (value or "").strip()
    if not value:
        return ""
    if value.isdigit():
        return str(int(value))
    return value.lstrip("0") or "0"


def _accounts_file_for(dataset_dir: Path, dataset_trans: str) -> Path:
    if DATASET_ACCOUNTS:
        return dataset_dir / DATASET_ACCOUNTS
    if dataset_trans.endswith("_Trans.csv"):
        return dataset_dir / dataset_trans.replace("_Trans.csv", "_accounts.csv")
    return dataset_dir / "accounts.csv"


def validate_q2_results():
    """
    Q2: Para cada banco origen, la transacción con el mayor monto en USD.
    - Filtro USD: currency == "US Dollar"
    - Reducción: max(amount) agrupado por bank_id

    Esperamos que el output contenga exactamente un registro por banco,
    con el monto máximo de todas sus transacciones USD en el dataset.
    """
    # 1. Leer dataset original
    dataset_file = Path(DATASET_DIR) / DATASET_TRANS
    accounts_file = _accounts_file_for(Path(DATASET_DIR), DATASET_TRANS)
    output_dir = Path("data/output")

    if not dataset_file.exists():
        print(f"ERROR: Dataset not found: {dataset_file}")
        return False
    if not accounts_file.exists():
        print(f"ERROR: Accounts dataset not found: {accounts_file}")
        return False

    if not output_dir.exists():
        print("ERROR: No output directory found. Test may have failed.")
        return False

    output_files = sorted(output_dir.glob("results_q2_*.csv"))
    if not output_files:
        print("ERROR: No Q2 output files found in data/output")
        return False

    # 2. Leer cuentas para validar el enriquecimiento de Bank Name.
    bank_names: dict[str, str] = {}
    try:
        with open(accounts_file, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                bank_id = _normalize_bank_id(row["Bank ID"])
                if bank_id:
                    bank_names.setdefault(bank_id, row["Bank Name"].strip())
    except Exception as e:
        print(f"ERROR reading accounts dataset: {e}")
        return False

    print(f"✓ Dataset has {len(bank_names)} bank account mappings")

    # 3. Leer transacciones del dataset original
    max_by_bank: dict[str, tuple[str, float]] = {}
    total_usd = 0
    try:
        with open(dataset_file, "r") as f:
            reader = csv.reader(f)
            header = next(reader)
            columns = _transaction_columns(header)
            for row in reader:
                currency = row[columns["currency"]].strip()
                if currency != USD_CURRENCY:
                    continue
                total_usd += 1
                bank_id = row[columns["from_bank"]].strip()
                amount = float(row[columns["amount"]])
                if bank_id not in max_by_bank or amount > max_by_bank[bank_id][1]:
                    max_by_bank[bank_id] = (
                        row[columns["from_account"]].strip(),
                        amount,
                    )
    except Exception as e:
        print(f"ERROR reading dataset: {e}")
        return False

    print(f"✓ Dataset has {total_usd} USD transactions across {len(max_by_bank)} banks")
    expected_rows = Counter()
    for bank_id, (account, amount) in max_by_bank.items():
        bank_name = bank_names.get(_normalize_bank_id(bank_id), "")
        expected_rows[(bank_id, account, bank_name, f"{amount:.2f}")] += 1

    print(f"Found {len(output_files)} output file(s)")

    for output_file in output_files:
        output_rows: list[dict] = []
        try:
            with open(output_file, "r") as f:
                reader = csv.DictReader(f)
                if reader.fieldnames != Q2_OUTPUT_COLUMNS:
                    print(
                        f"ERROR: invalid header {reader.fieldnames}, "
                        f"expected {Q2_OUTPUT_COLUMNS}"
                    )
                    return False
                output_rows = list(reader)
        except Exception as e:
            print(f"ERROR reading {output_file}: {e}")
            return False

        print(f"\n  Reading: {output_file.name}")
        print(f"    Lines in output: {len(output_rows)}")

        if len(output_rows) == 0 and max_by_bank:
            print("\nERROR: Output file is empty")
            return False

        # 4. Verificar que hay un registro por banco (sin duplicados)
        seen_banks: set[str] = set()
        for row in output_rows:
            bank_id = row.get("From Bank", "").strip()
            if bank_id in seen_banks:
                print(f"ERROR: Duplicate bank_id in output: '{bank_id}'")
                return False
            seen_banks.add(bank_id)

        print("    ✓ No duplicate banks in output")

        # 5. Verificar que cada banco tiene el monto máximo correcto
        errors = 0
        missing_bank_names = 0
        actual_rows = Counter()
        for row in output_rows:
            bank_id = row.get("From Bank", "").strip()
            try:
                reported_max = float(row.get("Amount Paid", ""))
            except ValueError:
                print(f"ERROR: Amount Paid is not numeric for bank_id='{bank_id}': {row.get('Amount Paid')}")
                return False

            expected = max_by_bank.get(bank_id)
            if expected is None:
                print(f"ERROR: bank_id '{bank_id}' not found in dataset")
                errors += 1
                continue

            expected_account, expected_max = expected
            if abs(reported_max - expected_max) > 1e-6:
                print(
                    f"ERROR: bank_id='{bank_id}' max_amount mismatch: "
                    f"got={reported_max}, expected={expected_max}"
                )
                errors += 1

            reported_account = row.get("Account", "").strip()
            if reported_account != expected_account:
                print(
                    f"ERROR: bank_id='{bank_id}' account mismatch: "
                    f"got={reported_account!r}, expected={expected_account!r}"
                )
                errors += 1

            expected_bank_name = bank_names.get(_normalize_bank_id(bank_id))
            reported_bank_name = row.get("Bank Name", "").strip()
            if expected_bank_name is None:
                missing_bank_names += 1
                if reported_bank_name:
                    print(
                        f"ERROR: bank_id='{bank_id}' has no Bank Name in accounts "
                        f"dataset but output reported {reported_bank_name!r}"
                    )
                    errors += 1
            elif reported_bank_name != expected_bank_name:
                print(
                    f"ERROR: bank_id='{bank_id}' bank_name mismatch: "
                    f"got={reported_bank_name!r}, expected={expected_bank_name!r}"
                )
                errors += 1
            actual_rows[
                (
                    bank_id,
                    reported_account,
                    reported_bank_name,
                    f"{reported_max:.2f}",
                )
            ] += 1

        if errors > 0:
            print(f"\nERROR: {errors} rows with incorrect Q2 values")
            return False

        if actual_rows != expected_rows:
            missing = expected_rows - actual_rows
            unexpected = actual_rows - expected_rows
            print(
                f"ERROR: output rows differ from expected "
                f"(missing={sum(missing.values())}, "
                f"unexpected={sum(unexpected.values())})"
            )
            return False

        print(
            "    ✓ All rows match notebook schema and expected "
            "max amounts/bank names"
        )
        if missing_bank_names:
            print(
                f"    ✓ {missing_bank_names} banks have no accounts mapping "
                "and were emitted with an empty bank_name"
            )

        # 6. Verificar count: debe haber un registro por cada banco distinto
        if len(output_rows) != len(max_by_bank):
            print(
                f"ERROR: Output has {len(output_rows)} rows but dataset has "
                f"{len(max_by_bank)} distinct banks"
            )
            return False

        print(f"    ✓ Output row count matches expected bank count ({len(max_by_bank)})")

    return True


def _transaction_columns(header):
    from_bank_idx = header.index("From Bank")
    return {
        "from_bank": from_bank_idx,
        "from_account": _column_index_after(header, "Account", from_bank_idx),
        "amount": header.index("Amount Paid"),
        "currency": header.index("Payment Currency"),
    }


def _column_index_after(header, name, start_index):
    for index in range(start_index + 1, len(header)):
        if header[index] == name:
            return index
    raise ValueError(f"missing required column {name!r} after index {start_index}")


if __name__ == "__main__":
    print("=" * 60)
    print("Q2 FLOW VALIDATION")
    print("=" * 60)
    success = validate_q2_results()
    print("=" * 60)
    if success:
        print("✓✓✓ Q2 TEST PASSED ✓✓✓")
        sys.exit(0)
    else:
        print("✗✗✗ Q2 TEST FAILED ✗✗✗")
        sys.exit(1)
