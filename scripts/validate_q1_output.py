#!/usr/bin/env python3
"""
Valida que el flujo Q1 funcionó correctamente.
Verifica que solo las transacciones que pasan ambos filtros (USD + Q1) llegaron.
"""
import sys
import csv
import os
from collections import Counter
from pathlib import Path

Q1_MAX_AMOUNT = 50
USD_CURRENCY = "US Dollar"
DATASET_DIR = os.environ.get("Q1_DATASET_DIR", "data/datasets/client-1/LI-Mini")
DATASET_TRANS = os.environ.get("Q1_DATASET_TRANS", "LI-Mini_Trans.csv")
Q1_OUTPUT_COLUMNS = ["From Bank", "Account", "To Bank", "Account.1", "Amount Paid"]

def validate_q1_results():
    """
    Q1 Filters:
    - Filtro USD: currency == "US Dollar"
    - Filtro Q1: amount < 50
    
    Esperamos que TODAS las transacciones en LI-Mini_Trans.csv 
    cumplan AMBOS filtros y aparezcan en el output.
    """
    # 1. Leer dataset original
    dataset_file = Path(DATASET_DIR) / DATASET_TRANS
    output_dir = Path("data/output")
    
    if not dataset_file.exists():
        print(f"ERROR: Dataset not found: {dataset_file}")
        return False
    
    # 2. Verificar que output dir existe
    if not output_dir.exists():
        print("ERROR: No output directory found. Test may have failed.")
        return False
    
    # 3. Buscar archivos de salida de Q1
    output_files = list(output_dir.glob("results_q1_*.csv"))
    if not output_files:
        print("ERROR: No Q1 output files found in data/output")
        return False
    
    print(f"✓ Found {len(output_files)} output file(s)")
    
    # 4. Leer transacciones del dataset original
    original_transactions = []
    original_header = []
    try:
        with open(dataset_file, 'r') as f:
            reader = csv.reader(f)
            original_header = next(reader)
            for row in reader:
                original_transactions.append(row)
    except Exception as e:
        print(f"ERROR reading dataset: {e}")
        return False
    
    print(f"✓ Dataset has {len(original_transactions)} transactions")
    
    original_columns = _transaction_columns(original_header)
    expected_rows = Counter()
    for tx in original_transactions:
        currency = tx[original_columns["currency"]].strip()
        amount = float(tx[original_columns["amount"]])
        
        # Q1 requires: currency == "US Dollar" AND amount < 50
        if currency == USD_CURRENCY and amount < Q1_MAX_AMOUNT:
            expected_rows[_dataset_q1_output_key(tx, original_columns)] += 1

    expected_count = sum(expected_rows.values())
            
    print(f"✓ Found {expected_count} expected transactions in dataset")
    
    # 6. Multiclient: cada client_X procesa el mismo dataset y debe emitir
    # exactamente expected_count transactions. No se suman entre clientes.
    mismatches = 0
    for output_file in sorted(output_files):
        print(f"\n  Reading: {output_file.name}")
        try:
            with open(output_file, 'r') as f:
                reader = csv.DictReader(f)
                if reader.fieldnames != Q1_OUTPUT_COLUMNS:
                    print(
                        f"    ERROR: invalid header {reader.fieldnames}, "
                        f"expected {Q1_OUTPUT_COLUMNS}"
                    )
                    mismatches += 1
                    continue
                rows = list(reader)
        except Exception as e:
            print(f"ERROR reading {output_file}: {e}")
            return False

        print(f"    Lines in output: {len(rows)}")

        if len(rows) != expected_count:
            print(
                f"    ERROR: {output_file.name} has {len(rows)} transactions, "
                f"expected {expected_count}"
            )
            mismatches += 1
            continue

        output_rows = Counter()
        invalid = False
        for i, row in enumerate(rows):
            try:
                output_rows[_output_q1_key(row)] += 1
            except ValueError as e:
                print(f"    ERROR: row {i} invalid amount: {e}")
                invalid = True

        if invalid:
            mismatches += 1
        elif output_rows != expected_rows:
            missing = expected_rows - output_rows
            unexpected = output_rows - expected_rows
            print(
                f"    ERROR: output rows differ from expected "
                f"(missing={sum(missing.values())}, "
                f"unexpected={sum(unexpected.values())})"
            )
            mismatches += 1
        else:
            print(
                f"    ✓ matches expected ({expected_count}) and schema "
                f"{','.join(Q1_OUTPUT_COLUMNS)}"
            )

    if mismatches > 0:
        print(
            f"\nERROR: {mismatches} of {len(output_files)} client outputs do not match"
        )
        return False

    print(
        f"\nAll {len(output_files)} client outputs match expected count "
        f"({expected_count})"
    )
    return True


def _transaction_columns(header):
    from_bank_idx = header.index("From Bank")
    to_bank_idx = header.index("To Bank")
    return {
        "from_bank": from_bank_idx,
        "from_account": _column_index_after(header, "Account", from_bank_idx),
        "to_bank": to_bank_idx,
        "to_account": _column_index_after(header, "Account", to_bank_idx),
        "amount": header.index("Amount Paid"),
        "currency": header.index("Payment Currency"),
    }


def _column_index_after(header, name, start_index):
    for index in range(start_index + 1, len(header)):
        if header[index] == name:
            return index
    raise ValueError(f"missing required column {name!r} after index {start_index}")


def _dataset_q1_output_key(row, columns):
    amount = float(row[columns["amount"]])
    return (
        row[columns["from_bank"]].strip(),
        row[columns["from_account"]].strip(),
        row[columns["to_bank"]].strip(),
        row[columns["to_account"]].strip(),
        f"{amount:.2f}",
    )


def _output_q1_key(row):
    amount = float(row["Amount Paid"])
    return (
        row["From Bank"].strip(),
        row["Account"].strip(),
        row["To Bank"].strip(),
        row["Account.1"].strip(),
        f"{amount:.2f}",
    )

if __name__ == "__main__":
    print("=" * 60)
    print("Q1 FLOW VALIDATION")
    print("=" * 60)
    success = validate_q1_results()
    print("=" * 60)
    if success:
        print("✓✓✓ Q1 TEST PASSED ✓✓✓")
        sys.exit(0)
    else:
        print("✗✗✗ Q1 TEST FAILED ✗✗✗")
        sys.exit(1)
