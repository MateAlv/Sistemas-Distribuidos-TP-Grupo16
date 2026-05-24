#!/usr/bin/env python3
"""
Valida que el flujo Q2 funcionó correctamente.
Verifica que el max amount por banco coincide con el dataset original.
"""
import sys
import csv
import os
from pathlib import Path

USD_CURRENCY = "US Dollar"
DATASET_DIR = os.environ.get("Q2_DATASET_DIR", "data/datasets/client-1/LI-Mini")
DATASET_TRANS = os.environ.get("Q2_DATASET_TRANS", "LI-Mini_Trans.csv")


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
    output_dir = Path("data/output")

    if not dataset_file.exists():
        print(f"ERROR: Dataset not found: {dataset_file}")
        return False

    if not output_dir.exists():
        print("ERROR: No output directory found. Test may have failed.")
        return False

    output_files = sorted(output_dir.glob("results_q2_*.csv"))
    if not output_files:
        print("ERROR: No Q2 output files found in data/output")
        return False

    # 2. Leer transacciones del dataset original
    max_by_bank: dict[str, float] = {}
    total_usd = 0
    try:
        with open(dataset_file, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                currency = row["Payment Currency"].strip()
                if currency != USD_CURRENCY:
                    continue
                total_usd += 1
                bank_id = row["From Bank"].strip()
                amount = float(row["Amount Paid"])
                if bank_id not in max_by_bank or amount > max_by_bank[bank_id]:
                    max_by_bank[bank_id] = amount
    except Exception as e:
        print(f"ERROR reading dataset: {e}")
        return False

    print(f"✓ Dataset has {total_usd} USD transactions across {len(max_by_bank)} banks")

    print(f"Found {len(output_files)} output file(s)")

    for output_file in output_files:
        output_rows: list[dict] = []
        try:
            with open(output_file, "r") as f:
                reader = csv.DictReader(f)
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
            bank_id = row.get("bank_id", "").strip()
            if bank_id in seen_banks:
                print(f"ERROR: Duplicate bank_id in output: '{bank_id}'")
                return False
            seen_banks.add(bank_id)

        print("    ✓ No duplicate banks in output")

        # 5. Verificar que cada banco tiene el monto máximo correcto
        errors = 0
        for row in output_rows:
            bank_id = row.get("bank_id", "").strip()
            try:
                reported_max = float(row.get("max_amount", ""))
            except ValueError:
                print(f"ERROR: max_amount is not numeric for bank_id='{bank_id}': {row.get('max_amount')}")
                return False

            expected_max = max_by_bank.get(bank_id)
            if expected_max is None:
                print(f"ERROR: bank_id '{bank_id}' not found in dataset")
                errors += 1
                continue

            if abs(reported_max - expected_max) > 1e-6:
                print(
                    f"ERROR: bank_id='{bank_id}' max_amount mismatch: "
                    f"got={reported_max}, expected={expected_max}"
                )
                errors += 1

        if errors > 0:
            print(f"\nERROR: {errors} rows with incorrect max_amount")
            return False

        print("    ✓ All max amounts match the dataset")

        # 6. Verificar count: debe haber un registro por cada banco distinto
        if len(output_rows) != len(max_by_bank):
            print(
                f"ERROR: Output has {len(output_rows)} rows but dataset has "
                f"{len(max_by_bank)} distinct banks"
            )
            return False

        print(f"    ✓ Output row count matches expected bank count ({len(max_by_bank)})")

    return True


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
