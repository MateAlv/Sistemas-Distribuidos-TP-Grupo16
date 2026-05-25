#!/usr/bin/env python3
"""
Valida que el flujo Q1 funcionó correctamente.
Verifica que solo las transacciones que pasan ambos filtros (USD + Q1) llegaron.
"""
import sys
import csv
import os
from pathlib import Path

Q1_MAX_AMOUNT = 50
USD_CURRENCY = "US Dollar"
DATASET_DIR = os.environ.get("Q1_DATASET_DIR", "data/datasets/client-1/LI-Mini")
DATASET_TRANS = os.environ.get("Q1_DATASET_TRANS", "LI-Mini_Trans.csv")

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
    try:
        with open(dataset_file, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                original_transactions.append(row)
    except Exception as e:
        print(f"ERROR reading dataset: {e}")
        return False
    
    print(f"✓ Dataset has {len(original_transactions)} transactions")
    
    expected_count = 0
    for i, tx in enumerate(original_transactions):
        currency = tx['Payment Currency'].strip()
        amount = float(tx['Amount Paid'])
        
        # Q1 requires: currency == "US Dollar" AND amount < 50
        if currency == USD_CURRENCY and amount < Q1_MAX_AMOUNT:
            expected_count += 1
            
    print(f"✓ Found {expected_count} expected transactions in dataset")
    
    # 6. Multiclient: cada client_X procesa el mismo dataset y debe emitir
    # exactamente expected_count transactions. No se suman entre clientes.
    mismatches = 0
    for output_file in sorted(output_files):
        print(f"\n  Reading: {output_file.name}")
        try:
            with open(output_file, 'r') as f:
                reader = csv.DictReader(f)
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

        # Validar que cada transaction del archivo respeta el filtro Q1.
        invalid = 0
        for i, tx in enumerate(rows):
            if 'currency' in tx and 'amount' in tx:
                currency = tx['currency'].strip()
                try:
                    amount = float(tx['amount'])
                except ValueError:
                    print(f"    ERROR: row {i} amount not numeric: {tx['amount']!r}")
                    invalid += 1
                    continue
                if currency != USD_CURRENCY:
                    print(f"    ERROR: row {i} invalid currency: {currency}")
                    invalid += 1
                elif amount >= Q1_MAX_AMOUNT:
                    print(f"    ERROR: row {i} invalid amount: {amount}")
                    invalid += 1

        if invalid > 0:
            mismatches += 1
        else:
            print(f"    ✓ matches expected ({expected_count}) and all rows pass Q1")

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
