#!/usr/bin/env python3
"""Validate Q5 output.

Q5: count transactions in [2022-09-01, 2022-09-05] with format Wire or ACH whose
amount converted to USD is less than 1. Each client emits its own count, which
must equal the reference count (counts are NOT summed across clients).

Compares every ``results_q5_*.csv`` against the precomputed reference
(``<dataset>/expected_results/q5.csv``), falling back to computing it from the
dataset when the reference is absent.
"""
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import reference_results as ref

DATASET_DIR = os.environ.get("Q5_DATASET_DIR", "data/datasets/LI-Small")
DATASET_TRANS = os.environ.get("Q5_DATASET_TRANS", "LI-Small_Trans.csv")


if __name__ == "__main__":
    print("=" * 60)
    print("Q5 FLOW VALIDATION")
    print("=" * 60)
    success = ref.validate_query("q5", DATASET_DIR, DATASET_TRANS)
    print("=" * 60)
    if success:
        print("✓✓✓ Q5 TEST PASSED ✓✓✓")
        sys.exit(0)
    print("✗✗✗ Q5 TEST FAILED ✗✗✗")
    sys.exit(1)
