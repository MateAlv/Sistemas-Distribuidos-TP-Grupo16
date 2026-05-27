#!/usr/bin/env python3
"""Validate Q2 output (per source bank, the max-amount USD transaction enriched
with the bank name).

Compares every ``results_q2_*.csv`` against the precomputed reference
(``<dataset>/expected_results/q2.csv``), falling back to computing it from the
dataset when the reference is absent.
"""
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import reference_results as ref

DATASET_DIR = os.environ.get("Q2_DATASET_DIR", "data/datasets/LI-Mini")
DATASET_TRANS = os.environ.get("Q2_DATASET_TRANS", "LI-Mini_Trans.csv")


if __name__ == "__main__":
    print("=" * 60)
    print("Q2 FLOW VALIDATION")
    print("=" * 60)
    success = ref.validate_query("q2", DATASET_DIR, DATASET_TRANS)
    print("=" * 60)
    if success:
        print("✓✓✓ Q2 TEST PASSED ✓✓✓")
        sys.exit(0)
    print("✗✗✗ Q2 TEST FAILED ✗✗✗")
    sys.exit(1)
