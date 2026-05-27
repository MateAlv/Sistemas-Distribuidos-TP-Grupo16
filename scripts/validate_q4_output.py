#!/usr/bin/env python3
"""Validate Q4 output.

Q4: over USD transactions in [2022-09-01, 2022-09-05], treat from_account ->
to_account as a directed graph and emit a pair (A, B) once there are >= 5
distinct intermediaries M with both A -> M and M -> B. This mirrors the
scatter-gather linker/detector semantics (src/workers/scatter_gather/).

Compares every ``results_q4_*.csv`` against the precomputed reference
(``<dataset>/expected_results/q4.csv``), falling back to computing it from the
dataset when the reference is absent.
"""
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import reference_results as ref

DATASET_DIR = os.environ.get("Q4_DATASET_DIR", "data/datasets/LI-Mini")
DATASET_TRANS = os.environ.get("Q4_DATASET_TRANS", "LI-Mini_Trans.csv")


if __name__ == "__main__":
    print("=" * 60)
    print("Q4 FLOW VALIDATION")
    print("=" * 60)
    success = ref.validate_query("q4", DATASET_DIR, DATASET_TRANS)
    print("=" * 60)
    if success:
        print("✓✓✓ Q4 TEST PASSED ✓✓✓")
        sys.exit(0)
    print("✗✗✗ Q4 TEST FAILED ✗✗✗")
    sys.exit(1)
