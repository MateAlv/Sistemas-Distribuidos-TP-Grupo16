#!/usr/bin/env python3
"""Validate Q4 output against the precomputed reference (or the dataset if absent)."""
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
        print(ref.green("✓✓✓ Q4 TEST PASSED ✓✓✓"))
        sys.exit(0)
    print(ref.red("✗✗✗ Q4 TEST FAILED ✗✗✗"))
    sys.exit(1)
