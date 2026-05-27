#!/usr/bin/env python3
"""Validate Q5 output against the precomputed reference (or the dataset if absent)."""
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
