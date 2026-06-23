#!/usr/bin/env python3
"""Crash-recovery smoke test for q2_bank_name_joiner on LI-Small.

This is the dedicated entrypoint for the Q2 bank-name joiner worker. It reuses
the Q2 joiner pipeline crash-test implementation and pins the target to:

  scenario = bank
  dataset  = LI-Small

Usage
-----
  venv/bin/python scripts/crash_test_q2_bank_name_joiner.py
  venv/bin/python scripts/crash_test_q2_bank_name_joiner.py --keep
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    args = parse_args()
    cmd = [
        sys.executable,
        str(ROOT / "scripts" / "crash_test_joiner.py"),
        "--scenario",
        "bank",
        "--dataset",
        "LI-Small",
    ]
    if args.keep:
        cmd.append("--keep")
    return subprocess.run(cmd).returncode


def parse_args():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--keep",
        action="store_true",
        default=False,
        help="Leave the stack up after the test (for log inspection)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    raise SystemExit(main())
