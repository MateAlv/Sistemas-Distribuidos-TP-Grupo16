#!/usr/bin/env python3
"""Precompute the per-dataset reference results for Q1-Q5.

For a dataset ``<NAME>`` it reads ``<root>/<NAME>/<NAME>_Trans.csv`` (+
``_accounts.csv`` for Q2) and writes ``<root>/<NAME>/expected_results/qN.csv``.
The reference is the single source of truth the validators compare against, so
the expensive Q4 graph computation runs once per dataset instead of on every
``make test``.

Usage:
    python scripts/precompute_expected.py --dataset LI-Mini
    python scripts/precompute_expected.py --dataset HI-Medium --queries q4
"""
import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import reference_results as ref


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", required=True, help="Dataset name, e.g. LI-Mini.")
    parser.add_argument(
        "--dataset-root", default="data/datasets",
        help="Directory holding dataset folders (default: data/datasets).",
    )
    parser.add_argument(
        "--trans", default=None,
        help="Transactions file name (default: <DATASET>_Trans.csv).",
    )
    parser.add_argument(
        "--queries", default=",".join(ref.QUERIES),
        help="Comma-separated queries to generate (default: all).",
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Regenerate even if the expected file already exists.",
    )
    args = parser.parse_args()

    dataset_dir = Path(args.dataset_root) / args.dataset
    trans_name = args.trans or f"{args.dataset}_Trans.csv"
    trans_file, _ = ref.dataset_paths(dataset_dir, trans_name)
    if not trans_file.exists():
        print(f"ERROR: transactions file not found: {trans_file}", file=sys.stderr)
        return 2

    queries = [q.strip() for q in args.queries.split(",") if q.strip()]
    unknown = [q for q in queries if q not in ref.QUERIES]
    if unknown:
        print(f"ERROR: unknown queries: {unknown}", file=sys.stderr)
        return 2

    print(f"Precomputing expected results for {args.dataset} ({trans_file})")
    for query in queries:
        out_path = ref.expected_path(dataset_dir, query)
        if out_path.exists() and not args.force:
            print(f"  {query}: up-to-date ({out_path}) — use --force to regenerate")
            continue
        start = time.monotonic()
        rows = ref.compute(query, dataset_dir, trans_name)
        ref.write_expected(query, rows, out_path, trans_name)
        elapsed = time.monotonic() - start
        count = "count=" + rows[0][0] if query == "q5" else f"{len(rows)} rows"
        print(f"  {query}: {count} ({elapsed:.1f}s) -> {out_path}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
