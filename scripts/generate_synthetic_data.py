#!/usr/bin/env python
"""Thin wrapper around ``txn-pipeline generate-demo-data`` for people who prefer a script.

    uv run python scripts/generate_synthetic_data.py --records 10000 --seed 42 \
        --output data/incoming/transactions.csv --format csv --invalid-rate 0.02 --duplicate-rate 0.01
"""

from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path

from txn_pipeline.contracts.enums import SourceFormat
from txn_pipeline.synthetic.generator import GeneratorConfig, generate, write_rows


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--records", type=int, default=10_000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--output", type=Path, default=Path("data/incoming/synthetic_transactions.csv")
    )
    parser.add_argument(
        "--format",
        choices=[f.value for f in SourceFormat],
        default=None,
        help="defaults to the output file extension",
    )
    parser.add_argument("--invalid-rate", type=float, default=0.02)
    parser.add_argument("--duplicate-rate", type=float, default=0.01)
    parser.add_argument("--conflict-rate", type=float, default=0.005)
    parser.add_argument("--warning-rate", type=float, default=0.01)
    parser.add_argument("--anchor-date", type=date.fromisoformat, default=date(2026, 10, 13))
    args = parser.parse_args()

    fmt = SourceFormat(args.format or args.output.suffix.lstrip(".").lower())
    cfg = GeneratorConfig(
        records=args.records,
        seed=args.seed,
        invalid_rate=args.invalid_rate,
        duplicate_rate=args.duplicate_rate,
        conflict_rate=args.conflict_rate,
        warning_rate=args.warning_rate,
        anchor_date=args.anchor_date,
    )
    rows, summary = generate(cfg)
    write_rows(rows, args.output, fmt)
    print(f"wrote {len(rows):,} rows to {args.output}")
    print(json.dumps(summary.as_dict(), indent=2))


if __name__ == "__main__":
    main()
