#!/usr/bin/env python
"""Local throughput benchmark: generate N synthetic rows, run the full pipeline, report.

    uv run python scripts/benchmark.py --sizes 10000 100000 --repeats 1

Uses a temporary data root and a SQLite metadata database by default (pass
--database-url for PostgreSQL). Results are printed as a Markdown table and written
to benchmarks/results.json. Numbers are for a single process on the local machine
and make no claim about production throughput.
"""

from __future__ import annotations

import argparse
import json
import platform
import resource
import shutil
import statistics
import sys
import tempfile
import time
from pathlib import Path

from txn_pipeline import PIPELINE_VERSION
from txn_pipeline.config import Settings
from txn_pipeline.contracts.enums import Actor, SourceFormat
from txn_pipeline.metadata.database import run_migrations
from txn_pipeline.observability.logging import configure_logging
from txn_pipeline.pipeline.orchestrator import PipelineOrchestrator
from txn_pipeline.synthetic.generator import GeneratorConfig, generate, write_rows


def peak_rss_mb() -> float:
    usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    # macOS reports bytes, Linux kilobytes.
    return usage / (1024 * 1024) if sys.platform == "darwin" else usage / 1024


def run_once(
    size: int, fmt: SourceFormat, database_url: str | None, warehouse: bool, seed: int
) -> dict[str, object]:
    root = Path(tempfile.mkdtemp(prefix="txn-bench-"))
    try:
        url = database_url or f"sqlite:///{root}/meta.db"
        settings = Settings(
            metadata_database_url=url,
            data_root=root / "data",
            log_level="ERROR",
            log_format="json",
            actor=Actor.SYSTEM,
        )
        run_migrations(url)
        rows, _ = generate(GeneratorConfig(records=size, seed=seed))
        source = root / f"bench_{size}_{seed}.{fmt.value}"
        write_rows(rows, source, fmt)
        rss_before = peak_rss_mb()
        start = time.perf_counter()
        result = PipelineOrchestrator(settings).run(
            source, source_name="BENCHMARK", build_warehouse=warehouse
        )
        elapsed = time.perf_counter() - start
        return {
            "size": size,
            "rows": result.counts.source_records,
            "format": fmt.value,
            "status": result.status.value,
            "elapsed_s": round(elapsed, 2),
            "records_per_s": round(result.counts.source_records / elapsed, 0),
            "stage_ms": result.metrics.get("stage_durations_ms", {}),
            "peak_rss_mb_after": round(peak_rss_mb(), 0),
            "peak_rss_mb_before": round(rss_before, 0),
            "warehouse": warehouse,
            "metadata": "sqlite" if url.startswith("sqlite") else "postgresql",
        }
    finally:
        shutil.rmtree(root, ignore_errors=True)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--sizes", type=int, nargs="+", default=[10_000, 100_000])
    parser.add_argument("--repeats", type=int, default=1)
    parser.add_argument("--format", choices=[f.value for f in SourceFormat], default="csv")
    parser.add_argument(
        "--database-url", default=None, help="PostgreSQL URL; default is a temporary SQLite file"
    )
    parser.add_argument("--no-warehouse", action="store_true")
    parser.add_argument("--output", type=Path, default=Path("benchmarks/results.json"))
    args = parser.parse_args()
    configure_logging("ERROR", "json")

    runs: list[dict[str, object]] = []
    for size in args.sizes:
        for i in range(args.repeats):
            runs.append(
                run_once(
                    size,
                    SourceFormat(args.format),
                    args.database_url,
                    not args.no_warehouse,
                    seed=42 + i,
                )
            )

    environment = {
        "pipeline_version": PIPELINE_VERSION,
        "python": platform.python_version(),
        "platform": platform.platform(),
        "machine": platform.machine(),
        "cpu_count": __import__("os").cpu_count(),
        "metadata": runs[0]["metadata"] if runs else None,
        "warehouse_build_included": not args.no_warehouse,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps({"environment": environment, "runs": runs}, indent=2))

    print("\n| rows | format | metadata | warehouse | elapsed (s) | records/s | peak RSS (MB) |")
    print("|-----:|--------|----------|-----------|------------:|----------:|--------------:|")
    for size in args.sizes:
        group = [r for r in runs if r["size"] == size]
        elapsed = statistics.median(float(r["elapsed_s"]) for r in group)
        rps = statistics.median(float(r["records_per_s"]) for r in group)
        rss = max(float(r["peak_rss_mb_after"]) for r in group)
        print(
            f"| {int(group[0]['rows']):,} | {group[0]['format']} | {group[0]['metadata']} | "
            f"{'yes' if not args.no_warehouse else 'no'} | {elapsed:.2f} | {rps:,.0f} | {rss:,.0f} |"
        )
    print(f"\nenvironment: {json.dumps(environment)}")
    print(f"results written to {args.output}")


if __name__ == "__main__":
    main()
