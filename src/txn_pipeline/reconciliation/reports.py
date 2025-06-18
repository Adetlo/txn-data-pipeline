"""Persist and render reconciliation reports."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from txn_pipeline.reconciliation.controls import ReconciliationReport


def write_report(report: ReconciliationReport, reports_root: Path) -> Path:
    directory = reports_root / f"batch_id={report.batch_id}"
    directory.mkdir(parents=True, exist_ok=True)
    target = directory / "reconciliation.json"
    tmp = target.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(report.as_dict(), indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(target)
    return target


def load_report(path: Path) -> dict[str, Any]:
    data: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    return data


def render_text(report: dict[str, Any]) -> str:
    """Compact plain-text rendering used by the CLI."""
    lines = [
        f"Batch:            {report['batch_id']}",
        f"Source records:   {report['source_records']:,}",
        f"Accepted unique:  {report['accepted_records']:,}",
        f"Rejected:         {report['rejected_records']:,} "
        f"(conflicts {report['conflict_records']:,})",
        f"Duplicates:       {report['duplicate_records']:,}",
        f"Curated rows:     {report['curated_records']:,}",
        f"Reconciliation:   {'PASS' if report['balanced'] else 'FAIL'}",
    ]
    for code, totals in report.get("currency_totals", {}).items():
        lines.append(
            f"  {code}: source {totals['source']}  curated {totals['curated']}  "
            f"difference {totals['difference']}"
        )
    for note in report.get("notes", []):
        lines.append(f"  ! {note}")
    return "\n".join(lines)
