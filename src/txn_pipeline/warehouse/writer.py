"""Idempotent Parquet writers for curated and quarantine outputs.

Deterministic paths + write-to-temp-then-rename make every write stage safe to retry:

    <data_root>/curated/booking_year=YYYY/booking_month=MM/batch_id=<batch>.parquet
    <data_root>/quarantine/batch_id=<batch>/rejected.parquet

Before writing, any file previously produced *by the same batch* is removed, so a retry
after a partial write never leaves orphaned or duplicated partitions.
"""

from __future__ import annotations

import contextlib
import json
from collections.abc import Sequence
from datetime import datetime
from pathlib import Path

import polars as pl

from txn_pipeline.contracts.rules import RULES
from txn_pipeline.contracts.schemas import PARTITION_COLUMNS, QUARANTINE_POLARS_SCHEMA
from txn_pipeline.exceptions import OutputWriteError
from txn_pipeline.ingestion.hashing import canonical_json
from txn_pipeline.validation.results import Rejection


def partition_path(curated_root: Path, batch_id: str, year: int, month: int) -> Path:
    return (
        curated_root
        / f"booking_year={year:04d}"
        / f"booking_month={month:02d}"
        / f"batch_id={batch_id}.parquet"
    )


def existing_batch_partitions(curated_root: Path, batch_id: str) -> list[Path]:
    if not curated_root.exists():
        return []
    return sorted(curated_root.glob(f"booking_year=*/booking_month=*/batch_id={batch_id}.parquet"))


def _atomic_write(frame: pl.DataFrame, target: Path) -> None:
    tmp = target.with_name(target.name + ".tmp")
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        frame.write_parquet(tmp, compression="zstd", statistics=True)
        tmp.replace(target)
    except OSError as exc:
        with contextlib.suppress(OSError):
            tmp.unlink(missing_ok=True)
        raise OutputWriteError("CURATED_WRITE", f"cannot write {target}: {exc}") from exc


def write_curated(frame: pl.DataFrame, curated_root: Path, batch_id: str) -> list[tuple[Path, int]]:
    """Write one Parquet file per (booking_year, booking_month) partition. Returns (path, rows)."""
    for stale in existing_batch_partitions(curated_root, batch_id):
        stale.unlink()
    written: list[tuple[Path, int]] = []
    if frame.is_empty():
        return written
    for key, part in frame.partition_by(list(PARTITION_COLUMNS), as_dict=True).items():
        year, month = int(key[0]), int(key[1])
        target = partition_path(curated_root, batch_id, year, month)
        _atomic_write(part, target)
        written.append((target, part.height))
    return sorted(written)


def quarantine_path(quarantine_root: Path, batch_id: str) -> Path:
    return quarantine_root / f"batch_id={batch_id}" / "rejected.parquet"


def quarantine_frame(
    rejections: Sequence[Rejection],
    *,
    batch_id: str,
    source_filename: str,
    source_file_hash: str,
    quarantined_at: datetime,
    pipeline_version: str,
) -> pl.DataFrame:
    rows = [
        {
            "batch_id": batch_id,
            "record_number": r.record_number,
            "transaction_id": r.transaction_id,
            "rule_id": r.rule_id,
            "all_rule_ids": ",".join(r.all_rule_ids),
            "severity": RULES[r.rule_id].severity.value,
            "reason": r.reason,
            "stage": r.stage.value,
            "raw_record": canonical_json(r.raw),
            "source_filename": source_filename,
            "source_file_hash": source_file_hash,
            "quarantined_at": quarantined_at,
            "pipeline_version": pipeline_version,
        }
        for r in rejections
    ]
    return pl.DataFrame(rows, schema=QUARANTINE_POLARS_SCHEMA, orient="row")


def write_quarantine(frame: pl.DataFrame, quarantine_root: Path, batch_id: str) -> Path:
    target = quarantine_path(quarantine_root, batch_id)
    _atomic_write(frame, target)
    return target


def read_quarantine(quarantine_root: Path, batch_id: str) -> pl.DataFrame:
    path = quarantine_path(quarantine_root, batch_id)
    if not path.exists():
        return pl.DataFrame(schema=QUARANTINE_POLARS_SCHEMA)
    return pl.read_parquet(path)


def curated_row_count(curated_root: Path, batch_id: str) -> int:
    """Post-write control: rows actually present on disk for this batch."""
    total = 0
    for path in existing_batch_partitions(curated_root, batch_id):
        total += pl.scan_parquet(path).select(pl.len()).collect().item()
    return int(total)


def decode_raw_record(raw_record: str) -> dict[str, object]:
    return dict(json.loads(raw_record))
