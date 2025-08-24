from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import polars as pl
import pytest

from tests.conftest import valid_row
from txn_pipeline.contracts.enums import Severity, Stage
from txn_pipeline.contracts.schemas import QUARANTINE_POLARS_SCHEMA
from txn_pipeline.contracts.transaction import TransactionRecord
from txn_pipeline.exceptions import OutputWriteError
from txn_pipeline.ingestion.hashing import sha256_record
from txn_pipeline.transformation.frame import build_curated_frame
from txn_pipeline.validation.duplicates import deduplicate
from txn_pipeline.validation.results import Finding, Rejection, ValidatedRecord
from txn_pipeline.warehouse.writer import (
    curated_row_count,
    decode_raw_record,
    existing_batch_partitions,
    partition_path,
    quarantine_frame,
    quarantine_path,
    read_quarantine,
    write_curated,
    write_quarantine,
)


def _frame(*rows: dict[str, object], batch_id: str = "b1") -> pl.DataFrame:
    items = [
        ValidatedRecord(i, r, sha256_record(r), TransactionRecord.model_validate(r))
        for i, r in enumerate(rows, 1)
    ]
    return build_curated_frame(
        items,
        deduplicate(items).payload_hashes,
        batch_id=batch_id,
        source_filename="f.csv",
        source_file_hash="c" * 64,
        ingested_at=datetime(2026, 10, 13, tzinfo=UTC),
        pipeline_version="0.1.0",
    )


def test_partition_path_is_deterministic(tmp_path: Path) -> None:
    assert (
        partition_path(tmp_path, "b1", 2026, 3)
        == tmp_path / "booking_year=2026" / "booking_month=03" / "batch_id=b1.parquet"
    )
    assert quarantine_path(tmp_path, "b1") == tmp_path / "batch_id=b1" / "rejected.parquet"


def test_write_curated_partitions_and_idempotent_rewrite(tmp_path: Path) -> None:
    root = tmp_path / "curated"
    frame = _frame(
        valid_row(
            transaction_id="T1",
            booking_date="2026-09-30",
            value_date="2026-09-30",
            transaction_timestamp="2026-09-30T10:00:00Z",
        ),
        valid_row(transaction_id="T2"),
        valid_row(transaction_id="T3"),
    )
    written = write_curated(frame, root, "b1")
    assert [(p.relative_to(root).as_posix(), n) for p, n in written] == [
        ("booking_year=2026/booking_month=09/batch_id=b1.parquet", 1),
        ("booking_year=2026/booking_month=10/batch_id=b1.parquet", 2),
    ]
    assert curated_row_count(root, "b1") == 3
    assert not list(root.rglob("*.tmp"))

    # Re-write with fewer rows: the stale September partition must disappear.
    smaller = frame.filter(pl.col("transaction_id") != "T1")
    write_curated(smaller, root, "b1")
    assert [p.name for p in existing_batch_partitions(root, "b1")] == ["batch_id=b1.parquet"]
    assert len(existing_batch_partitions(root, "b1")) == 1
    assert curated_row_count(root, "b1") == 2
    assert curated_row_count(root, "other") == 0
    assert existing_batch_partitions(tmp_path / "nope", "b1") == []

    # Another batch in the same partition does not disturb b1.
    write_curated(_frame(valid_row(transaction_id="T9"), batch_id="b2"), root, "b2")
    assert curated_row_count(root, "b1") == 2 and curated_row_count(root, "b2") == 1
    assert write_curated(frame.clear(), root, "b3") == []


def test_write_curated_error(tmp_path: Path) -> None:
    root = tmp_path / "curated"
    root.mkdir()
    (root / "booking_year=2026").write_text("not a directory")
    with pytest.raises(OutputWriteError):
        write_curated(_frame(valid_row()), root, "b1")


def test_quarantine_round_trip(tmp_path: Path) -> None:
    raw = valid_row(amount="abc")
    rejections = [
        Rejection(
            3,
            raw,
            "DQ003_INVALID_AMOUNT",
            "DQ003_INVALID_AMOUNT: bad",
            Stage.SCHEMA_VALIDATION,
            (
                Finding("DQ003_INVALID_AMOUNT", Severity.ERROR, "bad"),
                Finding("DQ007_UNKNOWN_STATUS", Severity.ERROR, "x"),
            ),
            raw["transaction_id"],
        ),
    ]
    frame = quarantine_frame(
        rejections,
        batch_id="b1",
        source_filename="f.csv",
        source_file_hash="c" * 64,
        quarantined_at=datetime(2026, 10, 13, tzinfo=UTC),
        pipeline_version="0.1.0",
    )
    assert dict(frame.schema) == QUARANTINE_POLARS_SCHEMA
    path = write_quarantine(frame, tmp_path / "q", "b1")
    assert path.exists()
    back = read_quarantine(tmp_path / "q", "b1")
    row = back.to_dicts()[0]
    assert row["record_number"] == 3
    assert row["all_rule_ids"] == "DQ003_INVALID_AMOUNT,DQ007_UNKNOWN_STATUS"
    assert row["severity"] == "ERROR"
    assert row["stage"] == "SCHEMA_VALIDATION"
    assert decode_raw_record(row["raw_record"]) == raw
    assert read_quarantine(tmp_path / "q", "missing").height == 0
