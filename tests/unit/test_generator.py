from __future__ import annotations

from pathlib import Path

import pytest

from txn_pipeline.contracts.enums import SourceFormat
from txn_pipeline.ingestion.readers import read_records
from txn_pipeline.synthetic.generator import (
    COLUMNS,
    INVALID_KINDS,
    GeneratorConfig,
    generate,
    write_rows,
)
from txn_pipeline.validation.duplicates import deduplicate
from txn_pipeline.validation.quality import QualityConfig, apply_quality_rules
from txn_pipeline.validation.schema import validate_schema


def test_same_seed_is_byte_identical_and_other_seed_differs() -> None:
    a, sa = generate(GeneratorConfig(records=500, seed=1))
    b, sb = generate(GeneratorConfig(records=500, seed=1))
    c, _ = generate(GeneratorConfig(records=500, seed=2))
    assert a == b and sa.as_dict() == sb.as_dict()
    assert a != c


def test_rates_produce_expected_counts() -> None:
    cfg = GeneratorConfig(
        records=1000,
        seed=3,
        invalid_rate=0.05,
        duplicate_rate=0.02,
        conflict_rate=0.01,
        warning_rate=0.03,
    )
    rows, summary = generate(cfg)
    assert summary.invalid_records == 50
    assert summary.duplicate_records == 20
    assert summary.conflict_records == 10
    assert summary.warning_records == 30
    assert summary.valid_records == 950
    assert summary.total_rows == len(rows) == 1030
    assert sum(summary.invalid_by_kind.values()) == 50
    assert set(summary.invalid_by_kind) <= set(INVALID_KINDS)
    assert all(set(r) == set(COLUMNS) for r in rows)
    assert all(r["account_id"].startswith("ACC-SYN-") for r in rows)


def test_generated_bad_data_is_classified_by_the_pipeline() -> None:
    cfg = GeneratorConfig(
        records=1000,
        seed=5,
        invalid_rate=0.05,
        duplicate_rate=0.02,
        conflict_rate=0.01,
        warning_rate=0.0,
    )
    rows, summary = generate(cfg)
    raw = [
        __import__("txn_pipeline.validation.results", fromlist=["RawRecord"]).RawRecord(
            i, r, str(i)
        )
        for i, r in enumerate(rows, 1)
    ]
    valid, rejected, _ = validate_schema(raw)
    accepted, rejected_q, _ = apply_quality_rules(valid, QualityConfig())
    dedup = deduplicate(accepted)
    assert len(rejected) + len(rejected_q) == summary.invalid_records
    assert len(dedup.duplicates) == summary.duplicate_records
    assert len(dedup.conflicts) == 2 * summary.conflict_records
    assert len(dedup.accepted) == summary.valid_records - summary.conflict_records


def test_zero_records_and_clean_config() -> None:
    rows, summary = generate(GeneratorConfig(records=0))
    assert rows == [] and summary.total_rows == 0
    rows, summary = generate(
        GeneratorConfig(
            records=50, invalid_rate=0, duplicate_rate=0, conflict_rate=0, warning_rate=0
        )
    )
    assert summary.total_rows == 50 and summary.invalid_records == 0


@pytest.mark.parametrize(
    "bad", [{"records": -1}, {"invalid_rate": 1.5}, {"duplicate_rate": -0.1}, {"reversal_rate": 2}]
)
def test_invalid_config(bad: dict[str, float]) -> None:
    with pytest.raises(ValueError, match="must be"):
        GeneratorConfig(**bad)  # type: ignore[arg-type]


@pytest.mark.parametrize("fmt", list(SourceFormat))
def test_writers_are_readable(tmp_path: Path, fmt: SourceFormat) -> None:
    rows, _ = generate(GeneratorConfig(records=200, seed=9))
    path = tmp_path / f"out.{fmt.value}"
    write_rows(rows, path, fmt)
    records = list(read_records(path, fmt))
    assert len(records) == len(rows)
    assert records[0].data["transaction_id"] == rows[0]["transaction_id"]
    valid, _, _ = validate_schema(records)
    assert len(valid) > 180


def test_typed_parquet_for_clean_data(tmp_path: Path) -> None:
    import pyarrow.parquet as pq

    rows, _ = generate(
        GeneratorConfig(records=50, seed=1, invalid_rate=0, duplicate_rate=0, conflict_rate=0)
    )
    path = tmp_path / "clean.parquet"
    write_rows(rows, path, SourceFormat.PARQUET)
    schema = pq.read_schema(path)
    assert str(schema.field("amount").type) == "decimal128(18, 4)"
    assert str(schema.field("booking_date").type) == "date32[day]"
    assert str(schema.field("is_reversal").type) == "bool"
    dirty, _ = generate(GeneratorConfig(records=200, seed=1, invalid_rate=0.2))
    write_rows(dirty, path, SourceFormat.PARQUET)
    assert str(pq.read_schema(path).field("amount").type) == "string"
