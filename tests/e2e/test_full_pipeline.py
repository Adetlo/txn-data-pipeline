"""End-to-end: file -> run -> quarantine -> curated -> reconciliation -> warehouse -> query."""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest
from typer.testing import CliRunner

from tests.conftest import MakeSettings
from txn_pipeline.cli.main import app
from txn_pipeline.config import Settings
from txn_pipeline.contracts import rules
from txn_pipeline.contracts.enums import BatchStatus, SourceFormat
from txn_pipeline.pipeline.orchestrator import PipelineOrchestrator
from txn_pipeline.synthetic.generator import GeneratorConfig, generate, write_rows
from txn_pipeline.warehouse.model import WarehouseBuilder
from txn_pipeline.warehouse.writer import (
    curated_row_count,
    existing_batch_partitions,
    read_quarantine,
)

runner = CliRunner(env={"COLUMNS": "250"})


def _run_scenario(settings: Settings, tmp_path: Path, fmt: SourceFormat) -> None:
    rows, summary = generate(GeneratorConfig(records=2000, seed=7))
    source = tmp_path / "incoming" / f"e2e.{fmt.value}"
    write_rows(rows, source, fmt)

    result = PipelineOrchestrator(settings).run(source, source_name="SYNTHETIC_CORE")
    assert result.status is BatchStatus.COMPLETED, result.error
    c = result.counts
    assert c.source_records == summary.total_rows
    assert c.rejected_records == summary.invalid_records + 2 * summary.conflict_records
    assert c.conflict_records == 2 * summary.conflict_records
    assert c.duplicate_records == summary.duplicate_records
    assert c.accepted_records == summary.valid_records - summary.conflict_records
    assert c.source_records == c.accepted_records + c.rejected_records + c.duplicate_records

    quarantine = read_quarantine(settings.quarantine_root, result.batch_id)
    assert quarantine.height == c.rejected_records > 0
    assert (
        quarantine.filter(pl.col("rule_id") == rules.DQ006_DUPLICATE_TRANSACTION).height
        == 2 * summary.conflict_records
    )
    assert quarantine.filter(pl.col("stage") == "DEDUPLICATION").height == c.conflict_records

    curated = pl.concat(
        [
            pl.read_parquet(p)
            for p in existing_batch_partitions(settings.curated_root, result.batch_id)
        ]
    )
    assert (
        curated.height
        == c.accepted_records
        == curated_row_count(settings.curated_root, result.batch_id)
    )
    assert curated["transaction_id"].n_unique() == curated.height
    assert curated["currency"].str.to_uppercase().equals(curated["currency"])

    report = result.reconciliation
    assert report is not None and report["balanced"] is True
    assert set(report["currency_totals"]) >= {"GBP", "EUR", "USD", "JPY"}
    for totals in report["currency_totals"].values():
        assert totals["source"] == totals["curated"] and totals["balanced"]
    assert json.loads(Path(result.report_file).read_text())["balanced"] is True  # type: ignore[arg-type]

    assert result.warehouse_tables["fact_transactions"] == c.curated_records
    builder = WarehouseBuilder(
        settings.duckdb_path, settings.curated_root, settings.quarantine_root
    )
    for name in (
        "daily_volume",
        "daily_value_by_currency",
        "rejected_trend",
        "duplicate_rate",
        "batch_quality_trend",
        "weekend_activity",
    ):
        assert builder.run_query(name, limit=10).height > 0, name
    trend = builder.run_query("batch_quality_trend").to_dicts()[0]
    assert trend["status"] == "COMPLETED" and trend["reconciliation_balanced"] is True

    base = [
        "--data-root",
        str(settings.data_root),
        "--database-url",
        settings.metadata_database_url,
        "--log-level",
        "WARNING",
    ]
    status = runner.invoke(app, [*base, "batch-status", "--batch-id", result.batch_id, "--json"])
    assert status.exit_code == 0, status.output
    payload = json.loads(status.stdout)
    assert payload["status"] == "COMPLETED"
    assert {cp["stage"] for cp in payload["checkpoints"] if cp["status"] == "COMPLETED"} >= {
        "INGESTION",
        "SCHEMA_VALIDATION",
        "QUALITY_VALIDATION",
        "DEDUPLICATION",
        "TRANSFORMATION",
        "RECONCILIATION",
        "QUARANTINE_WRITE",
        "CURATED_WRITE",
        "REGISTRY_COMMIT",
        "WAREHOUSE",
        "COMPLETION",
    }
    reconcile = runner.invoke(app, [*base, "reconcile", "--batch-id", result.batch_id])
    assert reconcile.exit_code == 0 and "PASS" in reconcile.stdout

    again = PipelineOrchestrator(settings).run(source, source_name="SYNTHETIC_CORE")
    assert again.skipped and again.batch_id == result.batch_id
    assert curated_row_count(settings.curated_root, result.batch_id) == c.accepted_records


@pytest.mark.parametrize("fmt", list(SourceFormat))
def test_full_pipeline_sqlite(settings: Settings, tmp_path: Path, fmt: SourceFormat) -> None:
    _run_scenario(settings, tmp_path, fmt)


@pytest.mark.postgres
def test_full_pipeline_postgres(
    make_settings: MakeSettings, postgres_url: str, tmp_path: Path
) -> None:
    _run_scenario(make_settings(postgres_url), tmp_path, SourceFormat.CSV)
