"""CLI tests via Typer's CliRunner against a SQLite metadata database."""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path

import pytest
from typer.testing import CliRunner

from txn_pipeline import PIPELINE_VERSION
from txn_pipeline.cli.main import app
from txn_pipeline.contracts.enums import SourceFormat
from txn_pipeline.warehouse.model import WarehouseBuilder

runner = CliRunner(env={"COLUMNS": "250"})


@pytest.fixture
def cli(tmp_path: Path) -> Callable[..., object]:
    base = [
        "--data-root",
        str(tmp_path / "data"),
        "--database-url",
        f"sqlite:///{tmp_path / 'meta.db'}",
        "--log-level",
        "WARNING",
        "--log-format",
        "json",
    ]

    def invoke(*args: str) -> object:
        return runner.invoke(app, [*base, *args], catch_exceptions=False)

    return invoke


def test_version_and_help(cli: Callable[..., object]) -> None:
    result = runner.invoke(app, ["--version"])
    assert result.exit_code == 0 and PIPELINE_VERSION in result.stdout
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0 and "generate-demo-data" in result.stdout
    assert runner.invoke(app, []).exit_code != 1


def test_generate_validate_run_and_inspect(cli: Callable[..., object], tmp_path: Path) -> None:
    out = tmp_path / "in.csv"
    result = cli("generate-demo-data", "--records", "600", "--seed", "3", "--output", str(out))
    assert result.exit_code == 0, result.output  # type: ignore[attr-defined]
    assert out.exists()

    result = cli("validate", "--input", str(out), "--json")
    assert result.exit_code == 0  # type: ignore[attr-defined]
    summary = json.loads(result.stdout)  # type: ignore[attr-defined]
    assert summary["counts"]["source_records"] > 600
    assert summary["counts"]["rejected_records"] > 0

    result = cli("validate", "--input", str(out), "--strict")
    assert result.exit_code == 1  # type: ignore[attr-defined]
    assert "First" in result.stdout  # type: ignore[attr-defined]

    result = cli("run", "--input", str(out), "--json")
    assert result.exit_code == 0, result.output  # type: ignore[attr-defined]
    payload = json.loads(result.stdout)  # type: ignore[attr-defined]
    batch_id = payload["batch_id"]
    assert payload["status"] == "COMPLETED"
    assert payload["counts"]["curated_records"] == summary["counts"]["accepted_records"]
    assert (
        payload["outputs"]["warehouse_tables"]["fact_transactions"]
        == payload["counts"]["curated_records"]
    )

    result = cli("run", "--input", str(out))
    assert result.exit_code == 0  # type: ignore[attr-defined]
    assert "idempotent skip" in result.stdout  # type: ignore[attr-defined]

    result = cli("batches")
    assert result.exit_code == 0 and batch_id[:8] in result.stdout  # type: ignore[attr-defined]

    result = cli("batch-status", "--batch-id", batch_id)
    assert result.exit_code == 0 and "COMPLETED" in result.stdout and "WAREHOUSE" in result.stdout  # type: ignore[attr-defined]
    result = cli("batch-status", "--batch-id", batch_id, "--json")
    status = json.loads(result.stdout)  # type: ignore[attr-defined]
    assert {c["stage"] for c in status["checkpoints"]} >= {"INGESTION", "COMPLETION", "WAREHOUSE"}
    assert status["lineage"]
    assert cli("batch-status", "--batch-id", "00000000-0000-0000-0000-000000000000").exit_code == 1  # type: ignore[attr-defined]

    result = cli("reconcile", "--batch-id", batch_id)
    assert result.exit_code == 0 and "PASS" in result.stdout and "matches" in result.stdout  # type: ignore[attr-defined]
    result = cli("reconcile", "--batch-id", batch_id, "--json")
    assert json.loads(result.stdout)["disk_matches_report"] is True  # type: ignore[attr-defined]
    assert cli("reconcile", "--batch-id", "00000000-0000-0000-0000-000000000000").exit_code == 1  # type: ignore[attr-defined]

    result = cli("quarantine", "--batch-id", batch_id, "--limit", "3", "--raw")
    assert (
        result.exit_code == 0 and "quarantined records" in result.stdout and "raw:" in result.stdout
    )  # type: ignore[attr-defined]
    result = cli("quarantine", "--batch-id", batch_id, "--rule", "DQ003_INVALID_AMOUNT")
    assert result.exit_code == 0 and "DQ003_INVALID_AMOUNT" in result.stdout  # type: ignore[attr-defined]
    result = cli("quarantine", "--batch-id", batch_id, "--rule", "NO_SUCH_RULE")
    assert "0 quarantined" in result.stdout  # type: ignore[attr-defined]

    result = cli("warehouse-build")
    assert result.exit_code == 0 and "fact_transactions" in result.stdout  # type: ignore[attr-defined]

    assert "daily_volume" in cli("query").stdout  # type: ignore[attr-defined]
    result = cli("query", "channel_distribution", "--limit", "3")
    assert result.exit_code == 0 and "channel_code" in result.stdout  # type: ignore[attr-defined]
    result = cli("query", "daily_volume", "--json")
    assert len(json.loads(result.stdout)) > 0  # type: ignore[attr-defined]
    assert cli("query", "no_such_query").exit_code == 1  # type: ignore[attr-defined]

    result = cli("db-upgrade")
    assert result.exit_code == 0 and "0001" in result.stdout  # type: ignore[attr-defined]


def test_run_without_warehouse_and_query_before_build(
    cli: Callable[..., object], tmp_path: Path
) -> None:
    out = tmp_path / "in.jsonl"
    cli("generate-demo-data", "--records", "100", "--output", str(out), "--format", "jsonl")
    assert cli("query", "daily_volume").exit_code == 1  # type: ignore[attr-defined]
    result = cli("run", "--input", str(out), "--no-warehouse", "--source", "OTHER_SOURCE")
    assert result.exit_code == 0 and "COMPLETED" in result.stdout  # type: ignore[attr-defined]
    assert not (tmp_path / "data" / "warehouse" / "txn_warehouse.duckdb").exists()


def test_run_errors(cli: Callable[..., object], tmp_path: Path) -> None:
    out = tmp_path / "in.csv"
    cli("generate-demo-data", "--records", "10", "--output", str(out))
    assert cli("run", "--input", str(out), "--adapter", "nope").exit_code == 1  # type: ignore[attr-defined]
    assert cli("validate", "--input", str(out), "--adapter", "nope").exit_code == 1  # type: ignore[attr-defined]
    weird = tmp_path / "data.txt"
    weird.write_bytes(out.read_bytes())
    assert cli("run", "--input", str(weird)).exit_code == 1  # type: ignore[attr-defined]
    assert cli("validate", "--input", str(weird)).exit_code == 1  # type: ignore[attr-defined]
    assert cli("run", "--input", str(weird), "--format", "csv").exit_code == 0  # type: ignore[attr-defined]


def test_warehouse_failure_gives_partial_exit_code(
    cli: Callable[..., object], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    out = tmp_path / "in.csv"
    cli("generate-demo-data", "--records", "100", "--output", str(out))

    def boom(self: WarehouseBuilder, batches: object) -> dict[str, int]:
        raise RuntimeError("duckdb exploded")

    monkeypatch.setattr(WarehouseBuilder, "build", boom)
    result = cli("run", "--input", str(out))
    assert result.exit_code == 2 and "PARTIAL" in result.stdout  # type: ignore[attr-defined]
    monkeypatch.undo()
    assert cli("warehouse-build").exit_code == 0  # type: ignore[attr-defined]
    assert cli("warehouse-build").exit_code == 0  # type: ignore[attr-defined]


def test_warehouse_build_without_curated_data(cli: Callable[..., object]) -> None:
    result = cli("warehouse-build")
    assert result.exit_code == 1 and "no curated" in result.output  # type: ignore[attr-defined]


def test_demo_command(cli: Callable[..., object]) -> None:
    result = cli("demo", "--records", "500", "--format", SourceFormat.PARQUET.value)
    assert result.exit_code == 0, result.output  # type: ignore[attr-defined]
    assert "no duplicate load" in result.stdout and "Reconciliation PASS" in result.stdout  # type: ignore[attr-defined]


def test_postgres_schema_guard(tmp_path: Path) -> None:
    result = runner.invoke(
        app,
        [
            "--data-root",
            str(tmp_path),
            "--database-url",
            "postgresql+psycopg://nobody:nothing@127.0.0.1:1/nodb",
            "batches",
        ],
    )
    assert result.exit_code == 1 and "cannot reach" in result.output
