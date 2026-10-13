"""Integration tests for the orchestrator, metadata, outputs and warehouse on SQLite."""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path

import duckdb
import polars as pl
import pytest
from sqlalchemy import select

from tests.conftest import MakeSettings
from txn_pipeline.config import Settings
from txn_pipeline.contracts import rules
from txn_pipeline.contracts.enums import BatchStatus, CheckpointStatus, SourceFormat, Stage
from txn_pipeline.contracts.schemas import CURATED_POLARS_SCHEMA
from txn_pipeline.exceptions import StageError
from txn_pipeline.metadata.database import build_engine
from txn_pipeline.metadata.models import (
    LineageRecordModel,
    PipelineBatchModel,
    QualityResultModel,
    QuarantineRecordModel,
    SourceFileModel,
    TransactionRegistryModel,
)
from txn_pipeline.metadata.repository import MetadataRepository
from txn_pipeline.pipeline.orchestrator import PipelineOrchestrator
from txn_pipeline.pipeline.stages import BatchContext
from txn_pipeline.synthetic.generator import GeneratorConfig, generate, write_rows
from txn_pipeline.warehouse.model import (
    DIMENSION_TABLES,
    FACT_TABLES,
    WarehouseBuilder,
    available_queries,
)
from txn_pipeline.warehouse.writer import (
    curated_row_count,
    existing_batch_partitions,
    read_quarantine,
)

SyntheticFile = Callable[..., Path]


def _curated(settings: Settings, batch_id: str) -> pl.DataFrame:
    files = existing_batch_partitions(settings.curated_root, batch_id)
    return (
        pl.concat([pl.read_parquet(f) for f in files])
        if files
        else pl.DataFrame(schema=CURATED_POLARS_SCHEMA)
    )


@pytest.mark.parametrize("fmt", list(SourceFormat))
def test_all_formats_give_identical_results(
    settings: Settings, synthetic_file: SyntheticFile, fmt: SourceFormat
) -> None:
    path = synthetic_file(records=800, seed=11, fmt=fmt)
    result = PipelineOrchestrator(settings).run(path)
    assert result.status is BatchStatus.COMPLETED, result.error
    c = result.counts
    assert (
        c.source_records,
        c.accepted_records,
        c.rejected_records,
        c.duplicate_records,
        c.conflict_records,
    ) == (
        812,
        780,
        24,
        8,
        8,
    )
    ids = sorted(_curated(settings, result.batch_id)["transaction_id"].to_list())
    assert len(ids) == 780 and ids == sorted(set(ids))
    assert result.rule_counts[rules.DQ006_DUPLICATE_TRANSACTION] == 8


def test_metadata_persistence(settings: Settings, synthetic_file: SyntheticFile) -> None:
    path = synthetic_file(records=600, seed=4)
    result = PipelineOrchestrator(settings).run(path)
    assert result.status is BatchStatus.COMPLETED
    engine = build_engine(settings.metadata_database_url)
    with MetadataRepository(engine) as repo:
        s = repo.session
        source = s.scalars(select(SourceFileModel)).one()
        assert (
            source.sha256 == result.source_hash
            and source.format == "csv"
            and source.filename == path.name
        )
        batch = s.scalars(select(PipelineBatchModel)).one()
        assert str(batch.id) == result.batch_id
        assert batch.status == "COMPLETED" and batch.actor == "TEST" and batch.attempt == 1
        assert batch.record_count == result.counts.source_records
        assert batch.accepted_count == result.counts.accepted_records
        assert batch.rejected_count == result.counts.rejected_records == batch.quarantine_count
        assert batch.duplicate_count == result.counts.duplicate_records
        assert batch.conflict_count == result.counts.conflict_records
        assert batch.reconciliation_balanced is True
        assert batch.reconciliation_report["balanced"] is True
        assert batch.metrics["records_processed"] == result.counts.source_records
        assert batch.completed_at is not None and batch.config_hash == settings.config_hash()
        assert len(batch.correlation_id) == 32

        quality = {q.rule_id: q.record_count for q in s.scalars(select(QualityResultModel))}
        assert quality == result.rule_counts
        for q in s.scalars(select(QualityResultModel)):
            assert q.severity == rules.RULES[q.rule_id].severity.value

        quarantine = list(s.scalars(select(QuarantineRecordModel)))
        assert len(quarantine) == result.counts.rejected_records
        first = quarantine[0]
        assert isinstance(first.raw_record, dict) and "transaction_id" in first.raw_record
        assert json.loads(json.dumps(first.raw_record)) == first.raw_record
        assert {q.stage for q in quarantine} <= {
            "SCHEMA_VALIDATION",
            "QUALITY_VALIDATION",
            "DEDUPLICATION",
        }

        checkpoints = {c.stage: c for c in repo.checkpoints(result.batch_id)}
        for stage in (
            Stage.INGESTION,
            Stage.SCHEMA_VALIDATION,
            Stage.QUALITY_VALIDATION,
            Stage.DEDUPLICATION,
            Stage.TRANSFORMATION,
            Stage.RECONCILIATION,
            Stage.QUARANTINE_WRITE,
            Stage.CURATED_WRITE,
            Stage.REGISTRY_COMMIT,
            Stage.WAREHOUSE,
            Stage.COMPLETION,
        ):
            assert checkpoints[stage].status is CheckpointStatus.COMPLETED, stage
            assert checkpoints[stage].completed_at is not None
        assert checkpoints[Stage.INGESTION].details == {
            "record_count": result.counts.source_records
        }

        lineage = list(s.scalars(select(LineageRecordModel)))
        kinds = {item.output_kind for item in lineage}
        assert kinds == {
            "CURATED_PARTITION",
            "QUARANTINE_FILE",
            "RECONCILIATION_REPORT",
            "WAREHOUSE",
        }
        partitions = [item for item in lineage if item.output_kind == "CURATED_PARTITION"]
        assert len(partitions) == len(result.curated_files)
        assert sum(item.row_count for item in partitions) == result.counts.curated_records
        assert all(item.source_hash == result.source_hash for item in lineage)

        registry = list(s.scalars(select(TransactionRegistryModel)))
        assert len(registry) == result.counts.accepted_records
        assert {str(r.batch_id) for r in registry} == {result.batch_id}
        assert repo.registry_count_for_batch(result.batch_id) == result.counts.accepted_records
        assert len(repo.list_batches()) == 1
        assert len(repo.lineage_for_batch(result.batch_id)) == len(lineage)
        assert len(repo.quarantine_for_batch(result.batch_id, limit=5)) == 5
        assert repo.quality_results_for_batch(result.batch_id) == result.rule_counts
    engine.dispose()


def test_quarantine_and_curated_parquet_contents(
    settings: Settings, synthetic_file: SyntheticFile
) -> None:
    path = synthetic_file(records=500, seed=8)
    result = PipelineOrchestrator(settings).run(path)
    q = read_quarantine(settings.quarantine_root, result.batch_id)
    assert q.height == result.counts.rejected_records
    assert set(q["stage"].unique().to_list()) <= {
        "SCHEMA_VALIDATION",
        "QUALITY_VALIDATION",
        "DEDUPLICATION",
    }
    assert set(q["rule_id"].unique().to_list()) <= set(rules.RULES)
    assert all(json.loads(r) for r in q["raw_record"].to_list())
    assert q["batch_id"].unique().to_list() == [result.batch_id]
    assert q["source_file_hash"].unique().to_list() == [result.source_hash]
    assert (
        q.filter(pl.col("rule_id") == rules.DQ006_DUPLICATE_TRANSACTION).height
        == result.counts.conflict_records
    )

    curated = _curated(settings, result.batch_id)
    assert dict(curated.schema) == CURATED_POLARS_SCHEMA
    assert curated.height == result.counts.curated_records == result.counts.accepted_records
    for p in result.curated_files:
        rel = Path(p).relative_to(settings.curated_root).as_posix()
        assert (
            rel.startswith("booking_year=")
            and "/booking_month=" in rel
            and rel.endswith(f"batch_id={result.batch_id}.parquet")
        )
    scale = curated.select(
        (pl.col("amount") * (10 ** pl.col("currency_exponent").cast(pl.Int64)))
        .cast(pl.Int64)
        .alias("m")
    )
    assert scale["m"].to_list() == curated["amount_minor"].to_list()
    assert curated.schema["transaction_timestamp"] == pl.Datetime("us", "UTC")
    assert curated["batch_id"].unique().to_list() == [result.batch_id]
    assert curated["source_filename"].unique().to_list() == [path.name]
    assert curated["source_file_hash"].unique().to_list() == [result.source_hash]
    assert curated["ingested_at"].null_count() == 0 and curated["payload_hash"].null_count() == 0
    assert curated.filter(pl.col("booking_year") != pl.col("booking_date").dt.year()).height == 0
    assert Path(result.report_file).exists()  # type: ignore[arg-type]


def test_duckdb_warehouse(settings: Settings, synthetic_file: SyntheticFile) -> None:
    result = PipelineOrchestrator(settings).run(synthetic_file(records=700, seed=2))
    builder = WarehouseBuilder(
        settings.duckdb_path, settings.curated_root, settings.quarantine_root
    )
    con = duckdb.connect(str(settings.duckdb_path), read_only=True)
    try:
        tables = {r[0] for r in con.execute("SHOW TABLES").fetchall()}
        assert set(DIMENSION_TABLES) | set(FACT_TABLES) <= tables
        assert "stg_curated" not in tables
        assert (
            con.execute("SELECT COUNT(*) FROM fact_transactions").fetchone()[0]
            == result.counts.curated_records
        )
        assert (
            con.execute("SELECT COUNT(DISTINCT transaction_key) FROM fact_transactions").fetchone()[
                0
            ]
            == result.counts.curated_records
        )
        lo, hi = con.execute("SELECT MIN(full_date), MAX(full_date) FROM dim_date").fetchone()
        blo, bhi = con.execute(
            "SELECT MIN(d.full_date), MAX(v.full_date) FROM fact_transactions f "
            "JOIN dim_date d ON d.date_key = f.booking_date_key "
            "JOIN dim_date v ON v.date_key = f.value_date_key"
        ).fetchone()
        assert lo <= blo and hi >= bhi
        sentinel = (
            "SELECT COUNT(*) FROM dim_counterparty "
            "WHERE counterparty_key = -1 AND counterparty_id = 'UNKNOWN'"
        )
        assert con.execute(sentinel).fetchone()[0] == 1
        orphan_cp = (
            "SELECT COUNT(*) FROM fact_transactions f LEFT JOIN dim_counterparty c "
            "USING (counterparty_key) WHERE c.counterparty_id IS NULL"
        )
        assert con.execute(orphan_cp).fetchone()[0] == 0
        orphan_acc = (
            "SELECT COUNT(*) FROM fact_transactions f LEFT JOIN dim_account a "
            "USING (account_key) WHERE a.account_id IS NULL"
        )
        assert con.execute(orphan_acc).fetchone()[0] == 0
        assert con.execute("SELECT COUNT(*) FROM dim_currency").fetchone()[0] == 7
        by_currency = (
            "SELECT c.currency_code, SUM(f.amount) FROM fact_transactions f "
            "JOIN dim_currency c USING (currency_key) GROUP BY 1"
        )
        totals = {row[0]: str(row[1]) for row in con.execute(by_currency).fetchall()}
        keys_sql = "SELECT account_key, account_id FROM dim_account ORDER BY account_id"
        keys_before = con.execute(keys_sql).fetchall()
        assert con.execute("SELECT status, record_count FROM fact_batch_quality").fetchall() == [
            ("COMPLETED", result.counts.source_records)
        ]
        quarantine_count = con.execute("SELECT COUNT(*) FROM fact_quarantine").fetchone()[0]
        assert quarantine_count == result.counts.rejected_records
    finally:
        con.close()
    report = result.reconciliation
    assert report is not None
    for code, t in report["currency_totals"].items():
        assert float(totals[code]) == float(t["curated"])

    for name in available_queries():
        frame = builder.run_query(name, limit=5)
        assert frame.height > 0, name
    with pytest.raises(KeyError):
        builder.run_query("nope")

    with MetadataRepository(build_engine(settings.metadata_database_url)) as repo:
        counts = builder.build(repo.batch_summaries())
    assert counts["fact_transactions"] == result.counts.curated_records
    assert builder.table_counts() == counts
    con = duckdb.connect(str(settings.duckdb_path), read_only=True)
    try:
        assert con.execute(keys_sql).fetchall() == keys_before
    finally:
        con.close()
    assert "daily_volume" in available_queries()


def test_warehouse_without_curated_files(tmp_path: Path) -> None:
    builder = WarehouseBuilder(tmp_path / "w.duckdb", tmp_path / "nope", tmp_path / "nope")
    with pytest.raises(StageError, match="no curated"):
        builder.build([])
    assert builder.curated_files() == []


def test_restart_after_failure_before_registry_commit(
    settings: Settings, synthetic_file: SyntheticFile
) -> None:
    path = synthetic_file(records=400, seed=6)

    def crash(ctx: BatchContext) -> None:
        raise OSError("disk full after curated write")

    failed = PipelineOrchestrator(settings, after_stage={Stage.CURATED_WRITE: crash}).run(path)
    assert failed.status is BatchStatus.FAILED
    assert failed.error is not None and "disk full" in failed.error
    assert (
        curated_row_count(settings.curated_root, failed.batch_id) == failed.counts.accepted_records
    )
    with MetadataRepository(build_engine(settings.metadata_database_url)) as repo:
        batch = repo.require_batch(failed.batch_id)
        assert batch.status == "FAILED" and "disk full" in (batch.error_message or "")
        cps = {c.stage: c.status for c in repo.checkpoints(failed.batch_id)}
        assert cps[Stage.CURATED_WRITE] is CheckpointStatus.FAILED
        assert cps[Stage.COMPLETION] is CheckpointStatus.FAILED
        assert Stage.REGISTRY_COMMIT not in cps
        assert repo.registry_count() == 0

    retry = PipelineOrchestrator(settings).run(path)
    assert retry.status is BatchStatus.COMPLETED
    assert retry.batch_id == failed.batch_id and retry.resumed and retry.attempt == 2
    assert retry.counts.accepted_records == failed.counts.accepted_records
    assert curated_row_count(settings.curated_root, retry.batch_id) == retry.counts.accepted_records
    assert len(existing_batch_partitions(settings.curated_root, retry.batch_id)) == len(
        retry.curated_files
    )
    with MetadataRepository(build_engine(settings.metadata_database_url)) as repo:
        assert repo.registry_count() == retry.counts.accepted_records
        assert len(repo.list_batches()) == 1
        cps = {c.stage: c for c in repo.checkpoints(retry.batch_id)}
        assert all(c.status is CheckpointStatus.COMPLETED for c in cps.values())
        assert cps[Stage.CURATED_WRITE].attempt == 2

    third = PipelineOrchestrator(settings).run(path)
    assert third.skipped and third.batch_id == retry.batch_id


def test_restart_after_failure_at_registry_commit(
    settings: Settings, synthetic_file: SyntheticFile
) -> None:
    path = synthetic_file(records=300, seed=16)

    def crash(ctx: BatchContext) -> None:
        raise OSError("crash during registry commit")

    failed = PipelineOrchestrator(settings, after_stage={Stage.REGISTRY_COMMIT: crash}).run(path)
    assert failed.status is BatchStatus.FAILED
    with MetadataRepository(build_engine(settings.metadata_database_url)) as repo:
        assert repo.registry_count() == 0  # rolled back
        assert {c.stage: c.status for c in repo.checkpoints(failed.batch_id)}[
            Stage.REGISTRY_COMMIT
        ] is CheckpointStatus.FAILED
    retry = PipelineOrchestrator(settings).run(path)
    assert retry.status is BatchStatus.COMPLETED and retry.resumed
    assert curated_row_count(settings.curated_root, retry.batch_id) == retry.counts.accepted_records


def test_parse_failure_does_not_corrupt_batch(settings: Settings, tmp_path: Path) -> None:
    rows, _ = generate(
        GeneratorConfig(records=50, seed=1, invalid_rate=0, duplicate_rate=0, conflict_rate=0)
    )
    path = tmp_path / "mixed.jsonl"
    write_rows(rows, path, SourceFormat.JSONL)
    with path.open("a") as fh:
        fh.write("{this is not json\n[1, 2, 3]\n")
    result = PipelineOrchestrator(settings).run(path)
    assert result.status is BatchStatus.COMPLETED
    assert result.counts.source_records == 52 and result.counts.accepted_records == 50
    assert result.counts.rejected_records == 2
    q = read_quarantine(settings.quarantine_root, result.batch_id)
    assert q.height == 2 and all("_unparseable_line" in r for r in q["raw_record"].to_list())


def test_transformation_failure_fails_batch(
    settings: Settings, synthetic_file: SyntheticFile
) -> None:
    path = synthetic_file(records=200, seed=12)

    def tamper(ctx: BatchContext) -> None:
        ctx.dedup.payload_hashes.clear()  # build_curated_frame will KeyError

    result = PipelineOrchestrator(settings, before_stage={Stage.TRANSFORMATION: tamper}).run(path)
    assert result.status is BatchStatus.FAILED
    assert result.error is not None and "KeyError" in result.error
    assert result.curated_files == [] and result.reconciliation is None
    assert existing_batch_partitions(settings.curated_root, result.batch_id) == []
    assert not (settings.quarantine_root / f"batch_id={result.batch_id}").exists()
    with MetadataRepository(build_engine(settings.metadata_database_url)) as repo:
        batch = repo.require_batch(result.batch_id)
        assert batch.status == "FAILED" and batch.record_count == result.counts.source_records
        cps = {c.stage: c.status for c in repo.checkpoints(result.batch_id)}
        assert cps[Stage.TRANSFORMATION] is CheckpointStatus.FAILED
        assert cps[Stage.DEDUPLICATION] is CheckpointStatus.COMPLETED


def test_unexpected_programming_error_is_recorded_and_raised(
    settings: Settings, synthetic_file: SyntheticFile
) -> None:
    path = synthetic_file(records=100, seed=13)

    def boom(ctx: BatchContext) -> None:
        raise RuntimeError("bug")

    with pytest.raises(RuntimeError, match="bug"):
        PipelineOrchestrator(settings, before_stage={Stage.SCHEMA_VALIDATION: boom}).run(path)
    with MetadataRepository(build_engine(settings.metadata_database_url)) as repo:
        batch = repo.list_batches()[0]
        assert batch.status == "FAILED" and "RuntimeError: bug" in (batch.error_message or "")


def test_reconciliation_failure_prevents_success(
    settings: Settings, synthetic_file: SyntheticFile
) -> None:
    path = synthetic_file(records=200, seed=14)

    def drop_row(ctx: BatchContext) -> None:
        ctx.curated = ctx.curated.head(ctx.curated.height - 1)

    result = PipelineOrchestrator(settings, before_stage={Stage.RECONCILIATION: drop_row}).run(path)
    assert result.status is BatchStatus.FAILED
    assert result.error is not None and "ReconciliationError" in result.error
    assert result.reconciliation is not None and result.reconciliation["balanced"] is False
    assert result.reconciliation["difference"] == 0  # row accounting fine...
    assert (
        result.reconciliation["curated_records"] == result.counts.accepted_records - 1
    )  # ...but curated != accepted
    assert existing_batch_partitions(settings.curated_root, result.batch_id) == []
    with MetadataRepository(build_engine(settings.metadata_database_url)) as repo:
        assert repo.registry_count() == 0
        batch = repo.require_batch(result.batch_id)
        assert batch.reconciliation_balanced is False and batch.status == "FAILED"


def test_warehouse_failure_degrades_to_partial(
    settings: Settings, synthetic_file: SyntheticFile, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = synthetic_file(records=200, seed=15)

    def boom(self: WarehouseBuilder, batches: object) -> dict[str, int]:
        raise RuntimeError("duckdb unavailable")

    monkeypatch.setattr(WarehouseBuilder, "build", boom)
    result = PipelineOrchestrator(settings).run(path)
    assert result.status is BatchStatus.PARTIAL and result.succeeded
    assert (
        curated_row_count(settings.curated_root, result.batch_id) == result.counts.accepted_records
    )
    assert any("warehouse build failed" in n for n in result.notes)
    with MetadataRepository(build_engine(settings.metadata_database_url)) as repo:
        batch = repo.require_batch(result.batch_id)
        assert batch.status == "PARTIAL" and "duckdb unavailable" in (batch.error_message or "")
        assert repo.registry_count() == result.counts.accepted_records
        cps = {c.stage: c.status for c in repo.checkpoints(result.batch_id)}
        assert (
            cps[Stage.WAREHOUSE] is CheckpointStatus.FAILED
            and cps[Stage.COMPLETION] is CheckpointStatus.COMPLETED
        )
    monkeypatch.undo()
    retry = PipelineOrchestrator(settings).run(path)
    assert (
        retry.status is BatchStatus.COMPLETED
        and retry.resumed
        and retry.batch_id == result.batch_id
    )
    assert retry.warehouse_tables["fact_transactions"] == retry.counts.curated_records


def test_no_warehouse_flag_skips_stage(settings: Settings, synthetic_file: SyntheticFile) -> None:
    result = PipelineOrchestrator(settings).run(
        synthetic_file(records=100, seed=17), build_warehouse=False
    )
    assert result.status is BatchStatus.COMPLETED and result.warehouse_tables == {}
    with MetadataRepository(build_engine(settings.metadata_database_url)) as repo:
        assert {c.stage: c.status for c in repo.checkpoints(result.batch_id)}[
            Stage.WAREHOUSE
        ] is CheckpointStatus.SKIPPED


def test_force_rerun_rewrites_identical_output(
    settings: Settings, synthetic_file: SyntheticFile
) -> None:
    path = synthetic_file(records=300, seed=18)
    first = PipelineOrchestrator(settings).run(path)
    ids_before = sorted(_curated(settings, first.batch_id)["transaction_id"].to_list())
    forced = PipelineOrchestrator(settings).run(path, force=True)
    assert (
        forced.batch_id == first.batch_id
        and forced.resumed
        and forced.attempt == 2
        and not forced.skipped
    )
    assert forced.status is BatchStatus.COMPLETED
    assert forced.counts.cross_batch_duplicates == 0
    assert sorted(_curated(settings, first.batch_id)["transaction_id"].to_list()) == ids_before
    assert curated_row_count(settings.curated_root, first.batch_id) == first.counts.accepted_records
    with MetadataRepository(build_engine(settings.metadata_database_url)) as repo:
        assert repo.registry_count() == first.counts.accepted_records
        assert len(repo.list_batches()) == 1


def test_new_file_version_cross_batch_classification(settings: Settings, tmp_path: Path) -> None:
    rows, _ = generate(
        GeneratorConfig(
            records=100, seed=20, invalid_rate=0, duplicate_rate=0, conflict_rate=0, warning_rate=0
        )
    )
    path = tmp_path / "incoming" / "daily.csv"
    write_rows(rows, path, SourceFormat.CSV)
    v1 = PipelineOrchestrator(settings).run(path)
    assert v1.counts.accepted_records == 100

    changed = dict(rows[0])
    changed["amount"] = "1.00" if changed["amount"] != "1.00" else "2.00"
    new = dict(rows[1])
    new["transaction_id"] = "TXN-SYN-20261013-999999"
    write_rows([*rows[2:], changed, rows[1], new], path, SourceFormat.CSV)
    v2 = PipelineOrchestrator(settings).run(path)
    assert v2.batch_id != v1.batch_id and v2.status is BatchStatus.COMPLETED and not v2.resumed
    assert v2.counts.source_records == 101
    assert v2.counts.cross_batch_duplicates == 99
    assert v2.counts.conflict_records == 1 and v2.counts.rejected_records == 1
    assert v2.counts.accepted_records == 1
    q = read_quarantine(settings.quarantine_root, v2.batch_id)
    assert q["rule_id"].to_list() == [rules.DQ006_DUPLICATE_TRANSACTION]
    assert f"batch {v1.batch_id}" in q["reason"][0]
    assert _curated(settings, v2.batch_id)["transaction_id"].to_list() == [
        "TXN-SYN-20261013-999999"
    ]
    with MetadataRepository(build_engine(settings.metadata_database_url)) as repo:
        assert repo.registry_count() == 101
        assert len(repo.list_batches()) == 2
        sources = list(repo.session.scalars(select(SourceFileModel)))
        assert len(sources) == 2 and {s.filename for s in sources} == {"daily.csv"}
        assert (
            len(
                repo.previous_versions_of_filename(
                    "SYNTHETIC_CORE", "daily.csv", exclude_hash=v2.source_hash
                )
            )
            == 1
        )
    assert v2.warehouse_tables["fact_transactions"] == 101


def test_source_timezone_accepts_naive_timestamps(
    make_settings: MakeSettings, sqlite_url: str, tmp_path: Path
) -> None:
    rows, _ = generate(
        GeneratorConfig(records=40, seed=21, invalid_rate=0, duplicate_rate=0, conflict_rate=0)
    )
    for r in rows:
        r["transaction_timestamp"] = r["transaction_timestamp"][:19]
    path = tmp_path / "naive.csv"
    write_rows(rows, path, SourceFormat.CSV)
    strict = PipelineOrchestrator(make_settings(sqlite_url)).run(path, build_warehouse=False)
    assert strict.counts.accepted_records == 0
    assert strict.rule_counts[rules.DQ009_INVALID_TIMESTAMP] == 40

    lenient = make_settings(
        sqlite_url, source_timezone="Asia/Singapore", data_root=tmp_path / "data2"
    )
    assert lenient.config_hash() != make_settings(sqlite_url).config_hash()
    result = PipelineOrchestrator(lenient).run(
        path, source_name="LENIENT_SOURCE", build_warehouse=False
    )
    assert result.counts.accepted_records == 40
    curated = _curated(lenient, result.batch_id)
    assert curated["transaction_timestamp_raw"][0] == rows[0]["transaction_timestamp"]


def test_fail_on_warnings_setting(
    make_settings: MakeSettings, sqlite_url: str, synthetic_file: SyntheticFile
) -> None:
    path = synthetic_file(
        records=300, seed=22, invalid_rate=0, duplicate_rate=0, conflict_rate=0, warning_rate=0.1
    )
    settings = make_settings(sqlite_url, fail_on_warnings=True)
    result = PipelineOrchestrator(settings).run(path, build_warehouse=False)
    assert result.counts.rejected_records >= 30
    assert result.counts.warning_records == 0


def test_validate_only_has_no_side_effects(
    settings: Settings, synthetic_file: SyntheticFile
) -> None:
    path = synthetic_file(records=300, seed=23)
    summary = PipelineOrchestrator(settings).validate_only(path, sample=3)
    assert summary.counts.source_records > 300
    assert summary.counts.rejected_records > 0 and len(summary.sample_rejections) == 3
    assert summary.counts.conflict_records > 0
    assert not any(settings.curated_root.iterdir())
    with MetadataRepository(build_engine(settings.metadata_database_url)) as repo:
        assert repo.list_batches() == []
    run = PipelineOrchestrator(settings).run(path, build_warehouse=False)
    assert run.counts.as_dict() == summary.counts.as_dict()
    assert run.rule_counts == summary.rule_counts


def test_corebank_adapter_end_to_end(settings: Settings, tmp_path: Path) -> None:
    from txn_pipeline.adapters import get_adapter

    entries = []
    for i in range(1, 6):
        entries.append(
            {
                "id": f"3f9c0d0e-0000-4000-8000-00000000000{i}",
                "reference": f"TRF-DEMO-{i:04d}",
                "booking_date": "2026-10-13",
                "value_date": "2026-10-13",
                "status": "POSTED",
                "currency": "GBP",
                "metadata": {"kind": "TRANSFER"},
                "reversal_of": None if i < 5 else "3f9c0d0e-0000-4000-8000-000000000001",
                "posted_at": "2026-10-13T09:31:22+00:00",
                "postings": [
                    {
                        "account_number": "SYN-0000000001-1",
                        "direction": "DEBIT",
                        "amount": f"{i}0.00",
                        "currency": "GBP",
                    },
                    {
                        "account_number": "SYN-0000000002-2",
                        "direction": "CREDIT",
                        "amount": f"{i}0.00",
                        "currency": "GBP",
                    },
                ],
            }
        )
    path = tmp_path / "corebank.jsonl"
    path.write_text("\n".join(json.dumps(e) for e in entries) + "\n")
    result = PipelineOrchestrator(settings).run(
        path,
        source_name="COREBANK_LEDGER_DEMO",
        adapter=get_adapter("corebank-ledger"),
        build_warehouse=False,
    )
    assert result.status is BatchStatus.COMPLETED and result.counts.accepted_records == 5
    curated = _curated(settings, result.batch_id)
    assert curated["source_system"].unique().to_list() == ["COREBANK_LEDGER_DEMO"]
    assert curated.filter(pl.col("is_reversal")).height == 1
    assert (
        result.reconciliation is not None
        and result.reconciliation["currency_totals"]["GBP"]["curated"] == "150.00"
    )
    summary = PipelineOrchestrator(settings).validate_only(
        path, adapter=get_adapter("corebank-ledger")
    )
    assert summary.counts.accepted_records == 5
