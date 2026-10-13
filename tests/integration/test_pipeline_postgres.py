"""PostgreSQL-backed integration tests (service container in CI, pgserver locally)."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import pytest
from alembic import command
from sqlalchemy import inspect, select, text

from tests.conftest import TABLES, MakeSettings
from txn_pipeline.contracts.enums import BatchStatus
from txn_pipeline.metadata.database import alembic_config, build_engine, current_revision
from txn_pipeline.metadata.models import (
    PipelineBatchModel,
    QuarantineRecordModel,
    TransactionRegistryModel,
)
from txn_pipeline.metadata.repository import MetadataRepository
from txn_pipeline.pipeline.orchestrator import PipelineOrchestrator
from txn_pipeline.warehouse.writer import curated_row_count

pytestmark = pytest.mark.postgres


def test_migrations_create_expected_schema(postgres_url: str) -> None:
    assert current_revision(postgres_url) == "0001"
    engine = build_engine(postgres_url)
    inspector = inspect(engine)
    assert set(TABLES) <= set(inspector.get_table_names())
    columns = {c["name"]: c for c in inspector.get_columns("quarantine_records")}
    assert str(columns["raw_record"]["type"]).upper() == "JSONB"
    assert str(columns["batch_id"]["type"]).upper() == "UUID"
    assert str(columns["quarantined_at"]["type"]).upper().startswith("TIMESTAMP")
    index_names = {i["name"] for i in inspector.get_indexes("pipeline_batches")}
    assert {"ix_pipeline_batches_source_hash", "ix_pipeline_batches_status"} <= index_names
    engine.dispose()


def test_full_pipeline_on_postgres(
    make_settings: MakeSettings, postgres_url: str, synthetic_file: Callable[..., Path]
) -> None:
    settings = make_settings(postgres_url)
    assert not settings.uses_sqlite
    path = synthetic_file(records=800, seed=31)
    result = PipelineOrchestrator(settings).run(path)
    assert result.status is BatchStatus.COMPLETED, result.error
    assert (
        curated_row_count(settings.curated_root, result.batch_id) == result.counts.accepted_records
    )
    engine = build_engine(postgres_url)
    with MetadataRepository(engine) as repo:
        batch = repo.session.scalars(select(PipelineBatchModel)).one()
        assert batch.status == "COMPLETED" and batch.reconciliation_report["balanced"] is True
        assert repo.session.scalar(select(QuarantineRecordModel.raw_record).limit(1)) is not None
        assert repo.registry_count() == result.counts.accepted_records
        with engine.connect() as conn:
            assert (
                conn.execute(
                    text("SELECT raw_record->>'transaction_id' FROM quarantine_records LIMIT 1")
                ).scalar()
                is not None
            )

    # Idempotent skip, then --force re-run exercising the ON CONFLICT upsert path.
    again = PipelineOrchestrator(settings).run(path)
    assert again.skipped
    forced = PipelineOrchestrator(settings).run(path, force=True)
    assert forced.status is BatchStatus.COMPLETED and forced.attempt == 2
    with MetadataRepository(engine) as repo:
        assert repo.registry_count() == result.counts.accepted_records
        rows = list(repo.session.scalars(select(TransactionRegistryModel)))
        assert {str(r.batch_id) for r in rows} == {result.batch_id}
    engine.dispose()


def test_cross_batch_duplicates_on_postgres(
    make_settings: MakeSettings, postgres_url: str, synthetic_file: Callable[..., Path]
) -> None:
    settings = make_settings(postgres_url)
    first_path = synthetic_file(
        records=200, seed=32, name="a.csv", duplicate_rate=0, conflict_rate=0
    )
    lines = first_path.read_text().splitlines()
    second_path = first_path.with_name("b.csv")
    second_path.write_text("\n".join([lines[0], *reversed(lines[1:])]) + "\n")
    first = PipelineOrchestrator(settings).run(first_path, build_warehouse=False)
    second = PipelineOrchestrator(settings).run(second_path, build_warehouse=False)
    assert second.batch_id != first.batch_id
    assert second.counts.cross_batch_duplicates == first.counts.accepted_records
    assert second.counts.accepted_records == 0


@pytest.mark.parametrize("backend", ["sqlite", "postgres"])
def test_alembic_round_trip(backend: str, tmp_path: Path, request: pytest.FixtureRequest) -> None:
    if backend == "sqlite":
        url = f"sqlite:///{tmp_path / 'rt.db'}"
        from txn_pipeline.metadata.database import run_migrations

        run_migrations(url)
    else:
        url = request.getfixturevalue("postgres_url")
    cfg = alembic_config(url)
    assert current_revision(url) == "0001"
    command.downgrade(cfg, "base")
    engine = build_engine(url)
    assert not set(TABLES) & set(inspect(engine).get_table_names())
    engine.dispose()
    command.upgrade(cfg, "head")
    engine = build_engine(url)
    assert set(TABLES) <= set(inspect(engine).get_table_names())
    engine.dispose()
    assert current_revision(url) == "0001"
