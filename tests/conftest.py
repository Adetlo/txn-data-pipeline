"""Shared fixtures.

* Unit / integration tests run against SQLite by default (zero infrastructure).
* Tests marked ``postgres`` run against a real PostgreSQL: ``TEST_DATABASE_URL`` if set
  (CI service container), otherwise an embedded server from the ``pgserver`` wheel.

The schema is always created by running the Alembic migrations.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest
from hypothesis import settings as hsettings
from sqlalchemy import text

from txn_pipeline.config import Settings
from txn_pipeline.contracts.enums import Actor, SourceFormat
from txn_pipeline.metadata.database import build_engine, run_migrations
from txn_pipeline.observability.logging import configure_logging
from txn_pipeline.synthetic.generator import GeneratorConfig, generate, write_rows

hsettings.register_profile("default", deadline=None, max_examples=50)
hsettings.load_profile("default")

TABLES = (
    "transaction_registry",
    "lineage_records",
    "quarantine_records",
    "quality_results",
    "pipeline_checkpoints",
    "pipeline_batches",
    "source_files",
)


@pytest.fixture(scope="session", autouse=True)
def _quiet_logging() -> None:
    configure_logging("WARNING", "json")


# --- databases -------------------------------------------------------------------------
def _postgres_url(tmp_path_factory: pytest.TempPathFactory) -> tuple[str, Any]:
    url = os.environ.get("TEST_DATABASE_URL")
    if url:
        return url, None
    pgserver = pytest.importorskip(
        "pgserver", reason="pgserver not installed and no TEST_DATABASE_URL"
    )
    datadir = tmp_path_factory.mktemp("pgdata")
    server = pgserver.get_server(str(datadir))
    uri = server.get_uri()
    return uri.replace("postgresql://", "postgresql+psycopg://", 1), server


@pytest.fixture(scope="session")
def database_url(tmp_path_factory: pytest.TempPathFactory) -> Iterator[str]:
    """PostgreSQL URL with migrations applied once per session."""
    url, server = _postgres_url(tmp_path_factory)
    run_migrations(url)
    try:
        yield url
    finally:
        if server is not None:
            server.cleanup()
            logging.getLogger("pgserver").disabled = True


@pytest.fixture
def postgres_url(database_url: str) -> Iterator[str]:
    """Function-scoped PostgreSQL URL: tables are truncated after each test."""
    yield database_url
    engine = build_engine(database_url)
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE {', '.join(TABLES)} CASCADE"))
    engine.dispose()


@pytest.fixture
def sqlite_url(tmp_path: Path) -> str:
    return f"sqlite:///{tmp_path / 'metadata' / 'pipeline.db'}"


# --- settings -------------------------------------------------------------------------
MakeSettings = Callable[..., Settings]


@pytest.fixture
def make_settings(tmp_path: Path) -> MakeSettings:
    def factory(url: str, **overrides: Any) -> Settings:
        run_migrations(url)
        base: dict[str, Any] = {
            "metadata_database_url": url,
            "data_root": tmp_path / "data",
            "log_format": "json",
            "log_level": "WARNING",
            "actor": Actor.TEST,
        }
        base.update(overrides)
        settings = Settings(**base)
        settings.ensure_directories()
        return settings

    return factory


@pytest.fixture
def settings(make_settings: MakeSettings, sqlite_url: str) -> Settings:
    return make_settings(sqlite_url)


# --- synthetic data ---------------------------------------------------------------------
@pytest.fixture
def synthetic_file(tmp_path: Path) -> Callable[..., Path]:
    def factory(
        records: int = 2000,
        seed: int = 42,
        fmt: SourceFormat = SourceFormat.CSV,
        name: str | None = None,
        **cfg: Any,
    ) -> Path:
        rows, _ = generate(GeneratorConfig(records=records, seed=seed, **cfg))
        target = tmp_path / "incoming" / (name or f"synthetic_{seed}_{records}.{fmt.value}")
        write_rows(rows, target, fmt)
        return target

    return factory


def valid_row(**overrides: Any) -> dict[str, Any]:
    """A canonical, valid contract input (all strings as a CSV reader would deliver)."""
    row = {
        "transaction_id": "TXN-SYN-20261013-000001",
        "source_system": "SYNTHETIC_CORE",
        "account_id": "ACC-SYN-000001",
        "counterparty_id": "CP-SYN-000203",
        "transaction_type": "TRANSFER",
        "amount": "250.00",
        "currency": "GBP",
        "booking_date": "2026-10-13",
        "value_date": "2026-10-13",
        "transaction_timestamp": "2026-10-13T09:31:22Z",
        "channel": "ONLINE",
        "country": "GB",
        "reference": "DEMO-TRANSFER-001",
        "status": "POSTED",
        "merchant_category": "",
        "beneficiary_country": "",
        "is_reversal": "false",
        "original_transaction_id": "",
        "schema_version": "financial-transaction/1",
    }
    row.update(overrides)
    return row
