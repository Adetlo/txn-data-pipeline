"""DuckDB star schema builder (docs/WAREHOUSE_MODEL.md).

The warehouse is a *full rebuild* from the curated Parquet files plus batch metadata.
Rebuilding is idempotent by construction: surrogate keys are deterministic hashes or
smart keys, so running the build twice yields the same tables.
"""

from __future__ import annotations

from collections.abc import Sequence
from importlib import resources
from pathlib import Path
from typing import Any

import duckdb
import polars as pl

from txn_pipeline.contracts.enums import (
    CURRENCY_EXPONENT,
    CURRENCY_NAME,
    Channel,
    Currency,
    TransactionType,
)
from txn_pipeline.contracts.schemas import QUARANTINE_POLARS_SCHEMA
from txn_pipeline.exceptions import StageError

FACT_TABLES: tuple[str, ...] = ("fact_transactions", "fact_batch_quality", "fact_quarantine")
DIMENSION_TABLES: tuple[str, ...] = (
    "dim_date",
    "dim_currency",
    "dim_channel",
    "dim_transaction_type",
    "dim_account",
    "dim_counterparty",
)

_SCHEMA_PKG = "txn_pipeline.warehouse.sql.schema"
_QUERY_PKG = "txn_pipeline.warehouse.sql.queries"


def _sql_files(package: str) -> list[tuple[str, str]]:
    root = resources.files(package)
    files = sorted((p for p in root.iterdir() if p.name.endswith(".sql")), key=lambda p: p.name)
    return [(p.name.removesuffix(".sql"), p.read_text(encoding="utf-8")) for p in files]


def _count(con: duckdb.DuckDBPyConnection, table: str) -> int:
    if table not in (*DIMENSION_TABLES, *FACT_TABLES):
        raise KeyError(f"not a warehouse table: {table!r}")
    # table is validated against the fixed allow-list above; DuckDB has no table-name binding.
    row = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()  # noqa: S608  # nosec B608
    return int(row[0]) if row else 0


def available_queries() -> dict[str, str]:
    """Named analytical queries shipped with the package."""
    return dict(_sql_files(_QUERY_PKG))


def _static_sources() -> dict[str, pl.DataFrame]:
    return {
        "src_currency": pl.DataFrame(
            {
                "currency_code": [c.value for c in Currency],
                "currency_name": [CURRENCY_NAME[c] for c in Currency],
                "minor_unit_exponent": [CURRENCY_EXPONENT[c] for c in Currency],
            }
        ),
        "src_channel": pl.DataFrame({"channel_code": [c.value for c in Channel]}),
        "src_transaction_type": pl.DataFrame(
            {"transaction_type_code": [t.value for t in TransactionType]}
        ),
    }


def _batches_frame(batches: Sequence[dict[str, Any]]) -> pl.DataFrame:
    schema: dict[str, pl.DataType] = {
        "batch_id": pl.String(),
        "source_name": pl.String(),
        "source_filename": pl.String(),
        "source_hash": pl.String(),
        "status": pl.String(),
        "actor": pl.String(),
        "pipeline_version": pl.String(),
        "attempt": pl.Int32(),
        "started_at": pl.Datetime("us", "UTC"),
        "completed_at": pl.Datetime("us", "UTC"),
        "record_count": pl.Int64(),
        "accepted_count": pl.Int64(),
        "rejected_count": pl.Int64(),
        "duplicate_count": pl.Int64(),
        "conflict_count": pl.Int64(),
        "warning_count": pl.Int64(),
        "reconciliation_balanced": pl.Boolean(),
    }
    return pl.DataFrame(list(batches), schema=schema, orient="row")


def _quarantine_frame(quarantine_root: Path) -> pl.DataFrame:
    files = (
        sorted(quarantine_root.glob("batch_id=*/rejected.parquet"))
        if quarantine_root.exists()
        else []
    )
    if not files:
        return pl.DataFrame(schema=QUARANTINE_POLARS_SCHEMA)
    return pl.concat([pl.read_parquet(f) for f in files], how="vertical_relaxed")


class WarehouseBuilder:
    def __init__(self, duckdb_path: Path, curated_root: Path, quarantine_root: Path) -> None:
        self.duckdb_path = duckdb_path
        self.curated_root = curated_root
        self.quarantine_root = quarantine_root

    def curated_files(self) -> list[Path]:
        if not self.curated_root.exists():
            return []
        return sorted(self.curated_root.glob("booking_year=*/booking_month=*/*.parquet"))

    def build(self, batches: Sequence[dict[str, Any]]) -> dict[str, int]:
        """Rebuild every table. Returns row counts per table."""
        if not self.curated_files():
            raise StageError("WAREHOUSE", f"no curated Parquet files under {self.curated_root}")
        self.duckdb_path.parent.mkdir(parents=True, exist_ok=True)
        glob = str(self.curated_root / "booking_year=*" / "booking_month=*" / "*.parquet")
        con = duckdb.connect(str(self.duckdb_path))
        try:
            for name, frame in _static_sources().items():
                con.register(name, frame.to_arrow())
            con.register("src_batches", _batches_frame(batches).to_arrow())
            con.register("src_quarantine", _quarantine_frame(self.quarantine_root).to_arrow())
            for _name, sql in _sql_files(_SCHEMA_PKG):
                con.execute(sql.replace("{curated_glob}", glob.replace("'", "''")))
            counts = {table: _count(con, table) for table in (*DIMENSION_TABLES, *FACT_TABLES)}
            # the staging view must not outlive the build: it references an absolute glob
            con.execute("DROP VIEW IF EXISTS stg_curated")
            for name in (
                "src_currency",
                "src_channel",
                "src_transaction_type",
                "src_batches",
                "src_quarantine",
            ):
                con.unregister(name)
            return counts
        finally:
            con.close()

    def run_query(self, name: str, *, limit: int | None = None) -> pl.DataFrame:
        queries = available_queries()
        if name not in queries:
            raise KeyError(f"unknown query {name!r}; available: {sorted(queries)}")
        sql = queries[name].rstrip().rstrip(";")
        con = duckdb.connect(str(self.duckdb_path), read_only=True)
        try:
            if limit is None:
                return con.execute(sql).pl()
            # the inner SQL is a packaged, read-only query file; the limit is a bound parameter
            wrapped = f"SELECT * FROM ({sql}) q LIMIT ?"  # noqa: S608  # nosec B608
            return con.execute(wrapped, [int(limit)]).pl()
        finally:
            con.close()

    def table_counts(self) -> dict[str, int]:
        con = duckdb.connect(str(self.duckdb_path), read_only=True)
        try:
            existing = {row[0] for row in con.execute("SHOW TABLES").fetchall()}
            return {t: _count(con, t) for t in (*DIMENSION_TABLES, *FACT_TABLES) if t in existing}
        finally:
            con.close()
