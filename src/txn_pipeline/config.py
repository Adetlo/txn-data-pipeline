"""Runtime configuration (environment variables / ``.env``), see ``.env.example``.

The configuration hash recorded on every batch covers the *behavioural* settings only;
connection strings are excluded so credentials never leak into audit metadata.
"""

from __future__ import annotations

import hashlib
import json
from datetime import date
from pathlib import Path
from typing import Literal

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from txn_pipeline.contracts.enums import Actor

DEFAULT_SQLITE_URL = "sqlite:///./data/metadata/pipeline.db"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore", frozen=True
    )

    # --- storage -------------------------------------------------------------------------
    metadata_database_url: str = Field(
        default=DEFAULT_SQLITE_URL,
        description="SQLAlchemy URL for pipeline metadata. PostgreSQL is the target; "
        "SQLite is supported for zero-infrastructure local runs (ADR 0004).",
    )
    data_root: Path = Field(default=Path("./data"))
    warehouse_path: Path | None = Field(
        default=None,
        description="DuckDB file; defaults to <data_root>/warehouse/txn_warehouse.duckdb",
    )

    # --- behaviour -----------------------------------------------------------------------
    source_timezone: str | None = Field(
        default=None,
        description="IANA zone applied to timestamps without an offset. None = reject them.",
    )
    min_booking_date: date = Field(default=date(2000, 1, 1))
    max_booking_days_in_future: int = Field(default=1, ge=0)
    value_date_tolerance_days: int = Field(default=5, ge=0)
    timestamp_booking_tolerance_days: int = Field(default=1, ge=0)
    fail_on_warnings: bool = Field(default=False)
    build_warehouse_on_run: bool = Field(default=True)
    actor: Actor = Field(default=Actor.CLI)

    # --- observability -------------------------------------------------------------------
    log_level: str = Field(default="INFO")
    log_format: Literal["json", "console"] = Field(default="console")

    @field_validator("log_level")
    @classmethod
    def _upper(cls, value: str) -> str:
        return value.upper()

    # --- derived paths -------------------------------------------------------------------
    @property
    def curated_root(self) -> Path:
        return self.data_root / "curated"

    @property
    def quarantine_root(self) -> Path:
        return self.data_root / "quarantine"

    @property
    def reports_root(self) -> Path:
        return self.data_root / "reports"

    @property
    def incoming_root(self) -> Path:
        return self.data_root / "incoming"

    @property
    def metadata_root(self) -> Path:
        return self.data_root / "metadata"

    @property
    def duckdb_path(self) -> Path:
        return self.warehouse_path or (self.data_root / "warehouse" / "txn_warehouse.duckdb")

    @property
    def uses_sqlite(self) -> bool:
        return self.metadata_database_url.startswith("sqlite")

    def behavioural_config(self) -> dict[str, object]:
        """Settings that influence results (hashed into batch metadata)."""
        return {
            "source_timezone": self.source_timezone,
            "min_booking_date": self.min_booking_date.isoformat(),
            "max_booking_days_in_future": self.max_booking_days_in_future,
            "value_date_tolerance_days": self.value_date_tolerance_days,
            "timestamp_booking_tolerance_days": self.timestamp_booking_tolerance_days,
            "fail_on_warnings": self.fail_on_warnings,
        }

    def config_hash(self) -> str:
        payload = json.dumps(self.behavioural_config(), sort_keys=True)
        return hashlib.sha256(payload.encode()).hexdigest()

    def ensure_directories(self) -> None:
        for path in (
            self.curated_root,
            self.quarantine_root,
            self.reports_root,
            self.incoming_root,
            self.metadata_root,
            self.duckdb_path.parent,
        ):
            path.mkdir(parents=True, exist_ok=True)


def load_settings(**overrides: object) -> Settings:
    return Settings(**overrides)  # type: ignore[arg-type]
