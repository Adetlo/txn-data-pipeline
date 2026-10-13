"""Engine / session factory and migration helpers."""

from __future__ import annotations

from pathlib import Path

from alembic import command
from alembic.config import Config
from alembic.runtime.migration import MigrationContext
from sqlalchemy import Engine, create_engine, event
from sqlalchemy.orm import Session, sessionmaker

PROJECT_ROOT = Path(__file__).resolve().parents[3]
ALEMBIC_INI = PROJECT_ROOT / "alembic.ini"
MIGRATIONS_DIR = PROJECT_ROOT / "migrations"


def ensure_sqlite_directory(url: str) -> None:
    """SQLite will not create missing parent directories; do it for file-based URLs."""
    if url.startswith("sqlite") and ":memory:" not in url:
        db_path = url.removeprefix("sqlite:///")
        Path(db_path).expanduser().parent.mkdir(parents=True, exist_ok=True)


def build_engine(url: str, *, echo: bool = False) -> Engine:
    if url.startswith("sqlite"):
        ensure_sqlite_directory(url)
        engine = create_engine(url, echo=echo, future=True)

        @event.listens_for(engine, "connect")
        def _sqlite_pragmas(dbapi_connection: object, _record: object) -> None:
            cursor = dbapi_connection.cursor()  # type: ignore[attr-defined]
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.close()

        return engine
    return create_engine(url, echo=echo, future=True, pool_pre_ping=True)


def build_session_factory(engine: Engine) -> sessionmaker[Session]:
    return sessionmaker(bind=engine, expire_on_commit=False, future=True)


def alembic_config(url: str) -> Config:
    cfg = Config(str(ALEMBIC_INI))
    cfg.set_main_option("script_location", str(MIGRATIONS_DIR))
    cfg.set_main_option("sqlalchemy.url", url.replace("%", "%%"))
    return cfg


def run_migrations(url: str, revision: str = "head") -> None:
    """Apply Alembic migrations (used by the CLI, tests and the container entrypoint)."""
    ensure_sqlite_directory(url)
    command.upgrade(alembic_config(url), revision)


def current_revision(url: str) -> str | None:
    engine = build_engine(url)
    with engine.connect() as conn:
        return MigrationContext.configure(conn).get_current_revision()
