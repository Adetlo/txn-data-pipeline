"""Structured logging built on structlog.

Every event carries ``timestamp``, ``level``, ``event`` and whatever context is bound
(``batch_id``, ``stage``, ``source``, ``correlation_id``). Full records are never logged
by default; stages log counts and durations only.
"""

from __future__ import annotations

import logging
import sys
from typing import Any

import structlog
from structlog.contextvars import bind_contextvars, clear_contextvars, unbind_contextvars

_CONFIGURED = False


class _StderrLogger:
    """Minimal structlog logger that resolves ``sys.stderr`` on every call.

    structlog's PrintLogger binds the stream once; resolving it lazily keeps logging
    working when the interpreter's stderr is swapped (test capture, daemonisation).
    """

    def msg(self, message: str) -> None:
        print(message, file=sys.stderr)  # noqa: T201 - this *is* the log sink

    log = debug = info = warning = warn = error = critical = exception = fatal = msg


class _StderrLoggerFactory:
    def __call__(self, *args: Any) -> _StderrLogger:
        return _StderrLogger()


def configure_logging(level: str = "INFO", fmt: str = "console") -> None:
    """Configure structlog + stdlib once. Safe to call repeatedly (idempotent)."""
    global _CONFIGURED  # noqa: PLW0603 - module-level guard is the simplest correct option
    renderer: Any
    if fmt == "json":
        renderer = structlog.processors.JSONRenderer(sort_keys=True)
    else:
        renderer = structlog.dev.ConsoleRenderer(colors=sys.stderr.isatty())

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            logging.getLevelNamesMapping().get(level.upper(), logging.INFO)
        ),
        logger_factory=_StderrLoggerFactory(),
        cache_logger_on_first_use=False,
    )
    logging.basicConfig(level=level.upper(), stream=sys.stderr, format="%(message)s")
    _CONFIGURED = True


def get_logger(name: str = "txn_pipeline") -> structlog.stdlib.BoundLogger:
    if not _CONFIGURED:
        configure_logging()
    return structlog.get_logger(name)  # type: ignore[no-any-return]


def bind_context(**values: Any) -> None:
    bind_contextvars(**{k: v for k, v in values.items() if v is not None})


def unbind_context(*keys: str) -> None:
    unbind_contextvars(*keys)


def clear_context() -> None:
    clear_contextvars()


# Canonical event names (docs/OPERATIONS.md lists them).
BATCH_REGISTERED = "BATCH_REGISTERED"
BATCH_RESUMED = "BATCH_RESUMED"
BATCH_SKIPPED_IDEMPOTENT = "BATCH_SKIPPED_IDEMPOTENT"
INGESTION_STARTED = "INGESTION_STARTED"
INGESTION_COMPLETED = "INGESTION_COMPLETED"
SCHEMA_VALIDATION_COMPLETED = "SCHEMA_VALIDATION_COMPLETED"
QUALITY_CHECK_COMPLETED = "QUALITY_CHECK_COMPLETED"
DEDUPLICATION_COMPLETED = "DEDUPLICATION_COMPLETED"
TRANSFORMATION_COMPLETED = "TRANSFORMATION_COMPLETED"
RECONCILIATION_COMPLETED = "RECONCILIATION_COMPLETED"
QUARANTINE_WRITTEN = "QUARANTINE_WRITTEN"
CURATED_WRITTEN = "CURATED_WRITTEN"
REGISTRY_COMMITTED = "REGISTRY_COMMITTED"
WAREHOUSE_BUILT = "WAREHOUSE_BUILT"
BATCH_COMPLETED = "BATCH_COMPLETED"
BATCH_PARTIAL = "BATCH_PARTIAL"
BATCH_FAILED = "BATCH_FAILED"
STAGE_STARTED = "STAGE_STARTED"
STAGE_COMPLETED = "STAGE_COMPLETED"
STAGE_FAILED = "STAGE_FAILED"
