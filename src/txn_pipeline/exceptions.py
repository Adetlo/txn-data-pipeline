"""Exception hierarchy for the pipeline.

Record-level problems are *not* exceptions: they are routed to quarantine as
:class:`~txn_pipeline.validation.results.Rejection` objects. Exceptions are reserved
for systemic failures (unreadable file, database down, broken invariant) that must
fail or partially fail the batch.
"""

from __future__ import annotations


class PipelineError(Exception):
    """Base class for all pipeline failures."""


class ConfigurationError(PipelineError):
    """Invalid or incomplete configuration."""


class SourceFileError(PipelineError):
    """The source file cannot be discovered or parsed at all."""


class UnsupportedFormatError(SourceFileError):
    """The source file extension is not one of CSV / JSONL / Parquet."""


class StageError(PipelineError):
    """A pipeline stage failed systemically (not a per-record problem)."""

    def __init__(self, stage: str, message: str) -> None:
        super().__init__(f"{stage}: {message}")
        self.stage = stage


class ReconciliationError(StageError):
    """Reconciliation controls did not balance; the batch must not complete."""

    def __init__(self, message: str) -> None:
        super().__init__("RECONCILIATION", message)


class OutputWriteError(StageError):
    """Curated / quarantine / warehouse output could not be written."""


class MetadataError(PipelineError):
    """Metadata repository failure."""


class BatchStateError(PipelineError):
    """An operation is not valid for the batch's current status."""
