"""Result objects returned by the orchestrator."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from txn_pipeline.contracts.enums import BatchStatus


@dataclass(slots=True)
class BatchCounts:
    source_records: int = 0
    accepted_records: int = 0
    rejected_records: int = 0
    conflict_records: int = 0
    duplicate_records: int = 0
    exact_duplicates: int = 0
    idempotent_duplicates: int = 0
    cross_batch_duplicates: int = 0
    warning_records: int = 0
    curated_records: int = 0

    @property
    def quality_pass_rate(self) -> float:
        return round(self.accepted_records / self.source_records, 4) if self.source_records else 1.0

    def as_dict(self) -> dict[str, Any]:
        return {
            "source_records": self.source_records,
            "accepted_records": self.accepted_records,
            "rejected_records": self.rejected_records,
            "conflict_records": self.conflict_records,
            "duplicate_records": self.duplicate_records,
            "exact_duplicates": self.exact_duplicates,
            "idempotent_duplicates": self.idempotent_duplicates,
            "cross_batch_duplicates": self.cross_batch_duplicates,
            "warning_records": self.warning_records,
            "curated_records": self.curated_records,
            "quality_pass_rate": self.quality_pass_rate,
        }


@dataclass(slots=True)
class BatchResult:
    batch_id: str
    status: BatchStatus
    source_filename: str
    source_hash: str
    source_size_bytes: int
    source_format: str
    source_name: str
    attempt: int = 1
    correlation_id: str = ""
    skipped: bool = False  # idempotent no-op: file already COMPLETED
    resumed: bool = False
    counts: BatchCounts = field(default_factory=BatchCounts)
    rule_counts: dict[str, int] = field(default_factory=dict)
    reconciliation: dict[str, Any] | None = None
    metrics: dict[str, Any] = field(default_factory=dict)
    curated_files: list[str] = field(default_factory=list)
    quarantine_file: str | None = None
    report_file: str | None = None
    warehouse_tables: dict[str, int] = field(default_factory=dict)
    error: str | None = None
    notes: list[str] = field(default_factory=list)

    @property
    def balanced(self) -> bool | None:
        if self.reconciliation is None:
            return None
        return bool(self.reconciliation.get("balanced"))

    @property
    def succeeded(self) -> bool:
        return self.status in {BatchStatus.COMPLETED, BatchStatus.PARTIAL}

    def as_dict(self) -> dict[str, Any]:
        return {
            "batch_id": self.batch_id,
            "status": self.status.value,
            "source": {
                "name": self.source_name,
                "filename": self.source_filename,
                "sha256": self.source_hash,
                "size_bytes": self.source_size_bytes,
                "format": self.source_format,
            },
            "attempt": self.attempt,
            "correlation_id": self.correlation_id,
            "skipped": self.skipped,
            "resumed": self.resumed,
            "counts": self.counts.as_dict(),
            "rule_counts": dict(sorted(self.rule_counts.items())),
            "reconciliation": self.reconciliation,
            "metrics": self.metrics,
            "outputs": {
                "curated_files": list(self.curated_files),
                "quarantine_file": self.quarantine_file,
                "report_file": self.report_file,
                "warehouse_tables": dict(self.warehouse_tables),
            },
            "error": self.error,
            "notes": list(self.notes),
        }


@dataclass(slots=True)
class ValidationSummary:
    """Result of ``txn-pipeline validate`` (no metadata writes, no outputs)."""

    source_filename: str
    source_hash: str
    counts: BatchCounts
    rule_counts: dict[str, int]
    sample_rejections: list[dict[str, Any]]

    def as_dict(self) -> dict[str, Any]:
        return {
            "source_filename": self.source_filename,
            "source_hash": self.source_hash,
            "counts": self.counts.as_dict(),
            "rule_counts": dict(sorted(self.rule_counts.items())),
            "sample_rejections": self.sample_rejections,
        }


def output_paths_as_str(paths: list[Path]) -> list[str]:
    return [str(p) for p in paths]
