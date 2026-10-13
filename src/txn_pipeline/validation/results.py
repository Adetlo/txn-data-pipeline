"""Value objects shared by the validation stages."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from txn_pipeline.contracts.enums import DuplicateKind, Severity, Stage
from txn_pipeline.contracts.transaction import TransactionRecord


@dataclass(frozen=True, slots=True)
class RawRecord:
    """One source row, exactly as read (values are strings for CSV, typed for Parquet)."""

    record_number: int  # 1-based position in the source file
    data: dict[str, Any]
    raw_hash: str  # SHA-256 of the canonical JSON of ``data``


@dataclass(frozen=True, slots=True)
class Finding:
    rule_id: str
    severity: Severity
    message: str
    field: str | None = None


@dataclass(frozen=True, slots=True)
class Rejection:
    """A record routed to quarantine. ``rule_id`` is the primary (first) failing rule."""

    record_number: int
    raw: dict[str, Any]
    rule_id: str
    reason: str
    stage: Stage
    findings: tuple[Finding, ...] = ()
    transaction_id: str | None = None

    @property
    def all_rule_ids(self) -> tuple[str, ...]:
        return tuple(dict.fromkeys(f.rule_id for f in self.findings)) or (self.rule_id,)


@dataclass(frozen=True, slots=True)
class ValidatedRecord:
    """A record that passed the schema stage; ``warnings`` is filled by the quality stage."""

    record_number: int
    raw: dict[str, Any]
    raw_hash: str
    record: TransactionRecord
    warnings: tuple[Finding, ...] = ()

    @property
    def transaction_id(self) -> str:
        return self.record.transaction_id


@dataclass(frozen=True, slots=True)
class DuplicateRecord:
    """A record dropped as a duplicate (never a conflict: conflicts become Rejections)."""

    record_number: int
    transaction_id: str
    kind: DuplicateKind
    duplicate_of_record_number: int | None  # None for cross-batch duplicates
    duplicate_of_batch_id: str | None


@dataclass(slots=True)
class RuleCounts:
    """Per-rule tallies for the quality_results table and the CLI summary."""

    counts: dict[str, int] = field(default_factory=dict)

    def add(self, rule_id: str, n: int = 1) -> None:
        self.counts[rule_id] = self.counts.get(rule_id, 0) + n

    def merge(self, other: RuleCounts) -> None:
        for rule_id, n in other.counts.items():
            self.add(rule_id, n)

    def as_dict(self) -> dict[str, int]:
        return dict(sorted(self.counts.items()))
