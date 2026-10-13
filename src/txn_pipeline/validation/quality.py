"""Business / data-quality rules evaluated on schema-valid records.

ERROR findings reject the record (quarantine, stage ``QUALITY_VALIDATION``); WARNING
findings are attached to the record and counted, but the record is accepted.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import date, timedelta

from txn_pipeline.contracts import rules
from txn_pipeline.contracts.enums import Severity, Stage
from txn_pipeline.contracts.transaction import TransactionRecord
from txn_pipeline.transformation.timestamps import utc_now
from txn_pipeline.validation.results import Finding, Rejection, RuleCounts, ValidatedRecord


@dataclass(frozen=True, slots=True)
class QualityConfig:
    min_booking_date: date = date(2000, 1, 1)
    max_booking_days_in_future: int = 1
    value_date_tolerance_days: int = 5
    timestamp_booking_tolerance_days: int = 1
    today: date | None = None  # injectable for deterministic tests

    def reference_today(self) -> date:
        return self.today or utc_now().date()


RuleFn = Callable[[TransactionRecord, QualityConfig], Finding | None]


def check_booking_range(record: TransactionRecord, cfg: QualityConfig) -> Finding | None:
    latest = cfg.reference_today() + timedelta(days=cfg.max_booking_days_in_future)
    if record.booking_date < cfg.min_booking_date:
        return Finding(
            rules.DQ004_BOOKING_OUTSIDE_ALLOWED_RANGE,
            Severity.ERROR,
            f"booking_date {record.booking_date} is before {cfg.min_booking_date}",
            "booking_date",
        )
    if record.booking_date > latest:
        return Finding(
            rules.DQ004_BOOKING_OUTSIDE_ALLOWED_RANGE,
            Severity.ERROR,
            f"booking_date {record.booking_date} is after the allowed horizon {latest}",
            "booking_date",
        )
    return None


def check_value_date_distance(record: TransactionRecord, cfg: QualityConfig) -> Finding | None:
    gap = abs((record.value_date - record.booking_date).days)
    if gap > cfg.value_date_tolerance_days:
        return Finding(
            rules.DQ005_VALUE_DATE_TOO_FAR_FROM_BOOKING,
            Severity.WARNING,
            f"value_date is {gap} days from booking_date "
            f"(tolerance {cfg.value_date_tolerance_days})",
            "value_date",
        )
    return None


def check_reversal(record: TransactionRecord, cfg: QualityConfig) -> Finding | None:
    if not record.is_reversal:
        return None
    if not record.original_transaction_id:
        return Finding(
            rules.DQ010_REVERSAL_MISSING_ORIGINAL,
            Severity.ERROR,
            "is_reversal is true but original_transaction_id is missing",
            "original_transaction_id",
        )
    if record.original_transaction_id == record.transaction_id:
        return Finding(
            rules.DQ010_REVERSAL_MISSING_ORIGINAL,
            Severity.ERROR,
            "a reversal cannot reference itself",
            "original_transaction_id",
        )
    return None


def check_timestamp_booking(record: TransactionRecord, cfg: QualityConfig) -> Finding | None:
    gap = abs((record.transaction_timestamp.date() - record.booking_date).days)
    if gap > cfg.timestamp_booking_tolerance_days:
        return Finding(
            rules.DQ011_TIMESTAMP_BOOKING_MISMATCH,
            Severity.WARNING,
            f"UTC timestamp date differs from booking_date by {gap} days",
            "transaction_timestamp",
        )
    return None


QUALITY_RULES: tuple[RuleFn, ...] = (
    check_booking_range,
    check_value_date_distance,
    check_reversal,
    check_timestamp_booking,
)


def evaluate(record: TransactionRecord, cfg: QualityConfig) -> list[Finding]:
    return [f for rule_fn in QUALITY_RULES if (f := rule_fn(record, cfg)) is not None]


def apply_quality_rules(
    records: Iterable[ValidatedRecord], cfg: QualityConfig, *, fail_on_warnings: bool = False
) -> tuple[list[ValidatedRecord], list[Rejection], RuleCounts]:
    """Split records into accepted (possibly with warnings) and rejected."""
    accepted: list[ValidatedRecord] = []
    rejected: list[Rejection] = []
    counts = RuleCounts()
    for item in records:
        findings = evaluate(item.record, cfg)
        for f in findings:
            counts.add(f.rule_id)
        errors = [f for f in findings if f.severity is Severity.ERROR]
        warnings = [f for f in findings if f.severity is Severity.WARNING]
        if errors or (fail_on_warnings and warnings):
            blocking = errors or warnings
            rejected.append(
                Rejection(
                    record_number=item.record_number,
                    raw=item.raw,
                    rule_id=blocking[0].rule_id,
                    reason="; ".join(f"{f.rule_id}: {f.message}" for f in blocking),
                    stage=Stage.QUALITY_VALIDATION,
                    findings=tuple(findings),
                    transaction_id=item.transaction_id,
                )
            )
        else:
            accepted.append(
                ValidatedRecord(
                    record_number=item.record_number,
                    raw=item.raw,
                    raw_hash=item.raw_hash,
                    record=item.record,
                    warnings=tuple(warnings),
                )
            )
    return accepted, rejected, counts
