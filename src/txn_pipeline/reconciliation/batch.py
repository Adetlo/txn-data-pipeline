"""Compute the reconciliation report for one batch."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

import polars as pl

from txn_pipeline.contracts.enums import Currency
from txn_pipeline.reconciliation.controls import (
    CurrencyControl,
    ReconciliationReport,
    RowCountControl,
)
from txn_pipeline.validation.results import ValidatedRecord


def currency_totals_from_records(
    records: Sequence[ValidatedRecord],
) -> dict[Currency, tuple[int, int]]:
    """(minor-unit total, count) per currency, computed with exact integer arithmetic."""
    totals: dict[Currency, tuple[int, int]] = {}
    for item in records:
        minor, count = totals.get(item.record.currency, (0, 0))
        totals[item.record.currency] = (minor + item.record.amount_minor, count + 1)
    return totals


def currency_totals_from_frame(frame: pl.DataFrame) -> dict[Currency, tuple[int, int]]:
    if frame.is_empty():
        return {}
    agg = frame.group_by("currency").agg(
        pl.col("amount_minor").sum().alias("minor"), pl.len().alias("count")
    )
    return {
        Currency(row["currency"]): (int(row["minor"]), int(row["count"])) for row in agg.to_dicts()
    }


def reconcile(
    *,
    batch_id: str,
    source_records: int,
    accepted: Sequence[ValidatedRecord],
    rejected_count: int,
    conflict_count: int,
    exact_duplicates: int,
    idempotent_duplicates: int,
    cross_batch_duplicates: int,
    curated: pl.DataFrame,
    rule_counts: Mapping[str, int],
) -> ReconciliationReport:
    duplicates = exact_duplicates + idempotent_duplicates + cross_batch_duplicates
    rows = RowCountControl(
        source_records=source_records,
        accepted_records=len(accepted),
        rejected_records=rejected_count,
        duplicate_records=duplicates,
        curated_records=curated.height,
        conflict_records=conflict_count,
        exact_duplicates=exact_duplicates,
        idempotent_duplicates=idempotent_duplicates,
        cross_batch_duplicates=cross_batch_duplicates,
    )
    source_totals = currency_totals_from_records(accepted)
    curated_totals = currency_totals_from_frame(curated)
    currencies: dict[Currency, CurrencyControl] = {}
    for currency in sorted(set(source_totals) | set(curated_totals), key=lambda c: c.value):
        s_minor, s_count = source_totals.get(currency, (0, 0))
        c_minor, c_count = curated_totals.get(currency, (0, 0))
        currencies[currency] = CurrencyControl(
            currency=currency,
            source_minor=s_minor,
            curated_minor=c_minor,
            source_count=s_count,
            curated_count=c_count,
        )
    report = ReconciliationReport(
        batch_id=batch_id, rows=rows, currencies=currencies, rule_counts=dict(rule_counts)
    )
    if not rows.balanced:
        report.notes.append(
            f"row counts do not balance: source={rows.source_records} "
            f"accounted={rows.accounted} curated={rows.curated_records}"
        )
    for code in report.unbalanced_currencies:
        report.notes.append(f"amount totals do not balance for {code}")
    return report
