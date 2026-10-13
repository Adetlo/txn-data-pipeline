"""Reconciliation controls (docs/RECONCILIATION.md).

Row-count invariant::

    source_records == accepted_records + rejected_records + duplicate_records

where ``rejected_records`` includes conflicts (``conflict_records`` is reported separately
as a subset) and ``duplicate_records`` counts exact, idempotent and cross-batch duplicates.

Amount control, per currency (never summed across currencies)::

    sum(amount_minor of accepted records) == sum(amount_minor of curated rows)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from txn_pipeline.contracts.enums import Currency
from txn_pipeline.transformation.currency import format_amount, from_minor_units


@dataclass(frozen=True, slots=True)
class RowCountControl:
    source_records: int
    accepted_records: int
    rejected_records: int
    duplicate_records: int
    curated_records: int
    conflict_records: int = 0
    exact_duplicates: int = 0
    idempotent_duplicates: int = 0
    cross_batch_duplicates: int = 0

    @property
    def accounted(self) -> int:
        return self.accepted_records + self.rejected_records + self.duplicate_records

    @property
    def difference(self) -> int:
        return self.source_records - self.accounted

    @property
    def balanced(self) -> bool:
        return self.difference == 0 and self.curated_records == self.accepted_records

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
            "curated_records": self.curated_records,
            "accounted_records": self.accounted,
            "difference": self.difference,
            "balanced": self.balanced,
        }


@dataclass(frozen=True, slots=True)
class CurrencyControl:
    currency: Currency
    source_minor: int  # accepted records, from the contract
    curated_minor: int  # curated frame, from amount_minor
    source_count: int
    curated_count: int

    @property
    def difference_minor(self) -> int:
        return self.source_minor - self.curated_minor

    @property
    def balanced(self) -> bool:
        return self.difference_minor == 0 and self.source_count == self.curated_count

    def as_dict(self) -> dict[str, Any]:
        return {
            "source": format_amount(
                from_minor_units(self.source_minor, self.currency), self.currency
            ),
            "curated": format_amount(
                from_minor_units(self.curated_minor, self.currency), self.currency
            ),
            "difference": format_amount(
                from_minor_units(self.difference_minor, self.currency), self.currency
            ),
            "source_count": self.source_count,
            "curated_count": self.curated_count,
            "balanced": self.balanced,
        }

    def source_amount(self) -> Decimal:
        return from_minor_units(self.source_minor, self.currency)


@dataclass(slots=True)
class ReconciliationReport:
    batch_id: str
    rows: RowCountControl
    currencies: dict[Currency, CurrencyControl] = field(default_factory=dict)
    rule_counts: dict[str, int] = field(default_factory=dict)
    notes: list[str] = field(default_factory=list)

    @property
    def balanced(self) -> bool:
        return self.rows.balanced and all(c.balanced for c in self.currencies.values())

    @property
    def unbalanced_currencies(self) -> list[str]:
        return [c.value for c, ctl in self.currencies.items() if not ctl.balanced]

    def as_dict(self) -> dict[str, Any]:
        return {
            "batch_id": self.batch_id,
            **self.rows.as_dict(),
            "balanced": self.balanced,
            "currency_totals": {
                c.value: ctl.as_dict() for c, ctl in sorted(self.currencies.items())
            },
            "rule_counts": dict(sorted(self.rule_counts.items())),
            "notes": list(self.notes),
        }
