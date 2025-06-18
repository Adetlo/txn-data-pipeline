from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from tests.conftest import valid_row
from txn_pipeline.contracts.enums import Currency
from txn_pipeline.contracts.transaction import TransactionRecord
from txn_pipeline.ingestion.hashing import sha256_record
from txn_pipeline.reconciliation.batch import (
    currency_totals_from_frame,
    currency_totals_from_records,
    reconcile,
)
from txn_pipeline.reconciliation.controls import (
    CurrencyControl,
    ReconciliationReport,
    RowCountControl,
)
from txn_pipeline.reconciliation.reports import load_report, render_text, write_report
from txn_pipeline.transformation.frame import build_curated_frame
from txn_pipeline.validation.duplicates import deduplicate
from txn_pipeline.validation.results import ValidatedRecord


def _items() -> list[ValidatedRecord]:
    rows = [
        valid_row(transaction_id="T1", amount="10.50", currency="GBP"),
        valid_row(transaction_id="T2", amount="0.50", currency="gbp"),
        valid_row(transaction_id="T3", amount="1500", currency="JPY"),
        valid_row(transaction_id="T4", amount="99.99", currency="USD"),
    ]
    return [
        ValidatedRecord(i, r, sha256_record(r), TransactionRecord.model_validate(r))
        for i, r in enumerate(rows, 1)
    ]


def _frame(items: list[ValidatedRecord]) -> pl.DataFrame:
    hashes = deduplicate(items).payload_hashes
    return build_curated_frame(
        items,
        hashes,
        batch_id="b",
        source_filename="f.csv",
        source_file_hash="a" * 64,
        ingested_at=datetime(2026, 10, 13, tzinfo=UTC),
        pipeline_version="0.1.0",
    )


def test_currency_totals_never_mix_currencies() -> None:
    items = _items()
    assert currency_totals_from_records(items) == {
        Currency.GBP: (1100, 2),
        Currency.JPY: (1500, 1),
        Currency.USD: (9999, 1),
    }
    assert currency_totals_from_frame(_frame(items)) == currency_totals_from_records(items)
    assert currency_totals_from_frame(_frame([])) == {}


def test_balanced_report() -> None:
    items = _items()
    report = reconcile(
        batch_id="b",
        source_records=7,
        accepted=items,
        rejected_count=2,
        conflict_count=1,
        exact_duplicates=1,
        idempotent_duplicates=0,
        cross_batch_duplicates=0,
        curated=_frame(items),
        rule_counts={"DQ003_INVALID_AMOUNT": 1},
    )
    assert report.balanced
    assert report.notes == []
    d = report.as_dict()
    assert d["accounted_records"] == 7 and d["difference"] == 0
    assert d["currency_totals"]["GBP"] == {
        "source": "11.00",
        "curated": "11.00",
        "difference": "0.00",
        "source_count": 2,
        "curated_count": 2,
        "balanced": True,
    }
    assert d["currency_totals"]["JPY"]["source"] == "1500"
    assert d["rule_counts"] == {"DQ003_INVALID_AMOUNT": 1}
    assert list(d["currency_totals"]) == ["GBP", "JPY", "USD"]


def test_row_count_mismatch_is_unbalanced() -> None:
    items = _items()
    report = reconcile(
        batch_id="b",
        source_records=10,
        accepted=items,
        rejected_count=0,
        conflict_count=0,
        exact_duplicates=0,
        idempotent_duplicates=0,
        cross_batch_duplicates=0,
        curated=_frame(items),
        rule_counts={},
    )
    assert not report.balanced
    assert report.rows.difference == 6
    assert any("row counts" in n for n in report.notes)


def test_currency_mismatch_is_unbalanced() -> None:
    items = _items()
    frame = _frame(items).filter(pl.col("transaction_id") != "T3")
    report = reconcile(
        batch_id="b",
        source_records=4,
        accepted=items,
        rejected_count=0,
        conflict_count=0,
        exact_duplicates=0,
        idempotent_duplicates=0,
        cross_batch_duplicates=0,
        curated=frame,
        rule_counts={},
    )
    assert not report.balanced
    assert report.unbalanced_currencies == ["JPY"]
    assert report.as_dict()["currency_totals"]["JPY"]["difference"] == "1500"
    assert not report.rows.balanced  # curated != accepted


def test_controls_value_objects() -> None:
    rows = RowCountControl(
        source_records=5,
        accepted_records=3,
        rejected_records=1,
        duplicate_records=1,
        curated_records=3,
    )
    assert rows.balanced and rows.accounted == 5
    ctl = CurrencyControl(Currency.JPY, 1500, 1400, 1, 1)
    assert ctl.difference_minor == 100 and not ctl.balanced
    assert ctl.source_amount() == 1500
    report = ReconciliationReport("b", rows, {Currency.JPY: ctl})
    assert not report.balanced


def test_write_load_render_report(tmp_path: Path) -> None:
    items = _items()
    report = reconcile(
        batch_id="batch-1",
        source_records=4,
        accepted=items,
        rejected_count=0,
        conflict_count=0,
        exact_duplicates=0,
        idempotent_duplicates=0,
        cross_batch_duplicates=0,
        curated=_frame(items),
        rule_counts={},
    )
    path = write_report(report, tmp_path / "reports")
    assert path == tmp_path / "reports" / "batch_id=batch-1" / "reconciliation.json"
    loaded = load_report(path)
    assert loaded == json.loads(json.dumps(report.as_dict()))
    text = render_text(loaded)
    assert "Reconciliation:   PASS" in text
    assert "GBP: source 11.00  curated 11.00  difference 0.00" in text
    loaded["balanced"] = False
    loaded["notes"] = ["something"]
    assert "FAIL" in render_text(loaded) and "! something" in render_text(loaded)
