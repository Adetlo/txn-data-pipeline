"""Metrics, config, rules catalogue, results, logging, lineage, result objects, adapters."""

from __future__ import annotations

import time
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from txn_pipeline.adapters import ADAPTERS, get_adapter
from txn_pipeline.adapters.corebank import adapt_corebank_entry
from txn_pipeline.config import DEFAULT_SQLITE_URL, Settings, load_settings
from txn_pipeline.contracts import rules
from txn_pipeline.contracts.enums import Actor, BatchStatus, Severity, Stage
from txn_pipeline.contracts.transaction import TransactionRecord
from txn_pipeline.observability import logging as events
from txn_pipeline.observability.lineage import LineageEntry, OutputKind
from txn_pipeline.observability.logging import (
    bind_context,
    clear_context,
    configure_logging,
    get_logger,
    unbind_context,
)
from txn_pipeline.observability.metrics import MetricsCollector
from txn_pipeline.pipeline.result import (
    BatchCounts,
    BatchResult,
    ValidationSummary,
    output_paths_as_str,
)
from txn_pipeline.validation.results import Finding, Rejection, RuleCounts


def test_metrics_summary_math() -> None:
    m = MetricsCollector()
    m.set("records_processed", 100)
    m.set("records_accepted", 90)
    m.increment("records_rejected", 7)
    m.increment("records_rejected", 1)
    m.set("records_duplicate", 2)
    with m.timed("X"):
        time.sleep(0.001)
    m.finish()
    s = m.summary()
    assert s["records_processed"] == 100 and s["records_rejected"] == 8
    assert s["quality_pass_rate"] == 0.9
    assert s["records_per_second"] > 0
    assert s["stage_durations_ms"]["X"] > 0
    assert s["duration_seconds"] == m.duration_seconds
    assert MetricsCollector().summary()["quality_pass_rate"] == 1.0


def test_settings_defaults_and_hash(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    a = Settings(
        metadata_database_url="postgresql+psycopg://u:secret@host/db", data_root=tmp_path / "d"
    )
    b = Settings(
        metadata_database_url=DEFAULT_SQLITE_URL, data_root=tmp_path / "e", log_level="debug"
    )
    assert a.config_hash() == b.config_hash()  # URLs and paths excluded
    assert "secret" not in str(a.behavioural_config())
    assert b.log_level == "DEBUG"
    assert (
        Settings(data_root=tmp_path, source_timezone="Europe/London").config_hash()
        != a.config_hash()
    )
    assert a.uses_sqlite is False and b.uses_sqlite is True
    assert b.curated_root == tmp_path / "e" / "curated"
    assert b.duckdb_path == tmp_path / "e" / "warehouse" / "txn_warehouse.duckdb"
    assert (
        Settings(data_root=tmp_path, warehouse_path=tmp_path / "w.duckdb").duckdb_path
        == tmp_path / "w.duckdb"
    )
    b.ensure_directories()
    for p in (
        b.curated_root,
        b.quarantine_root,
        b.reports_root,
        b.incoming_root,
        b.metadata_root,
        b.duckdb_path.parent,
    ):
        assert p.is_dir()
    assert load_settings(data_root=tmp_path, actor=Actor.TEST).actor is Actor.TEST
    with pytest.raises(ValueError, match="max_booking_days_in_future"):
        Settings(data_root=tmp_path, max_booking_days_in_future=-1)


def test_rules_catalogue_is_consistent() -> None:
    assert len(rules.RULES) == 18
    assert all(rid == r.rule_id for rid, r in rules.RULES.items())
    all_rules = set(rules.RULES)
    assert all_rules == rules.ERROR_RULES | rules.WARNING_RULES
    assert not rules.ERROR_RULES & rules.WARNING_RULES
    expected_warnings = {
        rules.DQ005_VALUE_DATE_TOO_FAR_FROM_BOOKING,
        rules.DQ011_TIMESTAMP_BOOKING_MISMATCH,
    }
    assert expected_warnings == rules.WARNING_RULES
    assert rules.rule(rules.DQ006_DUPLICATE_TRANSACTION).stage is Stage.DEDUPLICATION
    assert rules.severity_of(rules.DQ001_MISSING_TRANSACTION_ID) is Severity.ERROR
    for r in rules.RULES.values():
        assert r.description.endswith(".")
    with pytest.raises(KeyError):
        rules.rule("DQ999")


def test_results_value_objects() -> None:
    f1 = Finding("A", Severity.ERROR, "x")
    f2 = Finding("B", Severity.ERROR, "y")
    r = Rejection(1, {}, "A", "A: x", Stage.SCHEMA_VALIDATION, (f1, f2, f1))
    assert r.all_rule_ids == ("A", "B")
    assert Rejection(1, {}, "Z", "", Stage.SCHEMA_VALIDATION).all_rule_ids == ("Z",)
    counts = RuleCounts()
    counts.add("B")
    counts.add("A", 2)
    other = RuleCounts({"A": 1, "C": 1})
    counts.merge(other)
    assert counts.as_dict() == {"A": 3, "B": 1, "C": 1}


def test_logging_configuration(capsys: pytest.CaptureFixture[str]) -> None:
    configure_logging("INFO", "json")
    configure_logging("INFO", "json")  # idempotent
    log = get_logger("test")
    clear_context()
    bind_context(batch_id="b1", stage="X", nothing=None)
    log.info(events.BATCH_REGISTERED, record_count=3)
    unbind_context("stage")
    log.info(events.BATCH_COMPLETED)
    clear_context()
    err = capsys.readouterr().err
    assert (
        '"event": "BATCH_REGISTERED"' in err
        and '"batch_id": "b1"' in err
        and '"record_count": 3' in err
    )
    assert '"nothing"' not in err
    assert err.strip().splitlines()[-1].count('"stage"') == 0
    configure_logging("DEBUG", "console")
    get_logger().debug("console renderer works")
    configure_logging("WARNING", "json")
    assert events.STAGE_FAILED == "STAGE_FAILED"


def test_lineage_entry_as_dict() -> None:
    entry = LineageEntry(
        "b",
        "f.csv",
        "a" * 64,
        OutputKind.CURATED_PARTITION,
        "/x.parquet",
        10,
        "0.1.0",
        datetime(2026, 10, 13, tzinfo=UTC),
    )
    d = entry.as_dict()
    assert d["output_kind"] == "CURATED_PARTITION"
    assert d["created_at"] == "2026-10-13T00:00:00+00:00"
    assert d["row_count"] == 10


def test_batch_result_objects() -> None:
    counts = BatchCounts(
        source_records=200, accepted_records=150, rejected_records=30, duplicate_records=20
    )
    assert counts.quality_pass_rate == 0.75
    assert BatchCounts().quality_pass_rate == 1.0
    result = BatchResult(
        "b", BatchStatus.FAILED, "f.csv", "a" * 64, 10, "csv", "SRC", counts=counts, error="boom"
    )
    assert result.balanced is None and not result.succeeded
    result.reconciliation = {"balanced": True}
    result.status = BatchStatus.PARTIAL
    assert result.balanced is True and result.succeeded
    d = result.as_dict()
    assert (
        d["source"]["sha256"] == "a" * 64
        and d["counts"]["quality_pass_rate"] == 0.75
        and d["error"] == "boom"
    )
    summary = ValidationSummary("f.csv", "a" * 64, counts, {"X": 1}, [])
    assert summary.as_dict()["rule_counts"] == {"X": 1}
    assert output_paths_as_str([Path("/a"), Path("/b")]) == ["/a", "/b"]


def test_corebank_adapter() -> None:
    entry = {
        "id": "4f3d2b1a-0000-4000-8000-000000000001",
        "reference": "TRF-DEMO-0001",
        "booking_date": "2026-10-13",
        "value_date": "2026-10-14",
        "status": "POSTED",
        "currency": "GBP",
        "metadata": {"kind": "transfer", "channel": "MOBILE", "country": "IE"},
        "reversal_of": None,
        "posted_at": "2026-10-13T09:31:22+00:00",
        "postings": [
            {
                "account_number": "SYN-0000000001-1",
                "direction": "DEBIT",
                "amount": "250.00",
                "currency": "GBP",
            },
            {
                "account_number": "SYN-0000000002-2",
                "direction": "CREDIT",
                "amount": "250.00",
                "currency": "GBP",
            },
        ],
    }
    mapped = adapt_corebank_entry(entry)
    record = TransactionRecord.model_validate(mapped)
    assert record.transaction_id == entry["id"]
    assert record.account_id == "SYN-0000000001-1" and record.counterparty_id == "SYN-0000000002-2"
    assert (
        record.transaction_type.value == "TRANSFER"
        and record.channel.value == "MOBILE"
        and record.country == "IE"
    )
    assert record.value_date == date(2026, 10, 14)
    assert record.is_reversal is False

    reversal = {
        **entry,
        "id": "4f3d2b1a-0000-4000-8000-000000000002",
        "reversal_of": entry["id"],
        "metadata": {},
        "value_date": None,
        "posted_at": None,
        "created_at": "2026-10-13T10:00:00Z",
    }
    mapped = adapt_corebank_entry(reversal)
    assert mapped["is_reversal"] is True and mapped["original_transaction_id"] == entry["id"]
    assert (
        mapped["channel"] == "API"
        and mapped["country"] == "GB"
        and mapped["transaction_type"] == "TRANSFER"
    )
    assert (
        mapped["value_date"] == "2026-10-13"
        and mapped["transaction_timestamp"] == "2026-10-13T10:00:00Z"
    )

    assert adapt_corebank_entry({"id": "x", "postings": "junk"})["amount"] is None
    assert (
        adapt_corebank_entry(
            {"id": "x", "postings": [{"account_id": "A", "direction": "credit", "amount": "1"}]}
        )["counterparty_id"]
        == "A"
    )

    assert get_adapter(None) is None
    assert get_adapter("corebank-ledger") is adapt_corebank_entry and "corebank-ledger" in ADAPTERS
    with pytest.raises(KeyError, match="unknown adapter"):
        get_adapter("nope")
