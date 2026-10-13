from __future__ import annotations

from datetime import date

from tests.conftest import valid_row
from txn_pipeline.contracts import rules
from txn_pipeline.contracts.enums import Severity, Stage
from txn_pipeline.contracts.transaction import TransactionRecord
from txn_pipeline.ingestion.hashing import sha256_record
from txn_pipeline.validation.business_rules import QUALITY_RULES
from txn_pipeline.validation.quality import QualityConfig, apply_quality_rules, evaluate
from txn_pipeline.validation.results import ValidatedRecord

CFG = QualityConfig(today=date(2026, 10, 13))


def _validated(n: int = 1, **overrides: object) -> ValidatedRecord:
    data = valid_row(**overrides)
    return ValidatedRecord(n, data, sha256_record(data), TransactionRecord.model_validate(data))


def _rule_ids(**overrides: object) -> list[str]:
    return [
        f.rule_id for f in evaluate(TransactionRecord.model_validate(valid_row(**overrides)), CFG)
    ]


def test_clean_record_has_no_findings() -> None:
    assert _rule_ids() == []
    assert len(QUALITY_RULES) == 4


def test_booking_range() -> None:
    assert _rule_ids(
        booking_date="1999-12-31",
        value_date="1999-12-31",
        transaction_timestamp="1999-12-31T10:00:00Z",
    ) == [rules.DQ004_BOOKING_OUTSIDE_ALLOWED_RANGE]
    assert _rule_ids(booking_date="2026-10-14", value_date="2026-10-14") == []  # +1 day allowed
    assert _rule_ids(
        booking_date="2026-10-15",
        value_date="2026-10-15",
        transaction_timestamp="2026-10-15T01:00:00Z",
    ) == [rules.DQ004_BOOKING_OUTSIDE_ALLOWED_RANGE]
    assert (
        _rule_ids(
            booking_date="2000-01-01",
            value_date="2000-01-01",
            transaction_timestamp="2000-01-01T01:00:00Z",
        )
        == []
    )


def test_today_injection_versus_real_clock() -> None:
    record = TransactionRecord.model_validate(valid_row(booking_date="2026-10-13"))
    assert evaluate(record, QualityConfig(today=date(2026, 1, 1)))
    assert QualityConfig().reference_today() >= date(2026, 10, 13)


def test_value_date_distance_is_warning() -> None:
    findings = evaluate(TransactionRecord.model_validate(valid_row(value_date="2026-10-30")), CFG)
    assert [f.rule_id for f in findings] == [rules.DQ005_VALUE_DATE_TOO_FAR_FROM_BOOKING]
    assert findings[0].severity is Severity.WARNING
    assert _rule_ids(value_date="2026-10-18") == []
    assert _rule_ids(value_date="2026-10-08") == []


def test_reversal_rules() -> None:
    assert _rule_ids(is_reversal="true") == [rules.DQ010_REVERSAL_MISSING_ORIGINAL]
    assert _rule_ids(is_reversal="true", original_transaction_id="TXN-SYN-20261013-000001") == [
        rules.DQ010_REVERSAL_MISSING_ORIGINAL
    ]
    assert _rule_ids(is_reversal="true", original_transaction_id="TXN-SYN-0") == []


def test_timestamp_booking_mismatch_is_warning() -> None:
    ids = _rule_ids(transaction_timestamp="2026-10-10T09:00:00Z")
    assert ids == [rules.DQ011_TIMESTAMP_BOOKING_MISMATCH]
    assert _rule_ids(transaction_timestamp="2026-10-12T23:59:59Z") == []
    assert _rule_ids(transaction_timestamp="2026-10-14T00:30:00+01:00") == []


def test_apply_quality_rules_splits_and_counts() -> None:
    items = [
        _validated(1),
        _validated(2, value_date="2026-10-30"),
        _validated(
            3,
            booking_date="1999-12-31",
            value_date="1999-12-31",
            transaction_timestamp="1999-12-31T10:00:00Z",
        ),
        _validated(4, is_reversal="true", value_date="2026-11-30"),
    ]
    accepted, rejected, counts = apply_quality_rules(items, CFG)
    assert [a.record_number for a in accepted] == [1, 2]
    assert [r.record_number for r in rejected] == [3, 4]
    assert accepted[1].warnings[0].rule_id == rules.DQ005_VALUE_DATE_TOO_FAR_FROM_BOOKING
    assert rejected[1].rule_id == rules.DQ010_REVERSAL_MISSING_ORIGINAL
    assert rejected[1].stage is Stage.QUALITY_VALIDATION
    assert set(rejected[1].all_rule_ids) == {
        rules.DQ005_VALUE_DATE_TOO_FAR_FROM_BOOKING,
        rules.DQ010_REVERSAL_MISSING_ORIGINAL,
    }
    assert counts.as_dict() == {
        rules.DQ004_BOOKING_OUTSIDE_ALLOWED_RANGE: 1,
        rules.DQ005_VALUE_DATE_TOO_FAR_FROM_BOOKING: 2,
        rules.DQ010_REVERSAL_MISSING_ORIGINAL: 1,
    }


def test_fail_on_warnings_rejects_warning_records() -> None:
    accepted, rejected, _ = apply_quality_rules(
        [_validated(1, value_date="2026-10-30")], CFG, fail_on_warnings=True
    )
    assert accepted == []
    assert rejected[0].rule_id == rules.DQ005_VALUE_DATE_TOO_FAR_FROM_BOOKING
