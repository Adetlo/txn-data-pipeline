"""Canonical transaction contract: normalisation and rule-id classification."""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

import pytest
from pydantic import ValidationError

from tests.conftest import valid_row
from txn_pipeline.contracts import rules
from txn_pipeline.contracts.enums import Channel, Currency, TransactionStatus, TransactionType
from txn_pipeline.contracts.transaction import TransactionRecord, parse_rule_prefix
from txn_pipeline.ingestion.hashing import sha256_record
from txn_pipeline.validation.results import RawRecord
from txn_pipeline.validation.schema import classify_error, validate_record, validate_schema


def _raw(data: dict[str, object], n: int = 1) -> RawRecord:
    return RawRecord(record_number=n, data=data, raw_hash=sha256_record(data))


def _reject(data: dict[str, object]) -> str:
    outcome = validate_record(_raw(data))
    assert not hasattr(outcome, "record"), f"expected rejection, got {outcome}"
    return outcome.rule_id


def test_valid_row_is_normalised() -> None:
    record = TransactionRecord.model_validate(
        valid_row(
            transaction_id=" TXN-SYN-1 ",
            source_system="synthetic_core",
            currency="gbp",
            transaction_type="transfer",
            channel="online",
            country="gb",
            status="posted",
            reference="  DEMO   TRANSFER 001 ",
            amount="250.5",
            transaction_timestamp="2026-10-13T10:31:22+01:00",
        )
    )
    assert record.transaction_id == "TXN-SYN-1"
    assert record.source_system == "SYNTHETIC_CORE"
    assert record.currency is Currency.GBP
    assert record.transaction_type is TransactionType.TRANSFER
    assert record.channel is Channel.ONLINE
    assert record.status is TransactionStatus.POSTED
    assert record.country == "GB"
    assert record.reference == "DEMO TRANSFER 001"
    assert record.amount == Decimal("250.50")
    assert record.amount_minor == 25050
    assert record.transaction_timestamp == datetime(2026, 10, 13, 9, 31, 22, tzinfo=UTC)
    assert record.transaction_timestamp_raw == "2026-10-13T10:31:22+01:00"
    assert record.counterparty_id == "CP-SYN-000203"
    assert record.merchant_category is None
    assert record.is_reversal is False


def test_record_is_frozen() -> None:
    record = TransactionRecord.model_validate(valid_row())
    with pytest.raises(ValidationError):
        record.amount = Decimal("1")  # type: ignore[misc]


@pytest.mark.parametrize(
    ("overrides", "rule_id"),
    [
        ({"transaction_id": ""}, rules.DQ001_MISSING_TRANSACTION_ID),
        ({"transaction_id": "TXN 1"}, rules.DQ001_MISSING_TRANSACTION_ID),
        ({"transaction_id": "X" * 65}, rules.DQ001_MISSING_TRANSACTION_ID),
        ({"currency": "GBX"}, rules.DQ002_INVALID_CURRENCY),
        ({"currency": ""}, rules.DQ002_INVALID_CURRENCY),
        ({"amount": "abc"}, rules.DQ003_INVALID_AMOUNT),
        ({"amount": "-5.00"}, rules.DQ003_INVALID_AMOUNT),
        ({"amount": "0"}, rules.DQ003_INVALID_AMOUNT),
        ({"amount": "250.555"}, rules.DQ003_INVALID_AMOUNT),
        ({"amount": ""}, rules.DQ003_INVALID_AMOUNT),
        ({"amount": "Infinity"}, rules.DQ003_INVALID_AMOUNT),
        ({"amount": "100000000000000"}, rules.DQ003_INVALID_AMOUNT),
        ({"amount": "12.5", "currency": "JPY"}, rules.DQ003_INVALID_AMOUNT),
        ({"status": "DONE"}, rules.DQ007_UNKNOWN_STATUS),
        ({"status": ""}, rules.DQ007_UNKNOWN_STATUS),
        ({"country": "UKX"}, rules.DQ008_INVALID_COUNTRY_CODE),
        ({"beneficiary_country": "XX"}, rules.DQ008_INVALID_COUNTRY_CODE),
        ({"transaction_timestamp": "2026-10-13T09:31:22"}, rules.DQ009_INVALID_TIMESTAMP),
        ({"transaction_timestamp": "not-a-time"}, rules.DQ009_INVALID_TIMESTAMP),
        ({"transaction_timestamp": ""}, rules.DQ009_INVALID_TIMESTAMP),
        ({"booking_date": "13/10/2026"}, rules.SCH006_INVALID_DATE_FORMAT),
        ({"value_date": "2026-13-01"}, rules.SCH006_INVALID_DATE_FORMAT),
        ({"transaction_type": "WIRE"}, rules.SCH003_UNKNOWN_TRANSACTION_TYPE),
        ({"channel": "CARRIER_PIGEON"}, rules.SCH004_UNKNOWN_CHANNEL),
        ({"schema_version": "financial-transaction/2"}, rules.SCH005_UNSUPPORTED_SCHEMA_VERSION),
        ({"source_system": ""}, rules.SCH001_MISSING_REQUIRED_FIELD),
        ({"account_id": ""}, rules.SCH001_MISSING_REQUIRED_FIELD),
        ({"account_id": "ACC 1"}, rules.SCH002_INVALID_FIELD_VALUE),
        ({"channel": ""}, rules.SCH001_MISSING_REQUIRED_FIELD),
        ({"country": ""}, rules.SCH001_MISSING_REQUIRED_FIELD),
        ({"is_reversal": "maybe"}, rules.SCH002_INVALID_FIELD_VALUE),
        ({"reference": "x" * 141}, rules.SCH002_INVALID_FIELD_VALUE),
        ({"counterparty_id": "CP 1"}, rules.SCH002_INVALID_FIELD_VALUE),
    ],
)
def test_invalid_values_map_to_rule_ids(overrides: dict[str, str], rule_id: str) -> None:
    assert _reject(valid_row(**overrides)) == rule_id


@pytest.mark.parametrize(
    ("field", "rule_id"),
    [
        ("transaction_id", rules.DQ001_MISSING_TRANSACTION_ID),
        ("currency", rules.DQ002_INVALID_CURRENCY),
        ("amount", rules.DQ003_INVALID_AMOUNT),
        ("status", rules.DQ007_UNKNOWN_STATUS),
        ("country", rules.DQ008_INVALID_COUNTRY_CODE),
        ("transaction_timestamp", rules.DQ009_INVALID_TIMESTAMP),
        ("account_id", rules.SCH001_MISSING_REQUIRED_FIELD),
        ("booking_date", rules.SCH001_MISSING_REQUIRED_FIELD),
        ("transaction_type", rules.SCH001_MISSING_REQUIRED_FIELD),
    ],
)
def test_missing_fields_map_to_rule_ids(field: str, rule_id: str) -> None:
    data = valid_row()
    del data[field]
    assert _reject(data) == rule_id


def test_unparseable_record_rule() -> None:
    outcome = validate_record(RawRecord(1, {"_unparseable_line": "{oops"}, "0" * 64))
    assert outcome.rule_id == rules.DQ001_MISSING_TRANSACTION_ID  # type: ignore[union-attr]
    with pytest.raises(ValidationError) as exc:
        TransactionRecord.model_validate(["not", "a", "dict"])
    assert rules.SCH007_UNPARSEABLE_RECORD in str(exc.value)


def test_naive_timestamp_accepted_with_source_timezone() -> None:
    data = valid_row(transaction_timestamp="2026-10-13T10:31:22")
    assert _reject(data) == rules.DQ009_INVALID_TIMESTAMP
    outcome = validate_record(_raw(data), source_timezone="Europe/London")
    assert outcome.record.transaction_timestamp == datetime(  # type: ignore[union-attr]
        2026, 10, 13, 9, 31, 22, tzinfo=UTC
    )
    assert _reject(data) == rules.DQ009_INVALID_TIMESTAMP


def test_unknown_source_timezone_is_rejected_not_crash() -> None:
    outcome = validate_record(
        _raw(valid_row(transaction_timestamp="2026-10-13T10:31:22")), source_timezone="Mars/Olympus"
    )
    assert outcome.rule_id == rules.DQ009_INVALID_TIMESTAMP  # type: ignore[union-attr]


def test_jpy_scale_and_minor_units() -> None:
    record = TransactionRecord.model_validate(valid_row(amount="1500", currency="jpy"))
    assert record.amount == Decimal("1500")
    assert record.amount_minor == 1500
    record = TransactionRecord.model_validate(valid_row(amount="1500.00", currency="JPY"))
    assert record.amount_minor == 1500


def test_max_amount_boundary() -> None:
    record = TransactionRecord.model_validate(valid_row(amount="99999999999999.00"))
    assert record.amount_minor == 9999999999999900
    assert _reject(valid_row(amount="99999999999999.01")) == rules.DQ003_INVALID_AMOUNT


def test_extra_fields_are_ignored() -> None:
    record = TransactionRecord.model_validate(valid_row(internal_note="ignore me"))
    assert not hasattr(record, "internal_note")


@pytest.mark.parametrize("spelling", ["true", "TRUE", "1", "yes", "Y", "t", True, 1])
def test_is_reversal_spellings_true(spelling: object) -> None:
    record = TransactionRecord.model_validate(
        valid_row(is_reversal=spelling, original_transaction_id="TXN-SYN-0")
    )
    assert record.is_reversal is True
    assert record.original_transaction_id == "TXN-SYN-0"


@pytest.mark.parametrize("spelling", ["false", "0", "no", "N", "f", False, 0, "", None])
def test_is_reversal_spellings_false(spelling: object) -> None:
    assert TransactionRecord.model_validate(valid_row(is_reversal=spelling)).is_reversal is False


def test_typed_values_from_parquet_are_accepted() -> None:
    record = TransactionRecord.model_validate(
        valid_row(
            amount=Decimal("12.3400"),
            booking_date=date(2026, 10, 13),
            value_date=datetime(2026, 10, 14, 8, 0, tzinfo=UTC),
            transaction_timestamp=datetime(2026, 10, 13, 9, 0, tzinfo=UTC),
            is_reversal=False,
            counterparty_id=None,
            reference=None,
        )
    )
    assert record.amount == Decimal("12.34")
    assert record.value_date == date(2026, 10, 14)
    assert record.transaction_timestamp_raw == "2026-10-13T09:00:00+00:00"
    assert record.counterparty_id is None
    assert record.reference is None


def test_float_amount_uses_repr_not_binary_expansion() -> None:
    record = TransactionRecord.model_validate(valid_row(amount=0.1))
    assert record.amount == Decimal("0.10")


def test_null_like_strings_are_missing() -> None:
    record = TransactionRecord.model_validate(valid_row(reference="null", merchant_category="NaN"))
    assert record.reference is None
    assert record.merchant_category is None


def test_merchant_category_normalised() -> None:
    record = TransactionRecord.model_validate(valid_row(merchant_category="  grocery  store "))
    assert record.merchant_category == "GROCERY STORE"


def test_business_payload_and_hash_are_stable() -> None:
    a = TransactionRecord.model_validate(valid_row())
    b = TransactionRecord.model_validate(valid_row(currency="gbp", reference=" DEMO-TRANSFER-001 "))
    c = TransactionRecord.model_validate(valid_row(amount="250.01"))
    assert a.payload_hash() == b.payload_hash()
    assert a.payload_hash() != c.payload_hash()
    payload = a.business_payload()
    assert payload["amount"] == "250.00"
    assert "transaction_timestamp_raw" not in payload
    assert "amount_minor" not in payload


def test_parse_rule_prefix() -> None:
    assert parse_rule_prefix("DQ002_INVALID_CURRENCY: nope") == ("DQ002_INVALID_CURRENCY", "nope")
    assert parse_rule_prefix("plain message") == (None, "plain message")
    assert parse_rule_prefix("NOT_A_RULE: x") == (None, "NOT_A_RULE: x")


def test_classify_error_generic_type_error() -> None:
    finding = classify_error({"loc": ("amount",), "msg": "weird", "type": "value_error"})
    assert finding.rule_id == rules.SCH002_INVALID_FIELD_VALUE
    assert finding.message == "amount: weird"
    finding = classify_error({"loc": (), "msg": "weird", "type": "value_error"})
    assert finding.field is None


def test_validate_schema_collects_all_findings_and_counts() -> None:
    bad = valid_row(currency="ZZZ", status="WHAT")
    valid, rejected, counts = validate_schema([_raw(valid_row(), 1), _raw(bad, 2)])
    assert len(valid) == 1 and len(rejected) == 1
    rejection = rejected[0]
    assert set(rejection.all_rule_ids) == {rules.DQ002_INVALID_CURRENCY, rules.DQ007_UNKNOWN_STATUS}
    assert rejection.transaction_id == bad["transaction_id"]
    assert counts.as_dict() == {rules.DQ002_INVALID_CURRENCY: 1, rules.DQ007_UNKNOWN_STATUS: 1}
