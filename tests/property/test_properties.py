"""Hypothesis property tests for the invariants the pipeline promises."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import polars as pl
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from tests.conftest import MakeSettings, valid_row
from txn_pipeline.contracts.enums import (
    CURRENCY_EXPONENT,
    Channel,
    Currency,
    DuplicateKind,
    TransactionStatus,
    TransactionType,
)
from txn_pipeline.contracts.transaction import BUSINESS_FIELDS, TransactionRecord
from txn_pipeline.ingestion.hashing import sha256_record
from txn_pipeline.pipeline.orchestrator import PipelineOrchestrator
from txn_pipeline.synthetic.generator import COLUMNS, write_csv
from txn_pipeline.transformation.currency import (
    format_amount,
    from_minor_units,
    parse_amount,
    to_minor_units,
    validate_amount,
)
from txn_pipeline.transformation.frame import build_curated_frame
from txn_pipeline.transformation.timestamps import parse_timestamp
from txn_pipeline.validation.duplicates import deduplicate
from txn_pipeline.validation.quality import QualityConfig, apply_quality_rules
from txn_pipeline.validation.results import RawRecord, ValidatedRecord
from txn_pipeline.validation.schema import validate_schema
from txn_pipeline.warehouse.writer import curated_row_count

QUALITY = QualityConfig(today=date(2026, 12, 31))

currencies = st.sampled_from(list(Currency))
identifiers = st.from_regex(r"[A-Z]{3}-SYN-[0-9]{6}", fullmatch=True)
dates = st.dates(min_value=date(2020, 1, 1), max_value=date(2026, 12, 31))
offsets_minutes = st.integers(min_value=-12 * 60, max_value=14 * 60)


@st.composite
def amounts(draw: st.DrawFn, currency: Currency) -> Decimal:
    places = CURRENCY_EXPONENT[currency]
    return draw(
        st.decimals(
            min_value=Decimal("0.01") if places else Decimal(1),
            max_value=Decimal("99999999"),
            places=places,
            allow_nan=False,
            allow_infinity=False,
        )
    )


@st.composite
def valid_rows(draw: st.DrawFn) -> dict[str, str]:
    currency = draw(currencies)
    booking = draw(dates)
    offset = draw(offsets_minutes)
    ts = datetime.combine(
        booking, datetime.min.time(), tzinfo=timezone(timedelta(minutes=offset))
    ) + timedelta(hours=draw(st.integers(0, 23)), minutes=draw(st.integers(0, 59)))
    return valid_row(
        transaction_id=draw(st.from_regex(r"TXN-SYN-[0-9]{8}-[0-9]{6}", fullmatch=True)),
        account_id=draw(identifiers),
        counterparty_id=draw(st.one_of(st.just(""), identifiers)),
        transaction_type=draw(st.sampled_from(list(TransactionType))).value,
        amount=f"{draw(amounts(currency)):f}",
        currency=currency.value,
        booking_date=booking.isoformat(),
        value_date=(booking + timedelta(days=draw(st.integers(0, 3)))).isoformat(),
        transaction_timestamp=ts.isoformat(),
        channel=draw(st.sampled_from(list(Channel))).value,
        country=draw(st.sampled_from(["GB", "US", "DE", "JP", "SG", "AU", "CH"])),
        reference=draw(st.text(alphabet="ABC-123 ", min_size=0, max_size=20)),
        status=draw(st.sampled_from(list(TransactionStatus))).value,
    )


@st.composite
def invalid_rows(draw: st.DrawFn) -> dict[str, str]:
    row = draw(valid_rows())
    kind = draw(
        st.sampled_from(
            ["currency", "amount", "status", "country", "transaction_timestamp", "booking_date"]
        )
    )
    bad = {
        "currency": "XXX",
        "amount": "abc",
        "status": "???",
        "country": "ZZZ",
        "transaction_timestamp": "nope",
        "booking_date": "1990/01/01",
    }
    row[kind] = bad[kind]
    return row


def _raw(rows: list[dict[str, str]]) -> list[RawRecord]:
    return [RawRecord(i, r, sha256_record(r)) for i, r in enumerate(rows, 1)]


def _validated(row: dict[str, str], n: int = 1) -> ValidatedRecord:
    return ValidatedRecord(n, row, sha256_record(row), TransactionRecord.model_validate(row))


# (1) record accounting invariant -------------------------------------------------------------
@given(
    valid=st.lists(valid_rows(), min_size=0, max_size=15),
    invalid=st.lists(invalid_rows(), min_size=0, max_size=6),
    dup_index=st.lists(st.integers(0, 14), max_size=6),
    conflict_index=st.lists(st.integers(0, 14), max_size=3),
)
def test_accounting_invariant(
    valid: list[dict[str, str]],
    invalid: list[dict[str, str]],
    dup_index: list[int],
    conflict_index: list[int],
) -> None:
    rows = [*valid, *invalid]
    for i in dup_index:
        if i < len(valid):
            rows.append(dict(valid[i]))
    for i in conflict_index:
        if i < len(valid):
            twin = dict(valid[i])
            twin["reference"] = twin["reference"] + "-X"
            rows.append(twin)
    schema_valid, rejected, _ = validate_schema(_raw(rows))
    accepted, rejected_q, _ = apply_quality_rules(schema_valid, QUALITY)
    dedup = deduplicate(accepted)
    total_rejected = len(rejected) + len(rejected_q) + len(dedup.conflicts)
    assert len(dedup.accepted) + total_rejected + len(dedup.duplicates) == len(rows)
    assert len({a.transaction_id for a in dedup.accepted}) == len(dedup.accepted)


# (2) reprocessing never increases curated rows ----------------------------------------------
@settings(max_examples=8, suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(
    rows=st.lists(valid_rows(), min_size=1, max_size=12, unique_by=lambda r: r["transaction_id"])
)
def test_reprocessing_same_file_is_idempotent(
    rows: list[dict[str, str]], make_settings: MakeSettings, tmp_path: Path
) -> None:
    db = tmp_path / f"meta_{abs(hash(tuple(r['transaction_id'] for r in rows)))}.db"
    settings = make_settings(f"sqlite:///{db}", data_root=db.with_suffix(""))
    path = settings.incoming_root / "rows.csv"
    write_csv([{c: r.get(c, "") for c in COLUMNS} for r in rows], path)
    first = PipelineOrchestrator(settings).run(path, build_warehouse=False)
    before = curated_row_count(settings.curated_root, first.batch_id)
    assert before == first.counts.accepted_records
    second = PipelineOrchestrator(settings).run(path, build_warehouse=False)
    assert second.skipped
    forced = PipelineOrchestrator(settings).run(path, force=True, build_warehouse=False)
    assert forced.batch_id == first.batch_id
    assert curated_row_count(settings.curated_root, first.batch_id) == before


# (3) cosmetic raw differences -> idempotent duplicate -----------------------------------------
@given(row=valid_rows(), pad=st.text(alphabet=" \t", min_size=1, max_size=3))
def test_same_payload_is_idempotent_duplicate(row: dict[str, str], pad: str) -> None:
    variant = dict(row)
    variant["currency"] = row["currency"].lower()
    variant["status"] = row["status"].capitalize()
    variant["reference"] = pad + row["reference"] + pad
    variant["transaction_id"] = pad + row["transaction_id"]
    result = deduplicate([_validated(row, 1), _validated(variant, 2)])
    assert [a.record_number for a in result.accepted] == [1]
    assert [d.kind for d in result.duplicates] == [DuplicateKind.IDEMPOTENT_DUPLICATE]
    assert result.conflicts == []


# (4) any changed business field -> conflict for every version ---------------------------------
_MUTABLE = [f for f in BUSINESS_FIELDS if f not in {"transaction_id", "schema_version"}]


@given(row=valid_rows(), field=st.sampled_from(_MUTABLE), copies=st.integers(2, 4))
def test_changed_payload_is_conflict(row: dict[str, str], field: str, copies: int) -> None:
    record = TransactionRecord.model_validate(row)
    changed = dict(row)
    replacements = {
        "source_system": "OTHER_CORE",
        "account_id": "ACC-SYN-999999",
        "counterparty_id": "CP-SYN-999999" if not record.counterparty_id else "",
        "transaction_type": next(
            t.value for t in TransactionType if t is not record.transaction_type
        ),
        "amount": f"{(record.amount + Decimal(1)):f}",
        "currency": "JPY" if record.currency is not Currency.JPY else "GBP",
        "booking_date": (record.booking_date + timedelta(days=1)).isoformat(),
        "value_date": (record.value_date + timedelta(days=1)).isoformat(),
        "transaction_timestamp": (record.transaction_timestamp + timedelta(seconds=1)).isoformat(),
        "channel": next(c.value for c in Channel if c is not record.channel),
        "country": "FR" if record.country != "FR" else "DE",
        "reference": (record.reference or "") + "Z",
        "status": next(s.value for s in TransactionStatus if s is not record.status),
        "merchant_category": "FUEL",
        "beneficiary_country": "IE",
        "is_reversal": "true",
        "original_transaction_id": "TXN-SYN-00000000-000000",
    }
    changed[field] = replacements[field]
    if field == "currency":
        changed["amount"] = "100"
    if field == "is_reversal":
        changed["original_transaction_id"] = "TXN-SYN-00000000-000001"
    items = [_validated(row, 1)] + [_validated(changed, n) for n in range(2, copies + 1)]
    result = deduplicate(items)
    assert result.accepted == [] and result.duplicates == []
    assert sorted(c.record_number for c in result.conflicts) == list(range(1, copies + 1))


# (5) offsets representing the same instant normalise identically -----------------------------
@given(
    instant=st.datetimes(min_value=datetime(2020, 1, 1), max_value=datetime(2026, 12, 31)),
    offset=offsets_minutes,
    offset2=offsets_minutes,
)
def test_offsets_normalise_to_same_utc_instant(
    instant: datetime, offset: int, offset2: int
) -> None:
    utc_instant = instant.replace(tzinfo=UTC)
    a = utc_instant.astimezone(timezone(timedelta(minutes=offset))).isoformat()
    b = utc_instant.astimezone(timezone(timedelta(minutes=offset2))).isoformat()
    assert parse_timestamp(a) == parse_timestamp(b) == utc_instant
    assert parse_timestamp(a).tzinfo is UTC


# (6) Decimal round trip has no binary float artefacts ------------------------------------------
@given(currency=currencies, data=st.data())
def test_decimal_round_trip(currency: Currency, data: st.DataObject) -> None:
    amount = data.draw(amounts(currency))
    parsed = validate_amount(parse_amount(f"{amount:f}"), currency)
    minor = to_minor_units(parsed, currency)
    assert from_minor_units(minor, currency) == parsed == amount
    assert Decimal(format_amount(parsed, currency)) == parsed
    assert parse_amount(format_amount(parsed, currency)) == parsed
    assert isinstance(minor, int)
    assert Decimal(minor).scaleb(-CURRENCY_EXPONENT[currency]) == parsed


# (7) raw -> curated preserves transaction identity -------------------------------------------
@given(
    rows=st.lists(valid_rows(), min_size=1, max_size=20, unique_by=lambda r: r["transaction_id"])
)
def test_curated_frame_preserves_identity(rows: list[dict[str, str]]) -> None:
    items = [_validated(r, n) for n, r in enumerate(rows, 1)]
    dedup = deduplicate(items)
    frame = build_curated_frame(
        dedup.accepted,
        dedup.payload_hashes,
        batch_id="b",
        source_filename="f.csv",
        source_file_hash="a" * 64,
        ingested_at=datetime(2026, 10, 13, tzinfo=UTC),
        pipeline_version="0.1.0",
    )
    assert frame.height == len(rows)
    by_id = {r["transaction_id"]: r for r in frame.to_dicts()}
    for item in items:
        out = by_id[item.transaction_id]
        assert out["amount"] == item.record.amount
        assert out["amount_minor"] == item.record.amount_minor
        assert out["currency"] == item.record.currency.value
        assert out["transaction_timestamp"] == item.record.transaction_timestamp
        assert out["source_record_number"] == item.record_number
    assert frame.schema["amount"] == pl.Decimal(18, 4)
