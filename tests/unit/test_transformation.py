from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

import polars as pl
import pytest

from tests.conftest import valid_row
from txn_pipeline.contracts.enums import AmountBand, Direction
from txn_pipeline.contracts.schemas import CURATED_COLUMNS, CURATED_POLARS_SCHEMA
from txn_pipeline.contracts.transaction import TransactionRecord
from txn_pipeline.exceptions import StageError
from txn_pipeline.ingestion.hashing import sha256_record
from txn_pipeline.transformation.enrich import (
    amount_band_expr,
    date_part_exprs,
    direction_expr,
    enrich,
    region_expr,
)
from txn_pipeline.transformation.frame import (
    build_curated_frame,
    empty_curated_frame,
    validate_curated_frame,
)
from txn_pipeline.validation.duplicates import deduplicate
from txn_pipeline.validation.results import Finding, ValidatedRecord


def _items(*rows: dict[str, object]) -> list[ValidatedRecord]:
    return [
        ValidatedRecord(i, r, sha256_record(r), TransactionRecord.model_validate(r))
        for i, r in enumerate(rows, 1)
    ]


def _build(items: list[ValidatedRecord]) -> pl.DataFrame:
    return build_curated_frame(
        items,
        deduplicate(items).payload_hashes,
        batch_id="batch-1",
        source_filename="in.csv",
        source_file_hash="b" * 64,
        ingested_at=datetime(2026, 10, 13, 12, tzinfo=UTC),
        pipeline_version="0.1.0",
    )


def test_amount_band_boundaries() -> None:
    frame = pl.DataFrame(
        {
            "amount": pl.Series(
                [
                    Decimal("9.99"),
                    Decimal("10"),
                    Decimal("99.99"),
                    Decimal("100"),
                    Decimal("999.99"),
                    Decimal("1000"),
                ],
                dtype=pl.Decimal(18, 4),
            )
        }
    )
    bands = frame.select(amount_band_expr())["amount_band"].to_list()
    assert bands == [
        AmountBand.MICRO,
        AmountBand.SMALL,
        AmountBand.SMALL,
        AmountBand.MEDIUM,
        AmountBand.MEDIUM,
        AmountBand.LARGE,
    ]


def test_direction_rules() -> None:
    frame = pl.DataFrame(
        {
            "transaction_type": [
                "DEPOSIT",
                "REFUND",
                "INTEREST",
                "TRANSFER",
                "TRANSFER",
                "TRANSFER",
                "PAYMENT",
                "FEE",
            ],
            "counterparty_id": [
                "CP-1",
                None,
                "ACC-2",
                "ACC-SYN-2",
                "CP-SYN-2",
                None,
                "ACC-1",
                None,
            ],
        }
    )
    assert frame.select(direction_expr())["direction"].to_list() == [
        Direction.INBOUND,
        Direction.INBOUND,
        Direction.INBOUND,
        Direction.INTERNAL,
        Direction.OUTBOUND,
        Direction.OUTBOUND,
        Direction.OUTBOUND,
        Direction.OUTBOUND,
    ]


def test_date_parts_and_weekend() -> None:
    frame = pl.DataFrame(
        {
            "booking_date": [
                date(2026, 10, 12),
                date(2026, 10, 17),
                date(2026, 10, 18),
                date(2024, 2, 29),
            ]
        }
    )
    out = frame.select(date_part_exprs())
    assert out["booking_weekday"].to_list() == [1, 6, 7, 4]
    assert out["is_weekend"].to_list() == [False, True, True, False]
    assert out["booking_year"].to_list() == [2026, 2026, 2026, 2024]
    assert out["booking_month"].to_list() == [10, 10, 10, 2]
    assert out["booking_day"].to_list() == [12, 17, 18, 29]
    assert out.schema["booking_year"] == pl.Int32 and out.schema["booking_month"] == pl.Int8


def test_region_expr() -> None:
    frame = pl.DataFrame({"country": ["GB", "JP", "AQ", "ZZ"]})
    assert frame.select(region_expr())["country_region"].to_list() == [
        "EUROPE",
        "ASIA_PACIFIC",
        "OTHER",
        "OTHER",
    ]


def test_enrich_adds_all_columns() -> None:
    frame = pl.DataFrame(
        {
            "amount": pl.Series([Decimal("5")], dtype=pl.Decimal(18, 4)),
            "transaction_type": ["FEE"],
            "counterparty_id": [None],
            "booking_date": [date(2026, 10, 13)],
            "country": ["SG"],
        }
    )
    out = enrich(frame)
    assert set(out.columns) >= {
        "amount_band",
        "direction",
        "booking_year",
        "booking_month",
        "booking_day",
        "booking_weekday",
        "is_weekend",
        "country_region",
    }


def test_build_curated_frame_schema_and_lineage() -> None:
    items = _items(
        valid_row(transaction_id="T1"),
        valid_row(
            transaction_id="T2",
            amount="1500",
            currency="JPY",
            counterparty_id="",
            transaction_type="FEE",
        ),
    )
    items[0] = ValidatedRecord(
        1,
        items[0].raw,
        items[0].raw_hash,
        items[0].record,
        (Finding("DQ005_VALUE_DATE_TOO_FAR_FROM_BOOKING", "WARNING", "x"),),
    )  # type: ignore[arg-type]
    frame = _build(items)
    assert frame.columns == list(CURATED_COLUMNS)
    assert dict(frame.schema) == CURATED_POLARS_SCHEMA
    assert frame["amount"].to_list() == [Decimal("250.0000"), Decimal("1500.0000")]
    assert frame["amount_minor"].to_list() == [25000, 1500]
    assert frame["currency_exponent"].to_list() == [2, 0]
    assert frame["transaction_timestamp"][0] == datetime(2026, 10, 13, 9, 31, 22, tzinfo=UTC)
    assert frame["transaction_timestamp_raw"][0] == "2026-10-13T09:31:22Z"
    assert frame["batch_id"].unique().to_list() == ["batch-1"]
    assert frame["source_record_number"].to_list() == [1, 2]
    assert frame["raw_record_hash"].to_list() == [items[0].raw_hash, items[1].raw_hash]
    assert frame["payload_hash"][0] == items[0].record.payload_hash()
    assert frame["dq_warning_count"].to_list() == [1, 0]
    assert frame["dq_warnings"].to_list() == ["DQ005_VALUE_DATE_TOO_FAR_FROM_BOOKING", None]
    assert frame["direction"].to_list() == ["OUTBOUND", "OUTBOUND"]
    assert frame["counterparty_id"].to_list() == ["CP-SYN-000203", None]
    assert frame["pipeline_version"][0] == "0.1.0"


def test_build_empty_frame() -> None:
    frame = _build([])
    assert frame.height == 0
    assert frame.columns == list(CURATED_COLUMNS)
    assert dict(empty_curated_frame().schema) == CURATED_POLARS_SCHEMA


def test_pandera_violation_is_stage_error() -> None:
    frame = _build(_items(valid_row(transaction_id="T1"), valid_row(transaction_id="T2")))
    broken = frame.with_columns(pl.lit("T1").alias("transaction_id"))
    with pytest.raises(StageError, match="curated frame violates schema"):
        validate_curated_frame(broken)
    broken = frame.with_columns(pl.lit(-1).cast(pl.Int64).alias("amount_minor"))
    with pytest.raises(StageError):
        validate_curated_frame(broken)
    with pytest.raises(StageError):
        validate_curated_frame(frame.drop("payload_hash"))
