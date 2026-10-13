"""Curated dataframe schema: the Polars dtypes and the Pandera contract for the analytical layer.

This is the second contract boundary. The Pydantic model guarantees each *record*;
the Pandera schema guarantees the *frame* that is written to Parquet (column set,
dtypes, nullability, value domains). Money appears twice on purpose (ADR 0001):

* ``amount``       decimal(18, 4)   human-readable, DuckDB sums it exactly
* ``amount_minor`` int64            exact integer minor units; the pipeline's own arithmetic
                                    (reconciliation) is done on this column
"""

from __future__ import annotations

import pandera.polars as pa
import polars as pl

from txn_pipeline.contracts.enums import (
    AmountBand,
    Channel,
    Currency,
    Direction,
    TransactionStatus,
    TransactionType,
)
from txn_pipeline.transformation.currency import AMOUNT_PRECISION, AMOUNT_SCALE

AMOUNT_DTYPE = pl.Decimal(AMOUNT_PRECISION, AMOUNT_SCALE)
UTC_TIMESTAMP = pl.Datetime("us", "UTC")

#: Columns in the order they are written to curated Parquet.
CURATED_POLARS_SCHEMA: dict[str, pl.DataType] = {
    # identity & contract
    "transaction_id": pl.String(),
    "schema_version": pl.String(),
    "source_system": pl.String(),
    # business payload
    "account_id": pl.String(),
    "counterparty_id": pl.String(),
    "transaction_type": pl.String(),
    "amount": AMOUNT_DTYPE,
    "amount_minor": pl.Int64(),
    "currency": pl.String(),
    "currency_exponent": pl.Int8(),
    "booking_date": pl.Date(),
    "value_date": pl.Date(),
    "transaction_timestamp": UTC_TIMESTAMP,
    "transaction_timestamp_raw": pl.String(),
    "channel": pl.String(),
    "country": pl.String(),
    "reference": pl.String(),
    "status": pl.String(),
    "merchant_category": pl.String(),
    "beneficiary_country": pl.String(),
    "is_reversal": pl.Boolean(),
    "original_transaction_id": pl.String(),
    # enrichment
    "amount_band": pl.String(),
    "direction": pl.String(),
    "booking_year": pl.Int32(),
    "booking_month": pl.Int8(),
    "booking_day": pl.Int8(),
    "booking_weekday": pl.Int8(),
    "is_weekend": pl.Boolean(),
    "country_region": pl.String(),
    "dq_warning_count": pl.Int16(),
    "dq_warnings": pl.String(),
    # lineage
    "batch_id": pl.String(),
    "source_filename": pl.String(),
    "source_file_hash": pl.String(),
    "source_record_number": pl.Int64(),
    "raw_record_hash": pl.String(),
    "payload_hash": pl.String(),
    "ingested_at": UTC_TIMESTAMP,
    "pipeline_version": pl.String(),
}

CURATED_COLUMNS: tuple[str, ...] = tuple(CURATED_POLARS_SCHEMA)
PARTITION_COLUMNS: tuple[str, ...] = ("booking_year", "booking_month")

_SHA256 = r"^[0-9a-f]{64}$"

CURATED_SCHEMA = pa.DataFrameSchema(
    {
        "transaction_id": pa.Column(pl.String, pa.Check.str_length(1, 64), unique=True),
        "schema_version": pa.Column(pl.String, pa.Check.str_startswith("financial-transaction/")),
        "source_system": pa.Column(pl.String, pa.Check.str_length(1, 64)),
        "account_id": pa.Column(pl.String, pa.Check.str_length(1, 64)),
        "counterparty_id": pa.Column(pl.String, nullable=True),
        "transaction_type": pa.Column(pl.String, pa.Check.isin([t.value for t in TransactionType])),
        "amount": pa.Column(AMOUNT_DTYPE, pa.Check.gt(0)),
        "amount_minor": pa.Column(pl.Int64, pa.Check.gt(0)),
        "currency": pa.Column(pl.String, pa.Check.isin([c.value for c in Currency])),
        "currency_exponent": pa.Column(pl.Int8, pa.Check.isin([0, 2])),
        "booking_date": pa.Column(pl.Date),
        "value_date": pa.Column(pl.Date),
        "transaction_timestamp": pa.Column(UTC_TIMESTAMP),
        "transaction_timestamp_raw": pa.Column(pl.String, nullable=True),
        "channel": pa.Column(pl.String, pa.Check.isin([c.value for c in Channel])),
        "country": pa.Column(pl.String, pa.Check.str_length(2, 2)),
        "reference": pa.Column(pl.String, nullable=True),
        "status": pa.Column(pl.String, pa.Check.isin([s.value for s in TransactionStatus])),
        "merchant_category": pa.Column(pl.String, nullable=True),
        "beneficiary_country": pa.Column(pl.String, nullable=True),
        "is_reversal": pa.Column(pl.Boolean),
        "original_transaction_id": pa.Column(pl.String, nullable=True),
        "amount_band": pa.Column(pl.String, pa.Check.isin([b.value for b in AmountBand])),
        "direction": pa.Column(pl.String, pa.Check.isin([d.value for d in Direction])),
        "booking_year": pa.Column(pl.Int32, pa.Check.in_range(1900, 2200)),
        "booking_month": pa.Column(pl.Int8, pa.Check.in_range(1, 12)),
        "booking_day": pa.Column(pl.Int8, pa.Check.in_range(1, 31)),
        "booking_weekday": pa.Column(pl.Int8, pa.Check.in_range(1, 7)),
        "is_weekend": pa.Column(pl.Boolean),
        "country_region": pa.Column(pl.String),
        "dq_warning_count": pa.Column(pl.Int16, pa.Check.ge(0)),
        "dq_warnings": pa.Column(pl.String, nullable=True),
        "batch_id": pa.Column(pl.String),
        "source_filename": pa.Column(pl.String),
        "source_file_hash": pa.Column(pl.String, pa.Check.str_matches(_SHA256)),
        "source_record_number": pa.Column(pl.Int64, pa.Check.gt(0)),
        "raw_record_hash": pa.Column(pl.String, pa.Check.str_matches(_SHA256)),
        "payload_hash": pa.Column(pl.String, pa.Check.str_matches(_SHA256)),
        "ingested_at": pa.Column(UTC_TIMESTAMP),
        "pipeline_version": pa.Column(pl.String),
    },
    strict=True,
    ordered=True,
    name="curated_transactions",
)

QUARANTINE_POLARS_SCHEMA: dict[str, pl.DataType] = {
    "batch_id": pl.String(),
    "record_number": pl.Int64(),
    "transaction_id": pl.String(),
    "rule_id": pl.String(),
    "all_rule_ids": pl.String(),
    "severity": pl.String(),
    "reason": pl.String(),
    "stage": pl.String(),
    "raw_record": pl.String(),  # canonical JSON of the original row
    "source_filename": pl.String(),
    "source_file_hash": pl.String(),
    "quarantined_at": UTC_TIMESTAMP,
    "pipeline_version": pl.String(),
}
