"""Build the curated Polars frame from accepted records and validate it with Pandera."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from datetime import datetime
from typing import Any

import pandera.polars as pa
import polars as pl

from txn_pipeline.contracts.enums import CURRENCY_EXPONENT
from txn_pipeline.contracts.schemas import (
    CURATED_COLUMNS,
    CURATED_POLARS_SCHEMA,
    CURATED_SCHEMA,
)
from txn_pipeline.exceptions import StageError
from txn_pipeline.transformation.enrich import enrich
from txn_pipeline.validation.results import ValidatedRecord


def _row(
    item: ValidatedRecord,
    *,
    batch_id: str,
    source_filename: str,
    source_file_hash: str,
    ingested_at: datetime,
    pipeline_version: str,
    payload_hash: str,
) -> dict[str, Any]:
    r = item.record
    warnings = sorted({w.rule_id for w in item.warnings})
    return {
        "transaction_id": r.transaction_id,
        "schema_version": r.schema_version,
        "source_system": r.source_system,
        "account_id": r.account_id,
        "counterparty_id": r.counterparty_id,
        "transaction_type": r.transaction_type.value,
        "amount": r.amount,
        "amount_minor": r.amount_minor,
        "currency": r.currency.value,
        "currency_exponent": CURRENCY_EXPONENT[r.currency],
        "booking_date": r.booking_date,
        "value_date": r.value_date,
        "transaction_timestamp": r.transaction_timestamp,
        "transaction_timestamp_raw": r.transaction_timestamp_raw,
        "channel": r.channel.value,
        "country": r.country,
        "reference": r.reference,
        "status": r.status.value,
        "merchant_category": r.merchant_category,
        "beneficiary_country": r.beneficiary_country,
        "is_reversal": r.is_reversal,
        "original_transaction_id": r.original_transaction_id,
        "dq_warning_count": len(warnings),
        "dq_warnings": ",".join(warnings) if warnings else None,
        "batch_id": batch_id,
        "source_filename": source_filename,
        "source_file_hash": source_file_hash,
        "source_record_number": item.record_number,
        "raw_record_hash": item.raw_hash,
        "payload_hash": payload_hash,
        "ingested_at": ingested_at,
        "pipeline_version": pipeline_version,
    }


_BASE_SCHEMA: dict[str, pl.DataType] = {
    k: v
    for k, v in CURATED_POLARS_SCHEMA.items()
    if k
    not in {
        "amount_band",
        "direction",
        "booking_year",
        "booking_month",
        "booking_day",
        "booking_weekday",
        "is_weekend",
        "country_region",
    }
}


def build_curated_frame(
    records: Sequence[ValidatedRecord],
    payload_hashes: dict[int, str],
    *,
    batch_id: str,
    source_filename: str,
    source_file_hash: str,
    ingested_at: datetime,
    pipeline_version: str,
) -> pl.DataFrame:
    """Records -> typed, enriched, Pandera-validated frame with the canonical column order."""
    rows: Iterable[dict[str, Any]] = (
        _row(
            item,
            batch_id=batch_id,
            source_filename=source_filename,
            source_file_hash=source_file_hash,
            ingested_at=ingested_at,
            pipeline_version=pipeline_version,
            payload_hash=payload_hashes[item.record_number],
        )
        for item in records
    )
    frame = pl.DataFrame(list(rows), schema=_BASE_SCHEMA, orient="row")
    frame = enrich(frame).select(CURATED_COLUMNS)
    return validate_curated_frame(frame)


def validate_curated_frame(frame: pl.DataFrame) -> pl.DataFrame:
    """Enforce the Pandera contract; a violation is a *systemic* transformation failure."""
    try:
        validated = CURATED_SCHEMA.validate(frame, lazy=True)
    except pa.errors.SchemaErrors as exc:
        failures = exc.failure_cases
        detail = failures.head(5).to_dicts() if hasattr(failures, "to_dicts") else str(failures)
        raise StageError("TRANSFORMATION", f"curated frame violates schema: {detail}") from exc
    return validated


def empty_curated_frame() -> pl.DataFrame:
    return pl.DataFrame(schema=CURATED_POLARS_SCHEMA)
