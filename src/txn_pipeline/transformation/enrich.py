"""Safe synthetic enrichment, expressed as Polars expressions over the curated frame.

Nothing here calls an external service. The rules are documented in docs/DATA_CONTRACT.md.
"""

from __future__ import annotations

import polars as pl

from txn_pipeline.contracts.countries import COUNTRY_REGION, REGION_OTHER
from txn_pipeline.contracts.enums import INBOUND_TYPES, AmountBand, Direction, TransactionType

#: Amount bands are evaluated on the *major-unit* amount regardless of currency. This is a
#: deliberate simplification for a synthetic dataset (documented, not a business claim).
AMOUNT_BAND_UPPER_BOUNDS: tuple[tuple[int, AmountBand], ...] = (
    (10, AmountBand.MICRO),
    (100, AmountBand.SMALL),
    (1_000, AmountBand.MEDIUM),
)

#: Counterparty identifiers with this prefix are accounts of the same (synthetic) institution.
INTERNAL_COUNTERPARTY_PREFIX = "ACC-"


def amount_band_expr(amount_col: str = "amount") -> pl.Expr:
    amount = pl.col(amount_col).cast(pl.Float64)  # banding only: never used for arithmetic
    expr: pl.Expr = pl.lit(AmountBand.LARGE.value)
    for upper, band in reversed(AMOUNT_BAND_UPPER_BOUNDS):
        expr = pl.when(amount < upper).then(pl.lit(band.value)).otherwise(expr)
    return expr.alias("amount_band")


def direction_expr() -> pl.Expr:
    inbound = pl.col("transaction_type").is_in([t.value for t in INBOUND_TYPES])
    internal_transfer = (pl.col("transaction_type") == TransactionType.TRANSFER.value) & (
        pl.col("counterparty_id")
        .cast(pl.String)
        .str.starts_with(INTERNAL_COUNTERPARTY_PREFIX)
        .fill_null(False)
    )
    return (
        pl.when(inbound)
        .then(pl.lit(Direction.INBOUND.value))
        .when(internal_transfer)
        .then(pl.lit(Direction.INTERNAL.value))
        .otherwise(pl.lit(Direction.OUTBOUND.value))
        .alias("direction")
    )


def date_part_exprs(date_col: str = "booking_date") -> list[pl.Expr]:
    d = pl.col(date_col)
    weekday = d.dt.weekday()  # 1 = Monday ... 7 = Sunday (ISO)
    return [
        d.dt.year().cast(pl.Int32).alias("booking_year"),
        d.dt.month().cast(pl.Int8).alias("booking_month"),
        d.dt.day().cast(pl.Int8).alias("booking_day"),
        weekday.cast(pl.Int8).alias("booking_weekday"),
        (weekday >= 6).alias("is_weekend"),
    ]


def region_expr(country_col: str = "country") -> pl.Expr:
    return (
        pl.col(country_col)
        .replace_strict(COUNTRY_REGION, default=REGION_OTHER, return_dtype=pl.String)
        .alias("country_region")
    )


def enrich(frame: pl.DataFrame) -> pl.DataFrame:
    """Add all enrichment columns. Total function: cannot fail per row."""
    return frame.with_columns(
        amount_band_expr(), direction_expr(), *date_part_exprs(), region_expr()
    )
