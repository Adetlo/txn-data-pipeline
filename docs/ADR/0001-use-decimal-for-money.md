# ADR 0001: Use Decimal for money

## Status

Accepted (2026-10-13)

## Context

Financial amounts must be exact. Binary floating point cannot represent most decimal
fractions (`0.1 + 0.2 != 0.3`), and a pipeline that reconciles totals to the minor unit
cannot tolerate rounding drift. At the same time the analytical layer (Polars frames,
Parquet, DuckDB) needs a representation that is efficient and sums exactly.

## Decision

* The contract (`TransactionRecord.amount`) and every validation and reconciliation
  step use `decimal.Decimal`. Strings are parsed directly; floats are only accepted
  from typed sources and converted through `repr`, never through binary arithmetic.
* The number of decimal places must not exceed the ISO 4217 exponent of the currency
  (`CURRENCY_EXPONENT`), and the amount is quantised to it.
* The curated frame stores `amount` as Polars `Decimal(18, 4)` (Parquet
  `decimal128(18,4)`) **and** `amount_minor` as `int64` in minor units, with
  `currency_exponent` alongside. The pipeline's own arithmetic (reconciliation totals)
  runs on `amount_minor`; DuckDB sums `amount` exactly as `DECIMAL`.
* The only float conversion is inside the `amount_band` enrichment expression, which
  compares against thresholds and feeds no arithmetic.

## Consequences

* Reconciliation differences are exactly zero when the data is intact, and any non-zero
  difference is a real defect.
* Per-currency handling is mandatory; the code never sums across currencies.
* Two money columns cost a little storage; in exchange both human-readable analytics
  and exact integer accounting are available without conversion at query time.
* Python-level Decimal parsing is slower than vectorised float parsing; at the target
  volume (tens of thousands of rows) this is immaterial.
