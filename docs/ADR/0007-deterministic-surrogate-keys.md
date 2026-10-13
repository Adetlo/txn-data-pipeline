# ADR 0007: Deterministic surrogate keys in the warehouse

## Status

Accepted (2026-10-13)

## Context

The warehouse is rebuilt from scratch on every run (ADR 0003). Sequence-based surrogate
keys would change between rebuilds, breaking saved queries, notebooks and any external
reference to a key, and would require dimension lookups while building facts.

## Decision

* `dim_date` uses the smart key `YYYYMMDD` (integer), computed on the fact side.
* Static vocabulary dimensions (`dim_currency`, `dim_channel`,
  `dim_transaction_type`) use `row_number()` over the sorted code. Codes come from
  the contract enums and are only ever appended.
* Entity dimensions and the fact key use DuckDB's 64-bit `hash()` shifted right by one
  (`CAST(hash(x) >> 1 AS BIGINT)`) so the value is a non-negative signed `BIGINT`.
  `account_key`, `counterparty_key` and `transaction_key` are therefore pure functions
  of the natural key.
* A null counterparty maps to the sentinel key `-1` (`counterparty_id = 'UNKNOWN'`).

## Consequences

* Facts and dimensions can be built independently and in any order; a rebuild yields
  byte-identical keys, which makes the warehouse stage idempotent and testable.
* Hash keys are opaque; analysts join on them but read the natural key column next to
  them.
* A 63-bit hash collision between two different natural keys is theoretically possible
  and ignored at this scale; a production variant would enforce uniqueness on the
  natural key in the dimension and fail the build on collision.
* Adding a code to a static vocabulary in the middle of the sorted order shifts
  ordinal keys; appending does not. Keys are rebuilt with the warehouse anyway.
