# ADR 0002: Parquet as the curated format

## Status

Accepted (2026-10-13)

## Context

The curated output must be typed (dates, UTC timestamps, decimals), compact, readable
by many tools (DuckDB, Polars, PyArrow, Spark if ever needed) and safe to rewrite on
retry. CSV loses types; a database table couples the analytics layer to a server.

## Decision

* Curated data is written as Parquet (zstd, statistics on) with the explicit schema in
  `contracts/schemas.py`, validated by Pandera before writing.
* Layout: `curated/booking_year=YYYY/booking_month=MM/batch_id=<uuid>.parquet`. One
  file per batch per month; the partition columns are also stored in the file.
* Quarantine is Parquet too: `quarantine/batch_id=<uuid>/rejected.parquet`, holding
  the raw row as canonical JSON plus rule ids, stage and reason.
* Writes go to a temporary file followed by an atomic rename, and every write first
  deletes the files previously produced by the same batch id.

## Consequences

* Retrying a batch cannot duplicate or orphan output; deleting a batch's output is a
  glob.
* Partitioning by month keeps file counts low for small datasets while still allowing
  pruning; it is not tuned for billions of rows.
* Readers must not rely on Hive partition inference (`hive_partitioning=false`) because
  the columns are present in the files.
* Parquet is immutable per file; corrections arrive as new batches, never as in-place
  updates.
