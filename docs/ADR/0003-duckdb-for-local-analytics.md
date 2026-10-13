# ADR 0003: DuckDB for local analytics

## Status

Accepted (2026-10-13)

## Context

The project needs an analytical SQL engine that reads Parquet directly, supports exact
`DECIMAL` arithmetic, runs embedded with no server, and is fast at the target scale.

## Decision

* DuckDB is the analytical engine. The star schema (`fact_transactions`, six
  dimensions, two operational facts) is built by executing packaged SQL files against
  a staging view over the curated Parquet glob.
* The warehouse is a single DuckDB file under `DATA_ROOT` and is a **full rebuild** on
  every `warehouse-build` or pipeline run.
* Static dimensions come from the contract enums (registered as Arrow tables) so the
  warehouse never invents codes.
* Batch quality facts are loaded from the metadata database through the repository,
  which keeps DuckDB independent of the PostgreSQL / SQLite dialect.

## Consequences

* Analysts can open the file with the DuckDB CLI, Python or any DuckDB client with no
  infrastructure.
* Full rebuild means no incremental logic and no drift between curated Parquet and the
  warehouse; a failed rebuild leaves the batch `PARTIAL` and is repeated with
  `warehouse-build`.
* DuckDB allows one writer at a time; the pipeline and ad-hoc sessions must not write
  concurrently.
* Not a multi-user warehouse; moving to a served engine would keep the same SQL.
