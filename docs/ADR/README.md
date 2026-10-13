# Architecture decision records

| ADR | Decision |
|---|---|
| [0001](0001-use-decimal-for-money.md) | Money is `Decimal` at every boundary; the analytical frame carries `decimal(18,4)` plus integer minor units |
| [0002](0002-parquet-as-curated-format.md) | Curated and quarantine outputs are Parquet, partitioned by booking year and month, one file per batch |
| [0003](0003-duckdb-for-local-analytics.md) | DuckDB is the local analytical engine; the star schema is a full rebuild over curated Parquet |
| [0004](0004-postgresql-for-pipeline-metadata.md) | PostgreSQL holds pipeline metadata; SQLite is a supported zero-infrastructure fallback with one dialect-neutral migration |
| [0005](0005-file-hash-idempotency.md) | Batches are content-addressed by SHA-256; cross-batch idempotency uses a transaction registry |
| [0006](0006-quarantine-invalid-records.md) | Invalid records are quarantined with stable rule ids, never dropped or crashing the batch |
| [0007](0007-deterministic-surrogate-keys.md) | Warehouse surrogate keys are deterministic (smart date key, ordinal vocab keys, hash keys) |

Format: Status, Context, Decision, Consequences. New decisions get the next number;
superseded ADRs stay in place with their status updated.
