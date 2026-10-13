# Transaction Data Pipeline

A production-style educational data pipeline for ingesting, validating, reconciling and
transforming synthetic financial transactions into analytics-ready datasets.

Built with Python, PostgreSQL, Parquet and DuckDB, the project focuses on data quality,
idempotent processing, reconciliation, lineage, restartability and operational observability.

> **Disclaimer.** This project is an independent educational implementation using synthetic
> financial data and generic banking concepts. It is not affiliated with, derived from, or
> representative of any production banking system.

[![CI](https://github.com/Adetlo/txn-data-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/Adetlo/txn-data-pipeline/actions/workflows/ci.yml)
![Python 3.12](https://img.shields.io/badge/python-3.12-blue)
![License: MIT](https://img.shields.io/badge/license-MIT-green)

**At a glance:** multi-format ingestion (CSV, JSON Lines, Parquet) · versioned schema validation ·
18 data-quality rules · deduplication and conflict detection · idempotent, content-addressed batches ·
quarantine for every rejected row · normalisation to UTC and ISO codes · row-count and per-currency
reconciliation · batch and row-level lineage · checkpointed, restartable stages · partitioned Parquet
curated layer · DuckDB star schema · 237 automated tests at 98.5% coverage.

## Contents

1. [Overview](#1-overview)
2. [Why this project exists](#2-why-this-project-exists)
3. [What it demonstrates](#3-what-it-demonstrates)
4. [Architecture](#4-architecture)
5. [Pipeline flow](#5-pipeline-flow)
6. [Input contract](#6-input-contract)
7. [Data quality](#7-data-quality)
8. [Reconciliation](#8-reconciliation)
9. [Lineage](#9-lineage)
10. [Warehouse model](#10-warehouse-model)
11. [Technology stack](#11-technology-stack)
12. [Repository structure](#12-repository-structure)
13. [Quick start](#13-quick-start)
14. [CLI examples](#14-cli-examples)
15. [Docker](#15-docker)
16. [Demo scenario](#16-demo-scenario)
17. [Testing](#17-testing)
18. [Performance](#18-performance)
19. [Security](#19-security)
20. [Design decisions](#20-design-decisions)
21. [Limitations](#21-limitations)
22. [Roadmap](#22-roadmap)
23. [Disclaimer](#23-disclaimer)

## 1. Overview

`txn-pipeline` takes a file of financial transactions (CSV, JSON Lines or Parquet), registers an
immutable **batch** keyed by the file's SHA-256, validates every row against a versioned data
contract, routes bad rows to a reviewable **quarantine**, de-duplicates and detects conflicting
versions of the same transaction, normalises and enriches the survivors, **reconciles** row counts
and per-currency amount totals, writes partitioned **curated Parquet**, and rebuilds a DuckDB
**star schema** for analytics. Every step is checkpointed in a PostgreSQL (or SQLite) metadata
store with lineage and audit fields, and re-running the same file is a no-op.

### Relationship to corebank-ledger

[corebank-ledger](https://github.com/Adetlo/corebank-ledger) demonstrates transaction-system
integrity and posting controls: a double-entry ledger with idempotent posting, balanced-entry
invariants and reversals. txn-data-pipeline demonstrates the downstream side: data quality,
lineage, reconciliation and analytics preparation for transaction exports.

```
corebank-ledger  ->  synthetic transaction export  ->  txn-data-pipeline  ->  validated / enriched / reconciled data  ->  analytics / ML-ready datasets
```

The projects complement each other but are independently runnable and there is no live integration
between them. This repository ships a generic input contract, sample files and an optional adapter for
exports shaped like the corebank-ledger demo (`--adapter corebank-ledger`, see
[docs/DATA_CONTRACT.md](docs/DATA_CONTRACT.md)).

## 2. Why this project exists

Most "data pipeline" examples are a script that loads a CSV into a table. Real financial data
engineering is about what happens when the data is wrong: rows that do not parse, the same file
delivered twice, two versions of the same transaction with different amounts, a job that dies
halfway through writing output. This project exists to show how those cases are handled
deliberately, with controls that can be audited, in a codebase small enough to read in an
afternoon.

## 3. What it demonstrates

| Area | What is implemented |
|------|---------------------|
| Batch ingestion | CSV, JSON Lines, Parquet readers mapped onto one canonical model; directory and single-file discovery |
| Schema validation | Pydantic v2 contract with normalising validators; every failure maps to a stable rule id; invalid rows never crash the batch |
| Data quality | 18 rule ids (`SCH001`..`SCH007`, `DQ001`..`DQ011`) with ERROR / WARNING severity; per-rule counts persisted per batch |
| Deduplication | Exact, idempotent and cross-batch duplicates dropped; conflicting versions quarantined; registry of curated transaction ids |
| Normalisation | Codes upper-cased, whitespace collapsed, timestamps to UTC (offset required or explicit source timezone), raw timestamp preserved |
| Enrichment | Amount band, money direction, date parts, weekend flag, static country region (no external calls) |
| Reconciliation | `source = accepted + rejected + duplicates`, curated == accepted, on-disk post-write control, exact per-currency totals in minor units |
| Dimensional model | `fact_transactions` + six dimensions + two operational facts in DuckDB, rebuilt idempotently from Parquet |
| Lineage | Batch-level `lineage_records` and row-level lineage columns on every curated row |
| Observability | structlog JSON/console events per stage with batch id, correlation id, counts and durations; in-process metrics summary |
| Idempotency | Content-addressed batches; same file twice = no-op; same filename with new content = new version |
| Restartability | Per-stage checkpoints; FAILED / PARTIAL / stale RUNNING batches resume on re-run; all write stages are idempotent |
| Auditability | Actor, pipeline version, configuration hash, source hash, attempt, timestamps and result summary on every batch |
| Failure handling | Systemic failures mark the batch FAILED; a failed warehouse refresh after committed outputs marks it PARTIAL |
| Reproducible synthetic data | Seeded generator with configurable invalid / duplicate / conflict / warning rates |
| CI/CD and containers | GitHub Actions (quality, PostgreSQL integration, security, Docker smoke demo); multi-stage image; docker compose |

## 4. Architecture

```mermaid
flowchart LR
    A[Source files<br/>CSV / JSONL / Parquet] --> B[Discovery<br/>SHA-256, size, format]
    B --> C[Batch registration]
    C --> D[Ingestion]
    D --> E[Schema validation<br/>Pydantic contract]
    E --> F[Data quality rules]
    F --> G[Deduplication<br/>+ conflict detection]
    G --> H[Normalisation & enrichment<br/>Polars + Pandera]
    H --> I[Reconciliation]
    I --> J[(Curated Parquet<br/>booking_year / booking_month)]
    J --> K[Warehouse model<br/>DuckDB star schema]
    K --> L[Analytical queries]

    E -. rejected .-> Q[(Quarantine Parquet)]
    F -. rejected .-> Q
    G -. conflicts .-> Q

    C --> M[(Metadata DB<br/>PostgreSQL / SQLite)]
    G --> M
    I --> M
    J --> M
    Q --> M
    K --> M
```

Layers (see [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)):

* `contracts/` the canonical record, enums, rule catalogue and the curated frame schema
* `ingestion/` discovery, hashing and format readers
* `validation/` schema stage, quality rules, duplicate and conflict detection
* `transformation/` normalisation helpers, currency and timestamp rules, enrichment, frame build
* `reconciliation/` controls and reports
* `warehouse/` Parquet writers, DuckDB star schema and bundled queries
* `metadata/` SQLAlchemy models, repository, checkpoints, Alembic migrations
* `observability/` structured logging, metrics, lineage
* `pipeline/` stages and the orchestrator (state machine, checkpoints, failure handling)
* `cli/` Typer application

### Batch state machine

```mermaid
stateDiagram-v2
    [*] --> DISCOVERED: source file hashed and registered
    DISCOVERED --> RUNNING: attempt starts
    RUNNING --> VALIDATED: schema, quality and dedupe done, counts committed
    VALIDATED --> COMPLETED: reconciliation balanced, outputs and registry committed
    COMPLETED --> PARTIAL: warehouse refresh failed after commit
    RUNNING --> FAILED: systemic error
    VALIDATED --> FAILED: unbalanced reconciliation or write failure
    FAILED --> RUNNING: re-run same file (new attempt)
    PARTIAL --> RUNNING: re-run same file (new attempt)
```

## 5. Pipeline flow

| Stage | What happens | Persisted |
|-------|--------------|-----------|
| `DISCOVERY` | locate the file, SHA-256, size, format | |
| `REGISTRATION` | find or create the batch for (source, hash); skip if COMPLETED; resume if restartable | `source_files`, `pipeline_batches` |
| `INGESTION` | read all rows as raw records (optionally through an adapter) | checkpoint |
| `SCHEMA_VALIDATION` | Pydantic contract; failures become rejections with rule ids | checkpoint |
| `QUALITY_VALIDATION` | business rules; ERROR rejects, WARNING annotates | checkpoint |
| `DEDUPLICATION` | group by `transaction_id`, classify duplicates / conflicts, consult the registry | `quality_results`, counts, status VALIDATED |
| `TRANSFORMATION` | build the typed Polars frame, enrich, validate with Pandera | checkpoint |
| `RECONCILIATION` | row-count and per-currency amount controls; unbalanced = FAILED, nothing written | checkpoint |
| `QUARANTINE_WRITE` | rejected rows to Parquet and `quarantine_records` | checkpoint |
| `CURATED_WRITE` | partitioned Parquet, atomic replace, post-write row count check | checkpoint |
| `REGISTRY_COMMIT` | upsert `transaction_registry`, write the reconciliation report, lineage rows, status COMPLETED | `transaction_registry`, `lineage_records` |
| `WAREHOUSE` | rebuild the DuckDB star schema (failure here = PARTIAL) | `lineage_records` |
| `COMPLETION` | metrics summary, final status | `pipeline_batches` |

Invalid data is never silently dropped: every rejected row is written to quarantine with its
original payload, rule id, reason, stage and batch id.

## 6. Input contract

Schema version `financial-transaction/1` ([docs/DATA_CONTRACT.md](docs/DATA_CONTRACT.md)).

```json
{
  "transaction_id": "TXN-SYN-20261013-000001",
  "source_system": "SYNTHETIC_CORE",
  "account_id": "ACC-SYN-000001",
  "counterparty_id": "CP-SYN-000203",
  "transaction_type": "TRANSFER",
  "amount": "250.00",
  "currency": "GBP",
  "booking_date": "2026-10-13",
  "value_date": "2026-10-13",
  "transaction_timestamp": "2026-10-13T09:31:22Z",
  "channel": "ONLINE",
  "country": "GB",
  "reference": "DEMO-TRANSFER-001",
  "status": "POSTED"
}
```

Optional fields: `merchant_category`, `beneficiary_country`, `is_reversal`,
`original_transaction_id`, `schema_version`.

Money is `Decimal` at every contract and validation boundary. Amounts must be positive and
carry no more decimals than the currency allows (GBP, USD, EUR, CHF, SGD, AUD: 2; JPY: 0). The
curated frame stores `amount` as `decimal(18,4)` for analytics **and** `amount_minor` as an exact
`int64` in minor units, which is what reconciliation sums. Timestamps must carry an offset (or a
`SOURCE_TIMEZONE` must be configured) and are normalised to UTC; the original text is kept in
`transaction_timestamp_raw`.

## 7. Data quality

Every rule has a stable identifier, a severity and a stage ([docs/DATA_QUALITY.md](docs/DATA_QUALITY.md)).

| Rule | Severity | Meaning |
|------|----------|---------|
| `DQ001_MISSING_TRANSACTION_ID` | ERROR | id missing, blank or malformed |
| `DQ002_INVALID_CURRENCY` | ERROR | not a supported ISO 4217 code |
| `DQ003_INVALID_AMOUNT` | ERROR | unparseable, non-positive, too large or wrong scale |
| `DQ004_BOOKING_OUTSIDE_ALLOWED_RANGE` | ERROR | before the configured minimum or in the future |
| `DQ005_VALUE_DATE_TOO_FAR_FROM_BOOKING` | WARNING | value date beyond the tolerance |
| `DQ006_DUPLICATE_TRANSACTION` | ERROR | conflicting versions of one transaction id |
| `DQ007_UNKNOWN_STATUS` | ERROR | status outside the vocabulary |
| `DQ008_INVALID_COUNTRY_CODE` | ERROR | not ISO 3166-1 alpha-2 |
| `DQ009_INVALID_TIMESTAMP` | ERROR | unparseable or no timezone |
| `DQ010_REVERSAL_MISSING_ORIGINAL` | ERROR | reversal without (or referencing its own) original |
| `DQ011_TIMESTAMP_BOOKING_MISMATCH` | WARNING | UTC timestamp date far from booking date |
| `SCH001`..`SCH007` | ERROR | missing field, bad type, unknown type / channel, unsupported schema version, bad date, unparseable row |

ERROR rejects the record to quarantine; WARNING accepts it, counts the finding per rule and
stores the rule ids on the curated row (`dq_warnings`). Duplicates are handled as follows:
identical rows and same-id-same-payload rows are dropped as duplicates; same id with a
materially different payload is a **conflict** and every version is quarantined, because the
pipeline cannot know which one is right.

## 8. Reconciliation

Each batch produces a structured report ([docs/RECONCILIATION.md](docs/RECONCILIATION.md)):

```json
{
  "batch_id": "a9006f18-b277-4002-88f8-22ccdd5f389e",
  "source_records": 10150,
  "accepted_records": 9750,
  "rejected_records": 301,
  "conflict_records": 101,
  "duplicate_records": 99,
  "exact_duplicates": 66,
  "idempotent_duplicates": 33,
  "cross_batch_duplicates": 0,
  "curated_records": 9750,
  "balanced": true,
  "currency_totals": {
    "GBP": {"source": "433465.40", "curated": "433465.40", "difference": "0.00", "source_count": 4296, "curated_count": 4296, "balanced": true},
    "JPY": {"source": "7701563", "curated": "7701563", "difference": "0", "source_count": 547, "curated_count": 547, "balanced": true}
  },
  "rule_counts": {"DQ003_INVALID_AMOUNT": 63, "DQ006_DUPLICATE_TRANSACTION": 101, "SCH006_INVALID_DATE_FORMAT": 13},
  "notes": []
}
```

The full report for this run is in [examples/reconciliation_report.json](examples/reconciliation_report.json).

Invariant: `source_records = accepted_records + rejected_records + duplicate_records`, where
rejected includes conflicts. Amounts are reconciled per currency on integer minor units and are
never summed across currencies. An unbalanced batch is marked FAILED before any output is written.

## 9. Lineage

Source file -> SHA-256 -> `source_files` -> batch -> raw rows -> quality results -> curated
partitions / quarantine file / report -> warehouse. Every curated row carries `batch_id`,
`source_filename`, `source_file_hash`, `source_record_number`, `raw_record_hash`,
`payload_hash`, `ingested_at` and `pipeline_version`; batch-level artefacts are listed in
`lineage_records`. See [docs/LINEAGE.md](docs/LINEAGE.md).

## 10. Warehouse model

```mermaid
erDiagram
    fact_transactions }o--|| dim_date : booking_date_key
    fact_transactions }o--|| dim_date : value_date_key
    fact_transactions }o--|| dim_currency : currency_key
    fact_transactions }o--|| dim_channel : channel_key
    fact_transactions }o--|| dim_transaction_type : transaction_type_key
    fact_transactions }o--|| dim_account : account_key
    fact_transactions }o--|| dim_counterparty : counterparty_key
```

Grain of `fact_transactions`: one accepted canonical financial transaction. Surrogate keys are
deterministic (smart date keys, ordinal keys for static vocabularies, 63-bit hash keys for
accounts and counterparties) so a full rebuild is idempotent. Two operational facts,
`fact_batch_quality` and `fact_quarantine`, power the quality-trend queries. See
[docs/WAREHOUSE_MODEL.md](docs/WAREHOUSE_MODEL.md).

## 11. Technology stack

| Concern | Choice |
|---------|--------|
| Language | Python 3.12, `uv`, `pyproject.toml` |
| Contract and config | Pydantic v2, pydantic-settings |
| Dataframes | Polars (with Pandera for the curated frame contract), PyArrow / Parquet |
| Analytics | DuckDB (embedded, file-based) |
| Metadata | PostgreSQL via SQLAlchemy 2.x and Alembic; SQLite supported for zero-infrastructure runs |
| CLI and logging | Typer, Rich, structlog |
| Tests | pytest, Hypothesis, pytest-cov, embedded PostgreSQL (`pgserver`) locally, service container in CI |
| Quality and security | ruff, mypy (strict), bandit, pip-audit |
| Packaging | Docker multi-stage image, docker compose, GitHub Actions |

Deliberately not used: Spark, Kafka, Airflow, Kubernetes, dbt (see section 20).

## 12. Repository structure

```
txn-data-pipeline/
├── src/txn_pipeline/
│   ├── adapters/            # corebank-ledger-shaped export adapter
│   ├── cli/                 # Typer application
│   ├── contracts/           # enums, rule catalogue, TransactionRecord, curated schema, countries
│   ├── ingestion/           # discovery, hashing, csv / jsonl / parquet readers
│   ├── metadata/            # SQLAlchemy models, repository, checkpoints, database helpers
│   ├── observability/       # structlog events, metrics, lineage
│   ├── pipeline/            # stages, orchestrator, result objects
│   ├── reconciliation/      # controls, batch reconciliation, reports
│   ├── synthetic/           # seeded generator and writers
│   ├── transformation/      # normalise, currency, timestamps, enrich, frame
│   ├── validation/          # schema stage, quality rules, duplicates, result types
│   ├── warehouse/           # Parquet writers, DuckDB builder, sql/schema, sql/queries
│   ├── config.py
│   └── exceptions.py
├── migrations/              # Alembic (dialect-neutral initial migration)
├── tests/                   # unit / integration / property / e2e
├── data/samples/            # tiny committed synthetic fixtures
├── examples/                # canonical record, corebank-shaped export, CLI cheat sheet
├── scripts/                 # benchmark.py, generate_synthetic_data.py
├── warehouse/               # how to use the DuckDB warehouse directly
├── docs/                    # architecture, contract, quality, reconciliation, lineage, model, operations, performance, security, ADR/
├── docker/entrypoint.sh, Dockerfile, docker-compose.yml
├── .github/workflows/ci.yml
└── pyproject.toml, alembic.ini, .env.example, LICENSE, SECURITY.md, CONTRIBUTING.md
```

## 13. Quick start

Requirements: Python 3.12 and [uv](https://docs.astral.sh/uv/). No database server is needed
for the default SQLite metadata store.

```bash
git clone https://github.com/Adetlo/txn-data-pipeline.git
cd txn-data-pipeline
uv sync
cp .env.example .env            # optional; defaults work out of the box

uv run txn-pipeline demo        # generate 10,000 rows, run everything, query, reprocess
uv run txn-pipeline --help
```

To use PostgreSQL instead of SQLite:

```bash
export METADATA_DATABASE_URL=postgresql+psycopg://txn_pipeline:txn_pipeline_dev_only@localhost:5432/txn_pipeline
uv run txn-pipeline db-upgrade
uv run txn-pipeline run --input data/samples/transactions_sample.csv --source SYNTHETIC_CORE
```

## 14. CLI examples

```bash
txn-pipeline generate-demo-data --records 10000 --seed 42 --invalid-rate 0.02 --duplicate-rate 0.01 --output data/incoming/txns.csv
txn-pipeline validate --input data/incoming/txns.csv --strict          # no batch, no writes; exit 1 if anything would be rejected
txn-pipeline run --input data/incoming/txns.csv --source SYNTHETIC_CORE # exit 0 COMPLETED, 2 PARTIAL, 1 FAILED
txn-pipeline run --input data/incoming/txns.csv --json                 # same file again: idempotent skip
txn-pipeline batches
txn-pipeline batch-status --batch-id <uuid>                            # status, checkpoints, lineage
txn-pipeline reconcile --batch-id <uuid>                               # stored report + on-disk row check
txn-pipeline quarantine --batch-id <uuid> --rule DQ006_DUPLICATE_TRANSACTION --raw
txn-pipeline warehouse-build
txn-pipeline query                                                     # list bundled queries
txn-pipeline query daily_value_by_currency --limit 30
txn-pipeline run --input examples/corebank_ledger_export.jsonl --adapter corebank-ledger --source COREBANK_LEDGER_DEMO
```

Bundled analytical queries: `daily_volume`, `daily_value_by_currency`, `top_transaction_types`,
`channel_distribution`, `weekend_activity`, `rejected_trend`, `duplicate_rate`,
`batch_quality_trend`.

## 15. Docker

```bash
docker compose up --build                            # PostgreSQL + one full demo run
docker compose run --rm pipeline txn-pipeline demo   # run again (idempotent skip of the same file)
docker compose run --rm pipeline txn-pipeline batches
docker compose run --rm pipeline txn-pipeline query batch_quality_trend
docker compose down -v                               # remove metadata and data volumes
```

The entrypoint waits for PostgreSQL, applies Alembic migrations, then runs the command as a
non-root user. Data lives in the `pipeline-data` volume (`/app/data`).

## 16. Demo scenario

`txn-pipeline demo` runs the complete scenario end to end:

1. generate 10,000 synthetic transactions (2% invalid, 1% duplicates, 0.5% conflicting pairs, 1% warning-only);
2. run the pipeline; invalid rows are quarantined with rule ids;
3. exact and idempotent duplicates are dropped, conflicting versions are quarantined under `DQ006`;
4. accepted rows are normalised, enriched and reconciled by row count and per-currency amount;
5. curated Parquet is written per booking month and the DuckDB star schema is rebuilt;
6. four analytical queries are printed;
7. the same file is processed again and the curated row count is shown to be unchanged.

Representative output (seed 42, 10,000 requested rows):

```
Source records     10,150
Accepted unique    9,750
Rejected           301  (of which conflicts 101)
Duplicates         99  (exact 66, idempotent 33, cross-batch 0)
Curated rows       9,750
Quality pass rate  96.06%
Reconciliation     PASS
second run: status=COMPLETED skipped=True; curated rows before=9,750 after=9,750 -> no duplicate load
```

The generator injected 200 invalid rows, 100 duplicates and 50 conflicting second versions.
Each conflicting pair quarantines both versions (100 records), and one injected duplicate
happened to copy a conflicted transaction, so it is quarantined as a third conflicting version
rather than dropped as a duplicate: 301 rejected, 99 duplicates. The accounting invariant still
holds exactly: 9,750 + 301 + 99 = 10,150.

## 17. Testing

```bash
uv run pytest                      # SQLite plus embedded PostgreSQL (pgserver) when available
uv run pytest --cov                # coverage report
uv run pytest -m "not postgres"    # skip PostgreSQL-backed tests
TEST_DATABASE_URL=postgresql+psycopg://... uv run pytest -m postgres
```

Current state: **237 tests** (200 unit, 26 integration, 7 property, 4 end-to-end) passing in about
15 seconds locally, **98.5% line and branch coverage** of `txn_pipeline`. PostgreSQL-backed tests use
an embedded server locally and a service container in CI; the rest run on SQLite.

Suites:

* **unit**: contract validation and normalisation, Decimal precision, currency minor units,
  timestamps, every quality rule, duplicate and conflict classification, reconciliation math,
  partition paths, source hashing, enrichment, generator determinism, adapter mapping, CLI;
* **integration**: CSV / JSONL / Parquet ingestion parity, batch registration and metadata
  persistence, quarantine and curated outputs, DuckDB model, restart after an injected failure,
  idempotent re-runs and content versioning, Alembic upgrade / downgrade on SQLite and PostgreSQL;
* **property** (Hypothesis): record accounting invariant, reprocessing never grows curated rows,
  idempotent duplicates, changed payload becomes a conflict, offset timestamps map to the same UTC
  instant, Decimal round trips without binary float error, accepted rows keep their identity through
  the raw to curated transformation;
* **e2e**: synthetic file -> run -> quarantine -> curated -> reconciliation -> warehouse -> query,
  final status COMPLETED, on both SQLite and PostgreSQL.

Failure-injection tests cover: a parse failure does not corrupt the batch, a transformation failure
yields FAILED, a write failure followed by a retry does not duplicate curated output, and an
unbalanced reconciliation prevents COMPLETED.

## 18. Performance

A basic local benchmark, not a throughput claim. Context for every number below:

* Apple Silicon laptop, single local process (no cluster, no parallelism)
* CSV input generated with the default bad-data rates
* SQLite metadata store on a local SSD
* DuckDB warehouse rebuild included in the elapsed time

| rows | elapsed | records/s | peak RSS |
|-----:|--------:|----------:|---------:|
| 10,150 | 0.92 s | ~11,000 | ~370 MB |
| 101,500 | 7.53 s | ~13,500 | ~1.2 GB |

v1 reads the whole input file into memory and keeps raw rows, validated records and the Polars frame
alive at once, so memory becomes significant around 100k rows (about 1.2 GB peak RSS above). At the
same volume the metadata registry upsert is the slowest stage. Chunked, streaming processing is a
roadmap item, not a current capability. [docs/PERFORMANCE.md](docs/PERFORMANCE.md) has the per-stage
breakdown. No enterprise-scale throughput is implied.

## 19. Security

Synthetic data only; no real customer, account or employer data; no secrets in datasets or
configuration (`.env.example` holds fictional local values). Quarantine files contain invalid
*synthetic* rows here, but in a real system rejected data is still sensitive and needs the same
controls as curated data. A production deployment would additionally require encryption at rest
and in transit, access control, retention policies and data classification. This demo does not
claim to be production-compliant. See [SECURITY.md](SECURITY.md) and
[docs/SECURITY.md](docs/SECURITY.md).

## 20. Design decisions

Architecture decision records live in [docs/ADR](docs/ADR):

* [0001](docs/ADR/0001-use-decimal-for-money.md) Decimal for money; `amount_minor` integers for arithmetic
* [0002](docs/ADR/0002-parquet-as-curated-format.md) Parquet as the curated format, partitioned by booking year and month
* [0003](docs/ADR/0003-duckdb-for-local-analytics.md) DuckDB for local analytics (and why not dbt)
* [0004](docs/ADR/0004-postgresql-for-pipeline-metadata.md) PostgreSQL for metadata with a dialect-neutral SQLite fallback
* [0005](docs/ADR/0005-file-hash-idempotency.md) Content-addressed batches and the transaction registry
* [0006](docs/ADR/0006-quarantine-invalid-records.md) Quarantine instead of drop or crash
* [0007](docs/ADR/0007-deterministic-surrogate-keys.md) Deterministic surrogate keys in the warehouse

Other choices worth calling out: per-record Pydantic validation (slower than vectorised checks,
but it is what makes row-level quarantine with precise rule ids possible); the batch is marked
COMPLETED when curated data and the registry are committed, and only degrades to PARTIAL if the
derived warehouse refresh fails; same filename with different content is a new version, not a
conflict, because the file hash is the identity.

## 21. Limitations

These are conscious v1 scope decisions, not oversights. The project is an educational
implementation and does not claim production readiness for regulated financial workloads.

* **Single-process execution.** One batch at a time, one Python process, no parallelism.
* **Whole-file in-memory processing.** Sized for 10k to 100k rows per file; memory grows linearly.
* **Metadata registry bottleneck.** Every accepted transaction id is upserted into the metadata
  database, which dominates runtime at larger local volumes (about 3 s of the 7.5 s at 100k rows).
* **Warehouse full rebuild.** The DuckDB star schema is rebuilt from all curated Parquet on every run.
* **No reversal referential validation.** A reversal must name an original transaction, but the
  pipeline does not check that the original exists.
* **No same-file concurrent-run lock.** A batch left `RUNNING` by a crashed process is resumed by
  the next run of the same file; two simultaneous runs of one file are not supported.
* **SQLite is for local, single-user execution only.** PostgreSQL is the intended metadata store for
  anything resembling realistic persistence or concurrent access.
* **Static, illustrative enrichment.** Amount bands are currency-agnostic by design.
* **`validate` is intra-batch only.** Cross-batch duplicates need a registered run.

## 22. Roadmap

* Vectorised happy-path validation with per-record fallback for failures.
* Chunked ingestion to bound memory on larger files.
* Incremental warehouse refresh keyed on batch id.
* A second contract version (`financial-transaction/2`) to exercise schema evolution end to end.
* Optional Prometheus text-format metrics export.

## 23. Disclaimer

This project is an independent educational implementation using synthetic financial data and
generic banking concepts. It is not affiliated with, derived from, or representative of any
production banking system. All identifiers in sample and generated data are fictional
(`ACC-SYN-`, `CP-SYN-`, `TXN-SYN-`).

Licensed under the [MIT License](LICENSE).
