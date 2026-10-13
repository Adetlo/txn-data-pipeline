# Operations

How to run, inspect, retry and clean up the pipeline locally or in the container. The
CLI is `txn-pipeline` (`uv run txn-pipeline --help`).

## Configuration

Settings come from environment variables or a `.env` file (`txn_pipeline/config.py`,
see `.env.example`). The global CLI options `--data-root`, `--database-url`,
`--log-level` and `--log-format` override them.

| Setting | Default | Meaning |
|---|---|---|
| `METADATA_DATABASE_URL` | `sqlite:///./data/metadata/pipeline.db` | SQLAlchemy URL; PostgreSQL is the target, SQLite is the zero-infrastructure fallback |
| `DATA_ROOT` | `./data` | root of `incoming/`, `curated/`, `quarantine/`, `reports/`, `metadata/`, `warehouse/` |
| `WAREHOUSE_PATH` | `<DATA_ROOT>/warehouse/txn_warehouse.duckdb` | DuckDB file |
| `SOURCE_TIMEZONE` | unset | IANA zone applied to timestamps without an offset; unset means reject them |
| `MIN_BOOKING_DATE` | `2000-01-01` | `DQ004` lower bound |
| `MAX_BOOKING_DAYS_IN_FUTURE` | `1` | `DQ004` upper bound relative to today |
| `VALUE_DATE_TOLERANCE_DAYS` | `5` | `DQ005` threshold |
| `TIMESTAMP_BOOKING_TOLERANCE_DAYS` | `1` | `DQ011` threshold |
| `FAIL_ON_WARNINGS` | `false` | reject records with WARNING findings |
| `BUILD_WAREHOUSE_ON_RUN` | `true` | rebuild DuckDB after each batch |
| `ACTOR` | `CLI` | recorded on the batch (`CLI`, `SYSTEM`, `TEST`) |
| `LOG_LEVEL`, `LOG_FORMAT` | `INFO`, `console` | `LOG_FORMAT=json` for machine-readable logs |

## Metadata database

* **SQLite** (default): migrations are applied automatically by every command that
  needs the schema. Suitable for a single local process only.
* **PostgreSQL**: set `METADATA_DATABASE_URL=postgresql+psycopg://...` and run
  `txn-pipeline db-upgrade` once (Alembic `upgrade head`). Commands refuse to run
  against an uninitialised PostgreSQL database. The container entrypoint runs the
  migration before the command.

## Running a batch

```bash
txn-pipeline generate-demo-data --records 10000 --seed 42 --output data/incoming/txns.csv
txn-pipeline validate --input data/incoming/txns.csv          # dry run, no writes
txn-pipeline run --input data/incoming/txns.csv --source SYNTHETIC_CORE
txn-pipeline run --input export.jsonl --adapter corebank-ledger --source COREBANK
txn-pipeline demo                                             # full scripted scenario
```

`run` options: `--format` to force `csv` / `jsonl` / `parquet`, `--adapter` to map a
foreign export shape, `--no-warehouse` to skip the DuckDB rebuild, `--force` to re-run a
file that already completed, `--json` to print the full result.

Exit codes: `0` COMPLETED, `1` FAILED (or usage / pipeline error), `2` PARTIAL.

## Inspecting batches

```bash
txn-pipeline batches --limit 20
txn-pipeline batch-status --batch-id <uuid>          # status, counts, checkpoints, lineage
txn-pipeline batch-status --batch-id <uuid> --json
```

`batch-status` shows every checkpoint (`stage`, `status`, `attempt`, `details`) and the
`error_message` of a failed attempt. A `FAILED` checkpoint names the stage that broke;
`details.error` holds the exception text.

### Log events

Logs go to stderr through structlog. Every line carries `timestamp`, `level`, `event`
and the bound context `correlation_id`, `source`, `source_filename` and, once
registered, `batch_id`; stage events add `stage` and `duration_ms`. Full records are
never logged.

| Event | Extra fields |
|---|---|
| `BATCH_REGISTERED` | `source_hash`, `size_bytes`, `format`, `previous_versions` |
| `BATCH_RESUMED` | `attempt`, `previous_status` |
| `BATCH_SKIPPED_IDEMPOTENT` | `source_hash` |
| `INGESTION_STARTED` | |
| `INGESTION_COMPLETED` | `record_count` |
| `SCHEMA_VALIDATION_COMPLETED` | `valid`, `rejected` |
| `QUALITY_CHECK_COMPLETED` | `accepted`, `rejected_total` |
| `DEDUPLICATION_COMPLETED` | `accepted`, `duplicates`, `conflicts` |
| `TRANSFORMATION_COMPLETED` | `curated_rows` |
| `RECONCILIATION_COMPLETED` | `balanced` |
| `QUARANTINE_WRITTEN` | `quarantined`, `path` |
| `CURATED_WRITTEN` | `files`, `rows` |
| `REGISTRY_COMMITTED` | `registered` |
| `WAREHOUSE_BUILT` | row count per warehouse table |
| `BATCH_COMPLETED` / `BATCH_PARTIAL` | `status` and the metrics summary fields |
| `BATCH_FAILED` | `failure_kind` (`reconciliation` or `stage`), `error` |
| `STAGE_STARTED`, `STAGE_FAILED` | debug-level / checkpoint events |

### Metrics summary

Stored in `pipeline_batches.metrics`, printed by `run --json` and logged on
`BATCH_COMPLETED`: `records_processed`, `records_accepted`, `records_rejected`,
`records_duplicate`, `records_conflict`, `records_warning`, `quality_pass_rate`
(accepted / processed), `duration_seconds`, `records_per_second`,
`stage_durations_ms` (per stage).

## Reviewing quarantine

```bash
txn-pipeline quarantine --batch-id <uuid>                     # counts by rule + first rows
txn-pipeline quarantine --batch-id <uuid> --rule DQ003_INVALID_AMOUNT --limit 50
txn-pipeline quarantine --batch-id <uuid> --raw               # print the original rows
```

Sources: `<DATA_ROOT>/quarantine/batch_id=<uuid>/rejected.parquet` (readable with
Polars, DuckDB or PyArrow) and the `quarantine_records` table. Both hold the raw row
as JSON plus `rule_id`, `all_rule_ids`, `reason`, `stage` and `record_number`.
Quarantined rows are never re-injected automatically: fix the source and submit a new
file (which is a new batch, see below).

## Retrying

Re-run the same command with the same file. The orchestrator looks up the file's
SHA-256:

* a `COMPLETED` batch exists: no-op (`BATCH_SKIPPED_IDEMPOTENT`), exit 0, the previous
  result is printed;
* a `FAILED`, `PARTIAL`, `RUNNING` or `DISCOVERED` batch exists: that batch is resumed
  with `attempt + 1` and the same `batch_id`. A `RUNNING` batch left behind by a crashed
  process is therefore simply re-run; there is no lock, so make sure the previous
  process is really gone before retrying;
* `--force` re-runs a `COMPLETED` batch as a new attempt; outputs are rewritten
  byte-for-byte equivalent, never duplicated.

All in-memory stages (`INGESTION` to `RECONCILIATION`) are recomputed on a resume; they
are deterministic and cheap. The write stages are idempotent by construction:

* `QUARANTINE_WRITE` and `CURATED_WRITE` write to deterministic paths through a temp
  file and rename, and `CURATED_WRITE` first deletes every partition of the same
  `batch_id`;
* `REGISTRY_COMMIT` upserts on `transaction_id`, and registry rows of the batch being
  resumed are ignored during deduplication so its own rows are re-emitted;
* `WAREHOUSE` is a full rebuild.

A different file with the same name is a **new** batch (content-addressed); the log
reports `previous_versions` so the operator can see a file was replaced. Rows in the new
version that match already curated rows become cross-batch duplicates; rows that differ
become conflicts.

## Validating reconciliation

```bash
txn-pipeline reconcile --batch-id <uuid>
```

Prints the stored report and re-counts curated rows on disk. Exit 0 only when the
report is balanced and the files still match. See [RECONCILIATION.md](RECONCILIATION.md).

## Rebuilding the warehouse

```bash
txn-pipeline warehouse-build
txn-pipeline query                      # list bundled queries
txn-pipeline query batch_quality_trend --limit 50
```

Use this after a `PARTIAL` batch or after deleting curated files by hand. The rebuild
reads every curated partition and every quarantine file plus the batch table.

## Cleaning local generated data

Everything under `DATA_ROOT` is generated and git-ignored:

```bash
rm -rf data/incoming data/curated data/quarantine data/reports data/warehouse data/metadata
```

Delete `data/metadata/pipeline.db` (SQLite) or drop the tables in PostgreSQL
(`alembic downgrade base`) together with the curated files, otherwise the metadata
will describe outputs that no longer exist (`reconcile` will report a mismatch and the
registry will keep treating old transaction ids as curated). With Docker Compose the
PostgreSQL data lives in the named volume `pgdata` (`docker compose down -v` removes
it) and the pipeline data in the bind-mounted `./data`.

## Operational limitations

* One process, one file per run; no scheduler, queue or lock. Run batches sequentially.
* The whole file is held in memory as Python objects; comfortable for 10k to a few
  hundred thousand rows, not for multi-gigabyte files.
* The warehouse is a full rebuild on every run.
* SQLite metadata is for a single local process; use PostgreSQL for anything shared.
* No alerting or metrics export; metrics live in logs and the batch table.
* Quarantined records require a new source file to be reprocessed.
* Cross-file referential checks (does a reversal's original exist?) are out of scope;
  `DQ010` only checks that an original id is present.
