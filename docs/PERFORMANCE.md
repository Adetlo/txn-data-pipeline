# Performance

A basic local benchmark, not a throughput claim. The pipeline is a single Python process that
keeps the whole batch in memory; it is sized for the 10k to 100k rows per file that the brief
targets, not for billions of rows.

## How to run

```bash
uv run python scripts/benchmark.py --sizes 10000 100000 --repeats 1
# PostgreSQL metadata instead of a temporary SQLite file:
uv run python scripts/benchmark.py --sizes 10000 --database-url postgresql+psycopg://txn_pipeline:txn_pipeline_dev_only@localhost:5432/txn_pipeline
```

The script generates the rows with the default bad-data rates (2% invalid, 1% duplicates,
0.5% conflicting pairs), writes a CSV, runs the full pipeline including the DuckDB warehouse
rebuild, and prints a Markdown table. Raw results land in `benchmarks/results.json` (ignored by git).

## Results (2026-10-13)

Environment: Apple Silicon laptop (arm64, 8 cores), macOS 26.6, Python 3.12.13, pipeline 0.1.0,
SQLite metadata on local SSD, CSV input, warehouse rebuild included, one run per size.

| rows (incl. injected bad rows) | elapsed | records/s | peak RSS |
|-------------------------------:|--------:|----------:|---------:|
| 10,150 | 0.92 s | ~11,000 | ~370 MB |
| 101,500 | 7.53 s | ~13,500 | ~1.2 GB |

Per-stage timings for the 100k run (milliseconds):

| stage | ms | notes |
|-------|---:|-------|
| INGESTION | 748 | `csv.DictReader` + SHA-256 per row |
| SCHEMA_VALIDATION | 1,450 | Pydantic v2 model per record (~14 µs/row) |
| QUALITY_VALIDATION | 258 | four rules per record |
| DEDUPLICATION | 947 | canonical-JSON payload hash per record + registry lookup |
| TRANSFORMATION | 442 | Polars frame build, enrichment, Pandera validation |
| RECONCILIATION | 10 | integer minor-unit sums per currency |
| QUARANTINE_WRITE | 70 | Parquet + metadata rows |
| CURATED_WRITE | 61 | four monthly partitions, zstd |
| REGISTRY_COMMIT | 3,195 | 100k upserts into `transaction_registry` (SQLite) |
| WAREHOUSE | 192 | full DuckDB star-schema rebuild |

## Reading the numbers

* The pipeline is CPU-bound in pure-Python per-record stages (schema validation, hashing). That
  is a deliberate trade: per-record Pydantic validation is what makes row-level quarantine with
  stable rule ids possible. Vectorising the happy path and only falling back to per-record
  validation for failures would roughly halve the validation cost; it is listed in the roadmap.
* `REGISTRY_COMMIT` dominates at 100k rows because every accepted transaction id is upserted
  into the metadata database. On PostgreSQL this is a single multi-row `INSERT ... ON CONFLICT`
  per 5,000-row chunk; on SQLite it is the same statement but single-threaded on one file.
* Peak RSS grows roughly linearly with the file: raw rows, validated records and the Polars frame
  are all held at once. Streaming the source in chunks would bound memory, at the cost of making
  intra-batch duplicate detection a two-pass operation.
* Throughput is stable between 10k and 100k rows (no super-linear behaviour), which is the
  property that matters at this scale.

No production throughput is implied. Numbers will differ on other hardware and on CI runners.
