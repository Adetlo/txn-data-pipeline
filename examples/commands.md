# CLI cheat sheet

```bash
# zero-infrastructure (SQLite metadata under ./data/metadata)
uv run txn-pipeline demo                                   # full scenario with 10,000 synthetic rows
uv run txn-pipeline generate-demo-data --records 50000 --seed 1 --output data/incoming/big.parquet --format parquet
uv run txn-pipeline validate --input data/incoming/big.parquet --strict
uv run txn-pipeline run --input data/incoming/big.parquet --source SYNTHETIC_CORE --json
uv run txn-pipeline batches
uv run txn-pipeline batch-status --batch-id <uuid>
uv run txn-pipeline reconcile --batch-id <uuid>
uv run txn-pipeline quarantine --batch-id <uuid> --rule DQ006_DUPLICATE_TRANSACTION --raw
uv run txn-pipeline warehouse-build
uv run txn-pipeline query                                  # list bundled analytical queries
uv run txn-pipeline query daily_value_by_currency --limit 30

# PostgreSQL metadata
export METADATA_DATABASE_URL=postgresql+psycopg://txn_pipeline:txn_pipeline_dev_only@localhost:5432/txn_pipeline
uv run txn-pipeline db-upgrade
uv run txn-pipeline run --input data/samples/transactions_sample.csv

# containers
docker compose up --build                                  # postgres + one full demo run
docker compose run --rm pipeline txn-pipeline batches
```
