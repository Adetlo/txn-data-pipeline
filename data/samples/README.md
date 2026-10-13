# Sample fixtures

Tiny, committed, fully synthetic inputs (generator seed 2026) used by the tests and the quick start.

| File | Rows | Content |
|------|-----:|---------|
| `transactions_sample.csv` | 208 | 200 generated records + injected invalid rows, duplicates and one conflicting pair |
| `transactions_sample.jsonl` | 208 | the same rows as JSON Lines (typed booleans, nulls for blanks) |
| `transactions_sample_clean.parquet` | 200 | the clean variant with typed Parquet columns (`decimal128(18,4)`, `date32`, `bool`) |

Regenerate with:

```bash
uv run txn-pipeline generate-demo-data --records 200 --seed 2026 --invalid-rate 0.05 \
  --duplicate-rate 0.03 --conflict-rate 0.01 --warning-rate 0.02 --output data/samples/transactions_sample.csv
```

All identifiers are obviously synthetic (`ACC-SYN-`, `CP-SYN-`, `TXN-SYN-`).
