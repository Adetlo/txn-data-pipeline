# Transaction Data Pipeline

An educational data pipeline for ingesting, validating and transforming synthetic financial
transactions into analytics-ready datasets. Python 3.12, Pydantic v2 and Polars.

> **Disclaimer.** This project is an independent educational implementation using synthetic
> financial data and generic banking concepts. It is not affiliated with, derived from, or
> representative of any production banking system.

## Status

Early development. The canonical transaction contract (`financial-transaction/1`), the
controlled vocabularies, the data-quality rule catalogue and the first file readers are
being built; batch orchestration, reconciliation and the analytical layer will follow.

## Layout

```
src/txn_pipeline/
├── contracts/        # enums, rule catalogue, TransactionRecord, curated frame schema
├── ingestion/        # file readers (CSV, JSON Lines)
├── transformation/   # normalisation, currency and timestamp helpers
├── validation/       # result value objects
├── config.py
└── exceptions.py
```

## Development

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
ruff check . && mypy && pytest
```

Money is `Decimal` at every contract boundary; timestamps are normalised to UTC and must
carry an offset. Identifiers in all synthetic data are obviously fictional
(`ACC-SYN-`, `CP-SYN-`, `TXN-SYN-`).

Licensed under the [MIT License](LICENSE).
