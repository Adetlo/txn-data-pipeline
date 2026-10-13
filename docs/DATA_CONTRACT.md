# Data contract

The pipeline accepts one logical record type, the **financial transaction**, under the
contract version `financial-transaction/1`. Every supported file format is mapped onto
the same canonical model, `txn_pipeline.contracts.transaction.TransactionRecord`.

All identifiers in this repository are synthetic (`ACC-SYN-...`, `CP-SYN-...`,
`TXN-SYN-...`). The contract itself does not require that prefix; it only requires
non-blank identifiers without whitespace.

## Canonical record

| Field | Type | Required | Normalisation | Rule on failure |
|---|---|---|---|---|
| `schema_version` | string | no (defaults to `financial-transaction/1`) | trimmed | `SCH005_UNSUPPORTED_SCHEMA_VERSION` |
| `transaction_id` | string, 1 to 64 chars, no whitespace | yes | trimmed | `DQ001_MISSING_TRANSACTION_ID` |
| `source_system` | string, 1 to 64 chars | yes | trimmed, upper-cased | `SCH001_MISSING_REQUIRED_FIELD` / `SCH002_INVALID_FIELD_VALUE` |
| `account_id` | string, 1 to 64 chars | yes | trimmed | `SCH001` / `SCH002` |
| `counterparty_id` | string or null | no | trimmed; blank becomes null | `SCH002` |
| `transaction_type` | enum `TransactionType` | yes | trimmed, upper-cased, spaces and hyphens become `_` | `SCH003_UNKNOWN_TRANSACTION_TYPE` |
| `amount` | Decimal, strictly positive, at most 14 integer digits | yes | parsed without float; scale checked against the currency | `DQ003_INVALID_AMOUNT` |
| `currency` | enum `Currency` (GBP, USD, EUR, JPY, CHF, SGD, AUD) | yes | trimmed, upper-cased | `DQ002_INVALID_CURRENCY` |
| `booking_date` | ISO-8601 date | yes | parsed | `SCH006_INVALID_DATE_FORMAT` |
| `value_date` | ISO-8601 date | yes | parsed | `SCH006_INVALID_DATE_FORMAT` |
| `transaction_timestamp` | ISO-8601 timestamp with offset | yes | converted to UTC; original text kept in `transaction_timestamp_raw` | `DQ009_INVALID_TIMESTAMP` |
| `channel` | enum `Channel` (ONLINE, MOBILE, BRANCH, ATM, API, CARD, BATCH) | yes | trimmed, upper-cased | `SCH004_UNKNOWN_CHANNEL` |
| `country` | ISO 3166-1 alpha-2 | yes | trimmed, upper-cased | `DQ008_INVALID_COUNTRY_CODE` |
| `reference` | string up to 140 chars or null | no | trimmed, internal whitespace collapsed | `SCH002` |
| `status` | enum `TransactionStatus` (PENDING, POSTED, REVERSED, CANCELLED) | yes | trimmed, upper-cased | `DQ007_UNKNOWN_STATUS` |
| `merchant_category` | string or null | no | trimmed, collapsed, upper-cased | n/a |
| `beneficiary_country` | ISO 3166-1 alpha-2 or null | no | trimmed, upper-cased | `DQ008_INVALID_COUNTRY_CODE` |
| `is_reversal` | boolean | no (defaults to false) | accepts true/false, t/f, 1/0, yes/no, y/n | `SCH002` |
| `original_transaction_id` | string or null | required when `is_reversal` is true (quality stage) | trimmed | `DQ010_REVERSAL_MISSING_ORIGINAL` |

Rules that need more than one field (`DQ004`, `DQ005`, `DQ010`, `DQ011`) run in the
quality stage after the record has been parsed. Unknown input fields are ignored.
Blank strings and the literals `null`, `none`, `nan` are treated as missing. The full
rule catalogue is in [DATA_QUALITY.md](DATA_QUALITY.md).

Two derived fields are set by the model and are not part of the input:
`amount_minor` (exact integer minor units) and `transaction_timestamp_raw`.

### Example

```json
{
  "transaction_id": "TXN-20261013-000001",
  "source_system": "SYNTHETIC_CORE",
  "account_id": "ACC-100001",
  "counterparty_id": "ACC-200001",
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

## Schema versioning

`schema_version` is carried on every input record (defaulting to the current version),
on every curated row and on every batch (`pipeline_batches.schema_version`).
`SUPPORTED_SCHEMA_VERSIONS` in `contracts/enums.py` lists what the running pipeline
accepts; any other value is rejected with `SCH005_UNSUPPORTED_SCHEMA_VERSION`.

A future `financial-transaction/2` would be introduced as a second Pydantic model plus
an adapter that maps it onto the curated frame, with the version added to
`SUPPORTED_SCHEMA_VERSIONS`. Because the version is resolved per record, a file may mix
versions and each row is validated against its own contract. The curated frame keeps
the `schema_version` column so downstream consumers can tell which contract produced a
row.

## Input formats

| Format | Extensions | Reader | Value types seen by the contract |
|---|---|---|---|
| CSV | `.csv` | `ingestion/csv_reader.py` (`csv.DictReader`, UTF-8 with optional BOM) | every value is a string; the header row is required |
| JSON Lines | `.jsonl`, `.ndjson`, `.json` | `ingestion/json_reader.py` | JSON types (string, number, boolean, null); a line that is not a JSON object becomes a record rejected with `SCH007_UNPARSEABLE_RECORD` |
| Parquet | `.parquet`, `.pq` | `ingestion/parquet_reader.py` (PyArrow, batches of 10,000 rows) | typed values: `decimal128` becomes `Decimal`, `date32` becomes `date`, `bool` becomes `bool`; strings are accepted for every field as well |

The format is inferred from the extension or forced with `--format`. All three readers
yield `RawRecord(record_number, data, raw_hash)` where `record_number` is the 1-based
row position and `raw_hash` is the SHA-256 of the canonical JSON of the row.

## Money

See [ADR 0001](ADR/0001-use-decimal-for-money.md).

* The contract parses `amount` with `decimal.Decimal`. Strings may contain thousands
  separators (`1,250.00`); floats are only accepted from typed sources and are converted
  through `repr` so `0.1` becomes `Decimal("0.1")`.
* The amount must be finite, strictly positive and at most
  `10^14 - 1` (`MAX_AMOUNT` in `transformation/currency.py`).
* The number of decimal places must not exceed the ISO 4217 minor-unit exponent of the
  currency (`CURRENCY_EXPONENT`): `JPY` allows none, every other supported currency
  allows two. `250.555 GBP` and `100.5 JPY` are rejected with `DQ003_INVALID_AMOUNT`.
* The accepted amount is quantised to the currency exponent (`250.5` becomes `250.50`).
* `amount_minor = amount * 10^exponent` as an exact integer.

In the curated frame money is stored twice, deliberately:

| Column | Type | Purpose |
|---|---|---|
| `amount` | `decimal(18, 4)` | human-readable; DuckDB sums it exactly |
| `amount_minor` | `int64` | exact integer arithmetic inside the pipeline (reconciliation totals) |
| `currency_exponent` | `int8` | converts between the two |

No binary float is used for money at any point. The only float conversion is inside the
`amount_band` enrichment expression, which compares against thresholds and never feeds
arithmetic.

## Timezone policy

* `transaction_timestamp` must carry a UTC offset or `Z`.
* The stored analytical timestamp is always UTC (`Datetime("us", "UTC")` in Polars,
  `timestamp[us, tz=UTC]` in Parquet).
* A timestamp without an offset is rejected with `DQ009_INVALID_TIMESTAMP` unless the
  `SOURCE_TIMEZONE` setting names an IANA zone, in which case the naive value is
  interpreted in that zone. The host timezone is never used.
* The original text is preserved in `transaction_timestamp_raw` so the source offset
  remains visible for lineage.

`booking_date` and `value_date` are calendar dates and are not timezone-adjusted.

## Enrichment

All enrichment is computed from the record itself; nothing calls an external service.

| Column | Rule |
|---|---|
| `amount_band` | on the major-unit amount regardless of currency: `< 10` MICRO, `< 100` SMALL, `< 1000` MEDIUM, otherwise LARGE (`AMOUNT_BAND_UPPER_BOUNDS`) |
| `direction` | INBOUND for DEPOSIT, REFUND, INTEREST; INTERNAL for TRANSFER whose `counterparty_id` starts with `ACC-`; OUTBOUND otherwise |
| `booking_year`, `booking_month`, `booking_day` | parts of `booking_date` |
| `booking_weekday` | ISO weekday, 1 = Monday to 7 = Sunday |
| `is_weekend` | `booking_weekday >= 6` |
| `country_region` | static lookup `COUNTRY_REGION` in `contracts/countries.py` (EUROPE, NORTH_AMERICA, LATIN_AMERICA, ASIA_PACIFIC, MIDDLE_EAST_AFRICA); unmapped countries become `OTHER` |

The amount band thresholds are a documented simplification for synthetic data, not a
business definition.

## Curated columns

Defined in `contracts/schemas.py` (`CURATED_POLARS_SCHEMA`, written in this order).

Identity and contract: `transaction_id`, `schema_version`, `source_system`.

Business payload: `account_id`, `counterparty_id`, `transaction_type`, `amount`,
`amount_minor`, `currency`, `currency_exponent`, `booking_date`, `value_date`,
`transaction_timestamp`, `transaction_timestamp_raw`, `channel`, `country`,
`reference`, `status`, `merchant_category`, `beneficiary_country`, `is_reversal`,
`original_transaction_id`.

Enrichment and quality: `amount_band`, `direction`, `booking_year`, `booking_month`,
`booking_day`, `booking_weekday`, `is_weekend`, `country_region`, `dq_warning_count`,
`dq_warnings` (comma-separated WARNING rule ids, null when none).

Lineage: `batch_id`, `source_filename`, `source_file_hash`, `source_record_number`,
`raw_record_hash`, `payload_hash`, `ingested_at`, `pipeline_version`.

`transaction_id` is unique within the curated frame of a batch and, through the
transaction registry, across batches. `booking_year` and `booking_month` are also the
partition keys of the Parquet layout.

## Quarantine record

Written to `quarantine/batch_id=<id>/rejected.parquet` and to the `quarantine_records`
table (`QUARANTINE_POLARS_SCHEMA`):

| Column | Meaning |
|---|---|
| `batch_id` | batch that rejected the row |
| `record_number` | 1-based row number in the source file |
| `transaction_id` | as found in the raw row, if any |
| `rule_id` | primary (first) failing rule |
| `all_rule_ids` | every failing rule, comma-separated |
| `severity` | severity of `rule_id` |
| `reason` | human-readable `RULE_ID: message` list |
| `stage` | `SCHEMA_VALIDATION`, `QUALITY_VALIDATION` or `DEDUPLICATION` |
| `raw_record` | canonical JSON of the original row |
| `source_filename`, `source_file_hash` | source identity |
| `quarantined_at` | UTC timestamp |
| `pipeline_version` | version that rejected the row |

## corebank-ledger adapter

[corebank-ledger](https://github.com/Adetlo/corebank-ledger) is a double-entry ledger
whose journal entries have DEBIT and CREDIT postings. `adapters/corebank.py` flattens a
JSON Lines export of journal entries into canonical transactions. There is no runtime
dependency: the adapter only maps a JSON shape.

| Canonical field | Source in the export |
|---|---|
| `transaction_id` | entry `id` |
| `source_system` | constant `COREBANK_LEDGER_DEMO` |
| `account_id` | `account_number` (or `account_id`) of the first DEBIT posting |
| `counterparty_id` | `account_number` (or `account_id`) of the first CREDIT posting |
| `transaction_type` | `metadata.kind`, upper-cased, default `TRANSFER` |
| `amount`, `currency` | the DEBIT posting (currency falls back to the entry currency) |
| `booking_date` | entry `booking_date` |
| `value_date` | entry `value_date`, falling back to `booking_date` |
| `transaction_timestamp` | `posted_at`, falling back to `created_at` |
| `channel` | `metadata.channel`, default `API` |
| `country` | `metadata.country`, default `GB` |
| `reference` | entry `reference` |
| `status` | entry `status` |
| `is_reversal` | true when `reversal_of` is set |
| `original_transaction_id` | `reversal_of` |

Run it with `txn-pipeline run --input export.jsonl --adapter corebank-ledger --source COREBANK`.

Conceptual integration of the two projects:

```
corebank-ledger (ledger of record)
      -> synthetic journal-entry export (JSON Lines)
      -> txn-data-pipeline (validate, dedupe, reconcile, enrich)
      -> curated Parquet + DuckDB star schema (analytics / ML-ready)
```

Each project is independently runnable; the adapter and `examples/` are the only link.
