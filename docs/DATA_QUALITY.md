# Data quality

Every record that enters the pipeline ends up in exactly one of three buckets:
**accepted**, **rejected** (quarantined with a rule id) or **duplicate** (dropped as an
exact or idempotent repeat). Nothing is silently dropped.

Rule identifiers are stable. They are never renumbered or reused because
`quarantine_records`, `quality_results` and the warehouse key on them.
The catalogue lives in `src/txn_pipeline/contracts/rules.py`.

## Rule catalogue

| Rule id | Severity | Stage | Description | Example trigger |
|---|---|---|---|---|
| `SCH001_MISSING_REQUIRED_FIELD` | ERROR | SCHEMA_VALIDATION | A required contract field is absent or empty | empty `account_id` |
| `SCH002_INVALID_FIELD_VALUE` | ERROR | SCHEMA_VALIDATION | Wrong type or unparseable value not covered by a DQ rule | `is_reversal = "maybe"`, `reference` longer than 140 chars |
| `SCH003_UNKNOWN_TRANSACTION_TYPE` | ERROR | SCHEMA_VALIDATION | `transaction_type` not in the vocabulary | `WIRE` |
| `SCH004_UNKNOWN_CHANNEL` | ERROR | SCHEMA_VALIDATION | `channel` not in the vocabulary | `KIOSK` |
| `SCH005_UNSUPPORTED_SCHEMA_VERSION` | ERROR | SCHEMA_VALIDATION | `schema_version` not supported | `financial-transaction/9` |
| `SCH006_INVALID_DATE_FORMAT` | ERROR | SCHEMA_VALIDATION | `booking_date` / `value_date` is not an ISO-8601 date | `13/10/2026` |
| `SCH007_UNPARSEABLE_RECORD` | ERROR | SCHEMA_VALIDATION | The row could not be parsed into a key/value record | a JSON Lines line that is not a JSON object |
| `DQ001_MISSING_TRANSACTION_ID` | ERROR | SCHEMA_VALIDATION | `transaction_id` missing, blank, too long or containing whitespace | empty id |
| `DQ002_INVALID_CURRENCY` | ERROR | SCHEMA_VALIDATION | Currency is not a supported ISO 4217 code | `GBX` |
| `DQ003_INVALID_AMOUNT` | ERROR | SCHEMA_VALIDATION | Amount unparseable, not positive, too large, or more decimals than the currency allows | `abc`, `-12.00`, `250.555 GBP`, `100.5 JPY` |
| `DQ004_BOOKING_OUTSIDE_ALLOWED_RANGE` | ERROR | QUALITY_VALIDATION | `booking_date` before `MIN_BOOKING_DATE` or after today + `MAX_BOOKING_DAYS_IN_FUTURE` | `1999-12-31` |
| `DQ005_VALUE_DATE_TOO_FAR_FROM_BOOKING` | WARNING | QUALITY_VALIDATION | `value_date` differs from `booking_date` by more than `VALUE_DATE_TOLERANCE_DAYS` (default 5) | value date 30 days after booking |
| `DQ006_DUPLICATE_TRANSACTION` | ERROR | DEDUPLICATION | Same `transaction_id` seen with a materially different payload (conflict) | two rows with the same id and different amounts |
| `DQ007_UNKNOWN_STATUS` | ERROR | SCHEMA_VALIDATION | `status` not in the vocabulary | `DONE` |
| `DQ008_INVALID_COUNTRY_CODE` | ERROR | SCHEMA_VALIDATION | `country` / `beneficiary_country` not ISO 3166-1 alpha-2 | `UKX` |
| `DQ009_INVALID_TIMESTAMP` | ERROR | SCHEMA_VALIDATION | Timestamp unparseable, or no offset and no `SOURCE_TIMEZONE` configured | `2026-10-13T09:31:22` with no zone |
| `DQ010_REVERSAL_MISSING_ORIGINAL` | ERROR | QUALITY_VALIDATION | `is_reversal` true but `original_transaction_id` missing or self-referencing | reversal row with empty original id |
| `DQ011_TIMESTAMP_BOOKING_MISMATCH` | WARNING | QUALITY_VALIDATION | UTC date of the timestamp differs from `booking_date` by more than `TIMESTAMP_BOOKING_TOLERANCE_DAYS` (default 1) | timestamp three days after the booking date |

Thresholds come from `Settings` (see `.env.example`) and are part of the batch
`config_hash`.

## ERROR versus WARNING

* **ERROR**: the record is rejected. It is written to the quarantine Parquet file and
  the `quarantine_records` table with the primary rule id, all failing rule ids, the
  stage and the original raw row. It never reaches the curated output.
* **WARNING**: the record is accepted. The finding is counted per rule, and the curated
  row carries `dq_warning_count` and `dq_warnings` (comma-separated rule ids) so
  analysts can filter on it.
* `FAIL_ON_WARNINGS=true` turns warnings into rejections for the batch (useful for
  strict sources). The rule severity in the catalogue does not change; the quarantine
  row records the WARNING rule as the reason.

A record with several findings is quarantined once, under the first ERROR rule, with
every rule id listed in `all_rule_ids`. Each rule is still counted individually in the
per-rule summary.

## How Pydantic errors become rule ids

Validators in `TransactionRecord` raise `ValueError("<RULE_ID>: message")`. The schema
stage (`validation/schema.py`, `classify_error`) strips Pydantic's `Value error, `
prefix and uses the rule id when the prefix is in the catalogue. Two fallbacks cover
errors raised by Pydantic itself:

* a `missing` error maps to the field's dedicated rule when one exists
  (`transaction_id` to `DQ001`, `currency` to `DQ002`, `amount` to `DQ003`, `status` to
  `DQ007`, `country` to `DQ008`, `transaction_timestamp` to `DQ009`), otherwise to
  `SCH001_MISSING_REQUIRED_FIELD`;
* anything else maps to `SCH002_INVALID_FIELD_VALUE`.

A row that is not a mapping at all (bad JSON Lines line) is wrapped by the reader into a
record that fails with `SCH007_UNPARSEABLE_RECORD`.

## Duplicates and conflicts

Implemented in `validation/duplicates.py`. Two hashes are computed per record:

* `raw_hash`: SHA-256 of the canonical JSON of the raw row as read;
* `payload_hash`: SHA-256 of the canonical, normalised business payload
  (`TransactionRecord.business_payload()`), which excludes lineage-only fields.

Within a batch, records that passed schema and quality validation are grouped by
`transaction_id`:

| Situation | Classification | Outcome |
|---|---|---|
| same id, identical raw rows | `EXACT_DUPLICATE` | first occurrence kept, others dropped and counted |
| same id, same payload hash, different raw bytes (case, whitespace) | `IDEMPOTENT_DUPLICATE` | first occurrence kept, others dropped and counted |
| same id, different payload hashes | **conflict** | every version with that id is quarantined under `DQ006_DUPLICATE_TRANSACTION`; the reason lists the differing fields |

Across batches, the `transaction_registry` table records the payload hash of the
curated version of every `transaction_id`:

| Situation | Classification | Outcome |
|---|---|---|
| id already curated, same payload hash | `CROSS_BATCH_DUPLICATE` | dropped and counted |
| id already curated, different payload hash | conflict | only the new record is quarantined; the curated row from the earlier batch stands |

The first curated version wins. Registry rows written by the *same* batch are ignored
during deduplication, so a restarted batch re-emits its own rows instead of treating
them as cross-batch duplicates.

Rows that failed schema or quality validation never take part in deduplication: their
`transaction_id` cannot be trusted, so they are quarantined under their own rule.

### Worked example

A file contains these rows (other fields identical):

| row | transaction_id | amount | currency |
|---|---|---|---|
| 1 | TXN-SYN-1 | 10.00 | GBP |
| 2 | TXN-SYN-1 | 10.00 | gbp |
| 3 | TXN-SYN-2 | 5.00 | EUR |
| 4 | TXN-SYN-2 | 7.00 | EUR |
| 5 | TXN-SYN-3 | 9.00 | USD |

Row 2 normalises to the same payload as row 1: it is an `IDEMPOTENT_DUPLICATE` and row 1
is kept. Rows 3 and 4 disagree on `amount`: both are quarantined with
`DQ006_DUPLICATE_TRANSACTION` ("differing fields ['amount']"). Row 5 is accepted.
Result: accepted 2, duplicates 1, rejected 2 (conflicts 2), which satisfies
`5 = 2 + 2 + 1`.

If `TXN-SYN-3` was already curated by an earlier batch with amount 9.00, row 5 becomes a
`CROSS_BATCH_DUPLICATE` instead; with amount 9.50 it would be a cross-batch conflict and
quarantined alone.

## Per-rule summaries

Every batch stores one `quality_results` row per rule that fired
(`batch_id`, `rule_id`, `severity`, `stage`, `record_count`). The same counts appear in
the batch result (`rule_counts`), in the reconciliation report and in the CLI output of
`txn-pipeline run` and `txn-pipeline validate`.

```sql
-- PostgreSQL or SQLite metadata database
SELECT rule_id, severity, SUM(record_count) AS records
FROM quality_results
GROUP BY rule_id, severity
ORDER BY records DESC;
```

The warehouse exposes rejections over time through `fact_quarantine` and the bundled
`rejected_trend` query (see [WAREHOUSE_MODEL.md](WAREHOUSE_MODEL.md)).
