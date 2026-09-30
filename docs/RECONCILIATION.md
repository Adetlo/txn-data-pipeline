# Reconciliation

Reconciliation runs **before** any output is written. If the controls do not balance
the batch fails and nothing is written, so a `COMPLETED` batch is always one whose
numbers add up. Implementation: `reconciliation/controls.py`, `reconciliation/batch.py`.

## Row-count invariant

```
source_records = accepted_records + rejected_records + duplicate_records
```

* `source_records`: rows read from the file (including unparseable rows).
* `accepted_records`: unique canonical transactions that survived schema validation,
  quality rules and deduplication.
* `rejected_records`: every quarantined row. This **includes conflicts**
  (`DQ006_DUPLICATE_TRANSACTION`); `conflict_records` is reported separately as the
  subset.
* `duplicate_records = exact_duplicates + idempotent_duplicates + cross_batch_duplicates`.

Two further controls complete the row accounting:

* `curated_records == accepted_records`: the curated frame built in memory has exactly
  one row per accepted record;
* post-write control: after the Parquet partitions are written, the rows actually on
  disk for this batch id are counted again and must equal the curated row count
  (`warehouse/writer.py`, `curated_row_count`). A mismatch raises `OutputWriteError`.

## Amount reconciliation by currency

For each currency present in the accepted records:

```
sum(amount_minor of accepted records)  ==  sum(amount_minor of curated rows)
count(accepted records)                ==  count(curated rows)
```

Both sides use integer minor units (`amount_minor`), so the comparison is exact. Totals
are never summed across currencies; `JPY` totals are integers, two-decimal currencies
are rendered with two decimals. The report shows the totals as decimal strings formatted
with the currency's minor-unit digits.

The source side is computed from the validated Pydantic records and the curated side
from the Polars frame, so the control crosses the contract boundary and would detect a
transformation that dropped, duplicated or altered a row.

## Report structure

`ReconciliationReport.as_dict()` produces the JSON below (illustrative values):

```json
{
  "batch_id": "5cb20be7-e5dc-45f2-ab49-784c7b7c1be3",
  "source_records": 3045,
  "accepted_records": 2925,
  "rejected_records": 90,
  "conflict_records": 30,
  "duplicate_records": 30,
  "exact_duplicates": 20,
  "idempotent_duplicates": 10,
  "cross_batch_duplicates": 0,
  "curated_records": 2925,
  "accounted_records": 3045,
  "difference": 0,
  "balanced": true,
  "currency_totals": {
    "GBP": {
      "source": "139353.33",
      "curated": "139353.33",
      "difference": "0.00",
      "source_count": 1322,
      "curated_count": 1322,
      "balanced": true
    },
    "JPY": {
      "source": "1839551",
      "curated": "1839551",
      "difference": "0",
      "source_count": 145,
      "curated_count": 145,
      "balanced": true
    }
  },
  "rule_counts": {
    "DQ003_INVALID_AMOUNT": 14,
    "DQ006_DUPLICATE_TRANSACTION": 30
  },
  "notes": []
}
```

`balanced` is true only when the row-count control, the curated-equals-accepted
control and every currency control hold. `notes` lists the failing controls in words.

## Failure semantics

* An unbalanced report raises `ReconciliationError` inside the `RECONCILIATION` stage.
  The orchestrator records the report on the batch (`reconciliation_balanced = false`,
  `reconciliation_report`), sets the status to `FAILED` and stores the error message.
  No quarantine file, curated partition, registry row or lineage record is written for
  that attempt.
* A post-write mismatch raises `OutputWriteError` in `CURATED_WRITE`; the batch is
  `FAILED`. The next attempt deletes the partitions of that batch id before writing.
* Reconciliation runs on every attempt of a resumed batch.

## Where reports live

* `<DATA_ROOT>/reports/batch_id=<batch_id>/reconciliation.json`, written atomically
  (temp file then rename) by `reconciliation/reports.py`;
* `pipeline_batches.reconciliation_report` (JSON / JSONB) and
  `pipeline_batches.reconciliation_balanced` in the metadata database;
* a `lineage_records` row of kind `RECONCILIATION_REPORT` pointing at the file.

## `txn-pipeline reconcile --batch-id <id>`

Prints the stored report (plain text or `--json`), then re-counts the curated rows on
disk for that batch and compares them with `curated_records` in the report. Exit code
0 means the stored report is balanced **and** the rows on disk still match; exit code 1
means the batch has no report (it failed earlier), the report is unbalanced, or the
files on disk no longer match the report.
