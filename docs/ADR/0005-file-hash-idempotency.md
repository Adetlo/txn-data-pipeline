# ADR 0005: Content-addressed batches and a transaction registry

## Status

Accepted (2026-10-13)

## Context

Processing the same file twice must not duplicate curated records, a replaced file
with the same name must be distinguishable from the original, and a failed batch must
be restartable without corrupting earlier output.

## Decision

* A batch is identified by the pair (`source_name`, SHA-256 of the file content). The
  filename is recorded for humans but is not part of the identity.
* Same name and hash with a `COMPLETED` batch: the run is a no-op and returns the
  previous result. With a `FAILED`, `PARTIAL`, `RUNNING` or `DISCOVERED` batch: that
  batch is resumed under the same `batch_id` with `attempt + 1`. `--force` re-runs a
  completed batch as a new attempt and rewrites identical outputs.
* Same filename, different hash: a **new** batch (a new version of the file). The
  number of earlier versions is logged.
* Cross-batch record idempotency uses `transaction_registry`
  (`transaction_id -> payload_hash, batch_id`). An id already curated with the same
  payload hash is a cross-batch duplicate; with a different hash it is a conflict and
  the new record is quarantined. The first curated version wins. Registry rows of the
  batch being resumed are ignored during deduplication.
* Write stages are idempotent: deterministic paths, temp-file-and-rename, deletion of
  the batch's own earlier partitions, upsert on the registry, full warehouse rebuild.

## Consequences

* Re-runs are safe at every point of failure; the invariant "one curated row per
  `transaction_id`" holds across the whole curated dataset.
* Corrections to already curated transactions cannot be applied by resubmitting a
  changed row; they would surface as conflicts. A correction workflow (reversal
  records, or an explicit supersede command) is a future extension.
* The registry grows with the curated dataset (one small row per transaction).
