# ADR 0006: Quarantine invalid records

## Status

Accepted (2026-10-13)

## Context

Source files contain malformed rows. Failing the whole batch on the first bad row makes
the pipeline unusable; dropping bad rows silently makes it untrustworthy. Operators
need to see what was rejected, why, and prove that every source row was accounted for.

## Decision

* Record-level problems are values, not exceptions: a `Rejection` carries the record
  number, the raw row, the primary rule id, every failing rule id, the stage and a
  reason. Exceptions are reserved for systemic failures.
* Rules have stable identifiers and a severity. `ERROR` rejects; `WARNING` accepts and
  annotates the curated row (`dq_warnings`, `dq_warning_count`). `FAIL_ON_WARNINGS`
  can tighten this per deployment.
* Conflicting duplicates are quarantined under `DQ006_DUPLICATE_TRANSACTION`; every
  intra-batch version is quarantined because the pipeline cannot choose.
* Quarantine is written as Parquet per batch and as `quarantine_records` rows, with
  the raw row preserved as JSON. Per-rule tallies go to `quality_results`.
* Reconciliation enforces `source = accepted + rejected + duplicates`, so quarantine
  counts are part of the batch's proof of completeness.

## Consequences

* A batch with bad rows still completes, and the bad rows are reviewable with
  `txn-pipeline quarantine`.
* Quarantine contains the most suspicious data in the system and needs the same
  protection as curated data in a real deployment (see `docs/SECURITY.md`).
* Rule ids become a public interface: they are never renumbered or reused.
