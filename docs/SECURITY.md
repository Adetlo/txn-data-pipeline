# Security notes

This repository is an educational implementation. It processes **synthetic data only**
and must not be presented as, or used as, a production-grade financial system.

## What the data is

* Every identifier is generated (`ACC-SYN-`, `CP-SYN-`, `TXN-SYN-`) and every amount,
  date and reference is random. No real customer, account, employer or production data
  exists anywhere in the repository, the samples or the generator.
* The datasets contain no credentials, keys or tokens. `.env.example` holds fictional
  local values; `.env` is git-ignored.
* The metadata database stores counts, hashes, paths and quarantined raw rows. It never
  stores the source files themselves.

## Quarantine is still data

Rejected rows are kept verbatim (as JSON) in the quarantine Parquet files and the
`quarantine_records` table so they can be reviewed. Here they are synthetic, but in a
real system the quarantine area holds exactly the records that failed validation, which
can include mistyped or misrouted personal data. It must be classified, access
controlled, encrypted and retained with the same care as the curated data, if not more.

## What a production deployment would need in addition

None of the following is implemented here, and the code does not claim otherwise:

* encryption at rest for Parquet, DuckDB, SQLite and PostgreSQL storage, and TLS for
  every database connection;
* authentication and role-based access control for the CLI, the metadata database and
  the output locations; the `actor` column records a context label, not an identity;
* secret management (vault or cloud KMS) instead of environment variables;
* retention and deletion policies for curated, quarantine and audit data;
* data classification and handling rules for PII and financial data, including
  masking in logs (this pipeline already avoids logging full records, but that is a
  convention, not an enforced control);
* tamper-evident audit logs, backups and restore drills;
* network isolation, dependency pinning with provenance checks and image signing for
  the container.

## Dependency hygiene in this repository

CI runs `pip-audit` on the locked dependencies and `bandit` on `src/`. The container
runs as a non-root user. That is a baseline for an open-source project, not a
security certification.

## Reporting

See [SECURITY.md](../SECURITY.md) at the repository root.
