# ADR 0004: PostgreSQL for pipeline metadata, SQLite as fallback

## Status

Accepted (2026-10-13)

## Context

Batch state, checkpoints, quality results, quarantine records, lineage and the
transaction registry need transactional storage with constraints and indexes. The
project must also stay runnable on a laptop with nothing installed, and tests should
not require a database server.

## Decision

* PostgreSQL is the target metadata store (Docker Compose service, CI service
  container, `JSONB` for report and raw-record columns, `ON CONFLICT` upserts for the
  registry).
* SQLite is a supported fallback selected purely by `METADATA_DATABASE_URL`
  (default `sqlite:///./data/metadata/pipeline.db`). The CLI applies migrations
  automatically for SQLite and requires `db-upgrade` for PostgreSQL.
* One Alembic migration serves both dialects: `sa.Uuid`, `sa.DateTime(timezone=True)`,
  `sa.JSON().with_variant(JSONB, "postgresql")`, check constraints expressed in
  portable SQL, `render_as_batch` for SQLite. No `NUMERIC` columns: amounts in
  metadata are decimal strings inside JSON.
* Large raw files are never stored in the database; only quarantined rows (as JSON),
  hashes, paths and counts.

## Consequences

* Zero-infrastructure local runs and fast unit tests on SQLite; realistic integration
  and e2e tests on PostgreSQL (embedded `pgserver` locally, service container in CI).
* The repository has one dialect switch (registry upsert); everything else is portable.
* SQLite is single-process; it is documented as unsuitable for shared or concurrent use.
* Dialect-neutral schemas forgo some PostgreSQL features (partial indexes, enums).
