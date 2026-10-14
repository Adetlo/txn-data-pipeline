# History reconstruction

This repository was originally developed over an extended period beginning in 2024 and was later
consolidated and published to GitHub. The Git history was reconstructed from the final project
structure and known development phases. Historical commit dates represent approximate development
phases rather than forensic original commit timestamps.

## Reconstructed phases

| Period | Phase | What it added |
|---|---|---|
| May–Jun 2024 | Project setup, contracts, first readers | Package skeleton, exceptions, settings, enums and rule catalogue, country reference data, `TransactionRecord` with Decimal money and UTC timestamp policy, Pandera frame schema, result value objects, CSV and JSON Lines readers |
| Jul–Aug 2024 | Validation and transformation | Schema validation stage, Polars enrichment expressions, curated frame builder |
| Aug 2024 | Multi-format ingestion | Parquet reader, source discovery (SHA-256, size, format), format dispatch |
| Nov 2024 | Data quality and first tests | Quality rules with ERROR/WARNING severity, first unit tests |
| Mar–Apr 2025 | Deduplication and batch processing | Duplicate and conflict detection, stage checkpoints, batch result objects, pipeline stages, idempotent orchestrator |
| Jun–Jul 2025 | Reconciliation, lineage, observability | Row-count and per-currency controls, reports, structlog logging, metrics, lineage records |
| Aug 2025 | Curated outputs | Idempotent Parquet writers for curated and quarantine data |
| Sep 2025 | Analytics | DuckDB star schema DDL, warehouse builder, bundled queries |
| Oct 2025 | Metadata store | SQLAlchemy models, engine helpers, Alembic environment, repository |
| Nov 2025 | CLI and synthetic data | Seeded generator, corebank-ledger adapter, Typer CLI |
| Feb–Mar 2026 | Test consolidation | Shared fixtures, remaining unit tests, property, integration (SQLite + PostgreSQL) and end-to-end tests |
| May 2026 | Containers and CI | Dockerfile, entrypoint, compose stack, first GitHub Actions workflow |
| Sep–Oct 2026 | Documentation | Architecture, operations, lineage, reconciliation, warehouse model and security docs; examples; benchmark script; expanded README |
| 13–14 Oct 2026 | Publication | Dated ADRs, data contract, quality and performance notes, initial Alembic migration, sample fixtures, community health files, CI refresh with `uv.lock`, final README, this note |

## What is and is not authentic

* **File contents are authentic.** Every source, test, SQL, configuration and documentation file is
  committed in exactly the state that was published to GitHub in October 2026. No source or test
  file was rewritten for the reconstruction.
* **Commit dates, messages and grouping are reconstructed.** They were derived from the project's
  structure and the known order in which capabilities were developed. The exact original dates are
  unknown. Days and times within each phase were chosen to be plausible, not recovered.
* **The repository was published on GitHub in October 2026.** The pre-reconstruction history
  consisted of 13 commits all dated 2026-10-13, created when the project was consolidated for
  publication.
* **Intermediate versions** exist only for `pyproject.toml` (eight versions whose dependencies and
  tool configuration grow as features arrive), `README.md` (three versions), `LICENSE` (copyright
  year), `Dockerfile` (before and after the lockfile) and `.github/workflows/ci.yml` (first version
  with older action versions and an unlocked `uv sync`). These intermediates were written for the
  reconstruction and are consistent with the files present at each commit.

### Known anachronisms

* Several source and test files use `2026-10-13`-style example dates as fixture values and are
  committed with their feature phase: `src/txn_pipeline/cli/main.py`, `tests/conftest.py`,
  `tests/integration/test_pipeline_sqlite.py`, `tests/unit/test_contract.py`,
  `tests/unit/test_quality.py`, `tests/unit/test_support_modules.py`,
  `tests/unit/test_timestamps_normalize_countries.py`, `tests/unit/test_transformation.py`.
* Files whose content carries October 2026 dates (the seven ADRs, `docs/DATA_CONTRACT.md`,
  `docs/DATA_QUALITY.md`, `docs/PERFORMANCE.md`, the sample fixtures, the example records, the
  initial Alembic migration with its `Create Date`, `uv.lock` and the final `README.md`) are
  committed on 2026-10-13. The ADR acceptance dates reflect when the decisions were written up,
  not when they were taken.
* The initial Alembic migration file is committed in the publication phase, so commits between
  October 2025 and October 2026 contain the Alembic environment and the migration helpers but no
  revision file; the tests that run migrations would not have passed at those commits.
* Because source files are committed once at their final content, some early commits import
  modules that arrive later (for example the stage functions and orchestrator of April 2025 import
  the reconciliation, metadata, observability and warehouse packages added between June and
  October 2025; the first unit tests of November 2024 import fixtures from `tests/conftest.py`,
  committed in February 2026).
* The published `pyproject.toml` raises dependency lower bounds (for example `polars>=1.10`,
  `pydantic>=2.8`, `pandera>=0.21`, `pyarrow>=17`) that postdate the early phases; the intermediate
  manifests use bounds that were available at each commit date.
* The pipeline cross-references the sibling portfolio projects `corebank-ledger` and
  `fraud-detection-ml` in the final README and in `adapters/corebank.py` (committed November 2025).

## Archived original history

The pre-reconstruction history (13 commits dated 2026-10-13) is preserved in an archive
branch/tag of the published repository so that the original consolidation commits remain
inspectable.
