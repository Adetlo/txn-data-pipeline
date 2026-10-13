# Contributing

Thanks for taking a look. This is a portfolio project, but it is run like a real one:
every change goes through the same checks CI runs.

## Setup

Requirements: Python 3.12 and [uv](https://docs.astral.sh/uv/).

```bash
git clone https://github.com/Adetlo/txn-data-pipeline.git
cd txn-data-pipeline
uv sync                      # creates .venv with runtime + dev dependencies
uv run txn-pipeline --help
cp .env.example .env         # optional; defaults use SQLite under ./data
```

## Tests

```bash
uv run pytest                          # unit, integration, property and e2e tests
uv run pytest --cov                    # with coverage (CI requires >= 85 %)
uv run pytest -m "not postgres"        # skip PostgreSQL-backed tests
uv run pytest tests/property           # Hypothesis properties only
```

Tests marked `postgres` need a PostgreSQL database. Set `TEST_DATABASE_URL`
(for example `postgresql+psycopg://user:pass@localhost:5432/txn_test`) or install
nothing and let the `pgserver` dev dependency start an embedded PostgreSQL in a
temporary directory. Migrations are applied by the test fixtures, so the Alembic
migration is exercised on every run. Everything else runs on SQLite in a temp dir.

## Quality checks

```bash
uv run ruff check .
uv run ruff format --check .
uv run mypy
uv run bandit -c pyproject.toml -r src -q
uv export --frozen --no-emit-project --no-hashes --format requirements-txt -o /tmp/req.txt
uv run pip-audit -r /tmp/req.txt --strict
```

`mypy` runs in strict mode on `src/`. Keep type ignores to a minimum and comment them.

## Adding a data-quality rule

1. Add the identifier constant and a `RuleDefinition` (severity, stage, description)
   to `src/txn_pipeline/contracts/rules.py`. Never reuse or renumber an existing id.
2. Implement it:
   * a single-field check belongs in a `TransactionRecord` validator in
     `contracts/transaction.py`, raising `ValueError("<RULE_ID>: message")`;
   * a cross-field or threshold check belongs in `validation/quality.py` as a function
     `(record, QualityConfig) -> Finding | None`, appended to `QUALITY_RULES`. Add any
     threshold to `Settings` and to `Settings.behavioural_config()` so it is part of
     the batch `config_hash`.
3. Add unit tests (trigger and non-trigger cases, correct rule id and severity) and,
   if the generator should produce the bad case, an entry in `INVALID_KINDS` in
   `synthetic/generator.py`.
4. Document the rule in `docs/DATA_QUALITY.md`.

## Adding a source format

1. Create `src/txn_pipeline/ingestion/<format>_reader.py` with a function
   `read_<format>(path: Path) -> Iterator[RawRecord]`. Yield 1-based record numbers
   and `sha256_record(data)` as the raw hash; wrap file-level errors in
   `SourceFileError`, and turn row-level parse problems into records that will fail
   with `SCH007_UNPARSEABLE_RECORD` rather than raising.
2. Add the format to `SourceFormat` in `contracts/enums.py`, its extensions to
   `_EXTENSIONS` in `ingestion/discovery.py` and the reader to `READERS` in
   `ingestion/readers.py`.
3. Add a writer in `synthetic/generator.py` (`write_rows`) so demos and tests can
   produce the format, and an integration test asserting it yields the same accepted
   / rejected / duplicate counts as CSV for the same rows.
4. Mention the format in `docs/DATA_CONTRACT.md`.

## Adding an adapter

Adapters map a foreign export shape onto the canonical input dict before schema
validation. Add a module under `src/txn_pipeline/adapters/`, register it in `ADAPTERS`
in `adapters/__init__.py`, add an example file under `examples/` and a unit test, and
document the field mapping in `docs/DATA_CONTRACT.md`.

## Commit style

Conventional commits: `feat:`, `fix:`, `test:`, `docs:`, `ci:`, `chore:`, `refactor:`.
One logical change per commit; keep generated data out of commits (`data/` is
git-ignored except for the small samples).

## Pull requests

* CI must be green (quality, integration, security, docker jobs).
* New behaviour comes with tests and, where it changes a contract, rule or table, with
  the matching doc update (`docs/` or an ADR under `docs/ADR/`).
* Synthetic data only. Never add real identifiers, hostnames, credentials or
  institution-specific formats.
