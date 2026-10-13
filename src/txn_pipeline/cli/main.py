"""``txn-pipeline`` command-line interface."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Annotated, Any

import typer
from rich.console import Console
from rich.table import Table

from txn_pipeline import PIPELINE_VERSION
from txn_pipeline.adapters import ADAPTERS, get_adapter
from txn_pipeline.config import Settings
from txn_pipeline.contracts.enums import BatchStatus, SourceFormat
from txn_pipeline.contracts.rules import RULES
from txn_pipeline.exceptions import PipelineError
from txn_pipeline.metadata.database import build_engine, current_revision, run_migrations
from txn_pipeline.metadata.repository import MetadataRepository
from txn_pipeline.observability.logging import configure_logging
from txn_pipeline.pipeline.orchestrator import PipelineOrchestrator
from txn_pipeline.pipeline.result import BatchResult
from txn_pipeline.reconciliation.reports import render_text
from txn_pipeline.synthetic.generator import GeneratorConfig, generate, write_rows
from txn_pipeline.warehouse.model import WarehouseBuilder, available_queries
from txn_pipeline.warehouse.writer import curated_row_count, decode_raw_record, read_quarantine

app = typer.Typer(
    name="txn-pipeline",
    help="Production-style educational financial transaction data pipeline (synthetic data only).",
    no_args_is_help=True,
    rich_markup_mode="rich",
    pretty_exceptions_show_locals=False,
)
console = Console()
err_console = Console(stderr=True)

EXIT_OK = 0
EXIT_FAILED = 1
EXIT_PARTIAL = 2


class State:
    settings: Settings


state = State()


def _version(value: bool) -> None:
    if value:
        console.print(f"txn-pipeline {PIPELINE_VERSION}")
        raise typer.Exit()


@app.callback()
def main(
    data_root: Annotated[
        Path | None,
        typer.Option(
            "--data-root",
            envvar="DATA_ROOT",
            help="Root for incoming/curated/quarantine/warehouse data.",
        ),
    ] = None,
    database_url: Annotated[
        str | None,
        typer.Option(
            "--database-url",
            envvar="METADATA_DATABASE_URL",
            help="SQLAlchemy URL for pipeline metadata (PostgreSQL or SQLite).",
        ),
    ] = None,
    log_level: Annotated[str | None, typer.Option("--log-level", envvar="LOG_LEVEL")] = None,
    log_format: Annotated[
        str | None, typer.Option("--log-format", envvar="LOG_FORMAT", help="json or console")
    ] = None,
    version: Annotated[
        bool,
        typer.Option("--version", callback=_version, is_eager=True, help="Show version and exit."),
    ] = False,
) -> None:
    overrides: dict[str, Any] = {}
    if data_root is not None:
        overrides["data_root"] = data_root
    if database_url is not None:
        overrides["metadata_database_url"] = database_url
    if log_level is not None:
        overrides["log_level"] = log_level
    if log_format is not None:
        overrides["log_format"] = log_format
    state.settings = Settings(**overrides)
    configure_logging(state.settings.log_level, state.settings.log_format)


# --- helpers ---------------------------------------------------------------------------------
def _repo() -> MetadataRepository:
    return MetadataRepository(build_engine(state.settings.metadata_database_url))


def _ensure_schema() -> None:
    """Apply migrations automatically for SQLite (zero-infra mode); require them for PostgreSQL."""
    url = state.settings.metadata_database_url
    if state.settings.uses_sqlite:
        run_migrations(url)
        return
    try:
        rev = current_revision(url)
    except Exception as exc:
        err_console.print(f"[red]cannot reach metadata database:[/red] {exc}")
        raise typer.Exit(EXIT_FAILED) from None
    if rev is None:
        err_console.print(
            "[red]metadata schema is not initialised; run `txn-pipeline db-upgrade` first[/red]"
        )
        raise typer.Exit(EXIT_FAILED)


def _print_result(result: BatchResult, *, as_json: bool) -> None:
    if as_json:
        console.print_json(json.dumps(result.as_dict(), default=str))
        return
    colour = {"COMPLETED": "green", "PARTIAL": "yellow", "FAILED": "red"}.get(
        result.status.value, "white"
    )
    c = result.counts
    table = Table(title=f"Batch {result.batch_id}", show_header=False, box=None, pad_edge=False)
    table.add_row(
        "Status",
        f"[{colour}]{result.status.value}[/{colour}]"
        + ("  (idempotent skip)" if result.skipped else "")
        + ("  (resumed)" if result.resumed else ""),
    )
    table.add_row(
        "Source",
        f"{result.source_filename}  sha256={result.source_hash[:12]}…  {result.source_size_bytes:,} bytes  ({result.source_format})",
    )
    table.add_row("Source records", f"{c.source_records:,}")
    table.add_row("Accepted unique", f"{c.accepted_records:,}")
    table.add_row(
        "Rejected", f"{c.rejected_records:,}  (of which conflicts {c.conflict_records:,})"
    )
    table.add_row(
        "Duplicates",
        f"{c.duplicate_records:,}  (exact {c.exact_duplicates:,}, idempotent {c.idempotent_duplicates:,}, cross-batch {c.cross_batch_duplicates:,})",
    )
    table.add_row("Warnings", f"{c.warning_records:,} accepted records carry a WARNING finding")
    table.add_row("Curated rows", f"{c.curated_records:,}")
    table.add_row("Quality pass rate", f"{c.quality_pass_rate:.2%}")
    if result.reconciliation is not None:
        verdict = "[green]PASS[/green]" if result.reconciliation["balanced"] else "[red]FAIL[/red]"
        table.add_row("Reconciliation", verdict)
    if result.metrics:
        table.add_row(
            "Throughput",
            f"{result.metrics.get('records_per_second', 0):,.0f} records/s over {result.metrics.get('duration_seconds', 0)} s",
        )
    if result.warehouse_tables:
        table.add_row(
            "Warehouse", ", ".join(f"{k}={v:,}" for k, v in result.warehouse_tables.items())
        )
    if result.error:
        table.add_row("Error", f"[red]{result.error}[/red]")
    for note in result.notes:
        table.add_row("Note", note)
    console.print(table)
    if result.rule_counts:
        rules = Table(title="Quality findings by rule", pad_edge=False)
        rules.add_column("Rule")
        rules.add_column("Severity")
        rules.add_column("Stage")
        rules.add_column("Records", justify="right")
        for rule_id, n in sorted(result.rule_counts.items()):
            d = RULES[rule_id]
            rules.add_row(rule_id, d.severity.value, d.stage.value, f"{n:,}")
        console.print(rules)
    if result.reconciliation is not None and result.reconciliation.get("currency_totals"):
        totals = Table(title="Amount reconciliation by currency", pad_edge=False)
        for col in ("Currency", "Source (accepted)", "Curated", "Difference", "Rows"):
            totals.add_column(col, justify="right" if col != "Currency" else "left")
        for code, t in result.reconciliation["currency_totals"].items():
            totals.add_row(
                code, t["source"], t["curated"], t["difference"], f"{t['curated_count']:,}"
            )
        console.print(totals)


def _exit_code(result: BatchResult) -> int:
    if result.status is BatchStatus.COMPLETED:
        return EXIT_OK
    if result.status is BatchStatus.PARTIAL:
        return EXIT_PARTIAL
    return EXIT_FAILED


# --- commands ----------------------------------------------------------------------------------
@app.command("generate-demo-data")
def generate_demo_data(
    records: Annotated[int, typer.Option("--records", "-n", min=0)] = 10_000,
    seed: Annotated[int, typer.Option("--seed")] = 42,
    output: Annotated[
        Path | None,
        typer.Option(
            "--output", "-o", help="Output file; extension decides format unless --format is given."
        ),
    ] = None,
    fmt: Annotated[SourceFormat, typer.Option("--format", "-f")] = SourceFormat.CSV,
    invalid_rate: Annotated[float, typer.Option("--invalid-rate", min=0.0, max=1.0)] = 0.02,
    duplicate_rate: Annotated[float, typer.Option("--duplicate-rate", min=0.0, max=1.0)] = 0.01,
    conflict_rate: Annotated[
        float,
        typer.Option(
            "--conflict-rate",
            min=0.0,
            max=1.0,
            help="Share of records that get a second, conflicting version.",
        ),
    ] = 0.005,
    warning_rate: Annotated[float, typer.Option("--warning-rate", min=0.0, max=1.0)] = 0.01,
    anchor_date: Annotated[
        str,
        typer.Option(
            "--anchor-date", help="Latest booking date (ISO). Fixed by default for reproducibility."
        ),
    ] = "2026-10-13",
) -> None:
    """Generate reproducible synthetic transactions with configurable bad data."""
    cfg = GeneratorConfig(
        records=records,
        seed=seed,
        invalid_rate=invalid_rate,
        duplicate_rate=duplicate_rate,
        conflict_rate=conflict_rate,
        warning_rate=warning_rate,
        anchor_date=date.fromisoformat(anchor_date),
    )
    rows, summary = generate(cfg)
    target = output or (state.settings.incoming_root / f"synthetic_transactions_{seed}.{fmt.value}")
    write_rows(rows, target, fmt)
    console.print(f"[green]wrote[/green] {len(rows):,} rows to {target}")
    console.print_json(json.dumps(summary.as_dict()))


@app.command("run")
def run(
    input_path: Annotated[
        Path, typer.Option("--input", "-i", exists=True, dir_okay=False, readable=True)
    ],
    source: Annotated[
        str, typer.Option("--source", "-s", help="Logical source name recorded on the batch.")
    ] = "SYNTHETIC_CORE",
    fmt: Annotated[
        str | None,
        typer.Option(
            "--format", "-f", help="csv | jsonl | parquet (inferred from extension by default)"
        ),
    ] = None,
    adapter: Annotated[
        str | None,
        typer.Option(
            "--adapter",
            help=f"Map a differently shaped export first. One of: {', '.join(sorted(ADAPTERS))}",
        ),
    ] = None,
    force: Annotated[
        bool,
        typer.Option(
            "--force",
            help="Re-run even if this exact file already COMPLETED (output is rewritten, not duplicated).",
        ),
    ] = False,
    warehouse: Annotated[
        bool,
        typer.Option(
            "--warehouse/--no-warehouse", help="Rebuild the DuckDB star schema after the batch."
        ),
    ] = True,
    as_json: Annotated[bool, typer.Option("--json", help="Print the full result as JSON.")] = False,
) -> None:
    """Run the full pipeline for one source file and register a batch."""
    _ensure_schema()
    try:
        adapter_fn = get_adapter(adapter)
    except KeyError as exc:
        err_console.print(f"[red]{exc}[/red]")
        raise typer.Exit(EXIT_FAILED) from None
    try:
        result = PipelineOrchestrator(state.settings).run(
            input_path,
            source_name=source,
            fmt=fmt,
            adapter=adapter_fn,
            force=force,
            build_warehouse=warehouse,
        )
    except PipelineError as exc:
        err_console.print(f"[red]pipeline error:[/red] {exc}")
        raise typer.Exit(EXIT_FAILED) from None
    _print_result(result, as_json=as_json)
    raise typer.Exit(_exit_code(result))


@app.command("validate")
def validate(
    input_path: Annotated[
        Path, typer.Option("--input", "-i", exists=True, dir_okay=False, readable=True)
    ],
    fmt: Annotated[str | None, typer.Option("--format", "-f")] = None,
    adapter: Annotated[str | None, typer.Option("--adapter")] = None,
    strict: Annotated[
        bool, typer.Option("--strict", help="Exit 1 if any record would be rejected.")
    ] = False,
    sample: Annotated[int, typer.Option("--sample", help="Show the first N rejections.")] = 10,
    as_json: Annotated[bool, typer.Option("--json")] = False,
) -> None:
    """Validate a file (schema, quality rules, duplicates) without registering a batch or writing outputs."""
    try:
        summary = PipelineOrchestrator(state.settings).validate_only(
            input_path, fmt=fmt, adapter=get_adapter(adapter), sample=sample
        )
    except (PipelineError, KeyError) as exc:
        err_console.print(f"[red]{exc}[/red]")
        raise typer.Exit(EXIT_FAILED) from None
    if as_json:
        console.print_json(json.dumps(summary.as_dict()))
    else:
        c = summary.counts
        console.print(f"[bold]{summary.source_filename}[/bold]  sha256={summary.source_hash}")
        console.print(
            f"records={c.source_records:,}  would accept={c.accepted_records:,}  reject={c.rejected_records:,} "
            f"(conflicts {c.conflict_records:,})  duplicates={c.duplicate_records:,}  warnings={c.warning_records:,}"
        )
        if summary.rule_counts:
            table = Table(title="Findings by rule", pad_edge=False)
            table.add_column("Rule")
            table.add_column("Severity")
            table.add_column("Records", justify="right")
            for rule_id, n in sorted(summary.rule_counts.items()):
                table.add_row(rule_id, RULES[rule_id].severity.value, f"{n:,}")
            console.print(table)
        if summary.sample_rejections:
            table = Table(
                title=f"First {len(summary.sample_rejections)} rejections", pad_edge=False
            )
            table.add_column("#", justify="right")
            table.add_column("transaction_id")
            table.add_column("Rule")
            table.add_column("Reason")
            for r in summary.sample_rejections:
                table.add_row(
                    str(r["record_number"]),
                    str(r["transaction_id"] or ""),
                    r["rule_id"],
                    r["reason"][:110],
                )
            console.print(table)
    if strict and summary.counts.rejected_records:
        raise typer.Exit(EXIT_FAILED)


@app.command("reconcile")
def reconcile_cmd(
    batch_id: Annotated[str, typer.Option("--batch-id", "-b")],
    as_json: Annotated[bool, typer.Option("--json")] = False,
) -> None:
    """Show the stored reconciliation report and re-verify curated rows on disk against it."""
    _ensure_schema()
    with _repo() as repo:
        batch = repo.get_batch(batch_id)
        if batch is None:
            err_console.print(f"[red]batch {batch_id} not found[/red]")
            raise typer.Exit(EXIT_FAILED)
        report = batch.reconciliation_report
    if not report:
        err_console.print(
            "[yellow]no reconciliation report stored (batch failed before reconciliation?)[/yellow]"
        )
        raise typer.Exit(EXIT_FAILED)
    on_disk = curated_row_count(state.settings.curated_root, batch_id)
    disk_ok = on_disk == int(report["curated_records"])
    if as_json:
        console.print_json(
            json.dumps({**report, "curated_rows_on_disk": on_disk, "disk_matches_report": disk_ok})
        )
    else:
        console.print(render_text(report))
        console.print(
            f"Curated on disk:  {on_disk:,}  ({'matches' if disk_ok else 'MISMATCH vs'} report)"
        )
    raise typer.Exit(EXIT_OK if report["balanced"] and disk_ok else EXIT_FAILED)


@app.command("batch-status")
def batch_status(
    batch_id: Annotated[str, typer.Option("--batch-id", "-b")],
    as_json: Annotated[bool, typer.Option("--json")] = False,
) -> None:
    """Show a batch, its stage checkpoints and lineage records."""
    _ensure_schema()
    with _repo() as repo:
        batch = repo.get_batch(batch_id)
        if batch is None:
            err_console.print(f"[red]batch {batch_id} not found[/red]")
            raise typer.Exit(EXIT_FAILED)
        checkpoints = repo.checkpoints(batch_id)
        lineage = repo.lineage_for_batch(batch_id)
        payload: dict[str, Any] = {
            "batch_id": str(batch.id),
            "status": batch.status,
            "attempt": batch.attempt,
            "actor": batch.actor,
            "source_name": batch.source_name,
            "source_filename": batch.source_filename,
            "source_hash": batch.source_hash,
            "pipeline_version": batch.pipeline_version,
            "config_hash": batch.config_hash,
            "schema_version": batch.schema_version,
            "correlation_id": batch.correlation_id,
            "started_at": batch.started_at.isoformat(),
            "completed_at": batch.completed_at.isoformat() if batch.completed_at else None,
            "counts": {
                "record_count": batch.record_count,
                "accepted_count": batch.accepted_count,
                "rejected_count": batch.rejected_count,
                "duplicate_count": batch.duplicate_count,
                "conflict_count": batch.conflict_count,
                "warning_count": batch.warning_count,
            },
            "reconciliation_balanced": batch.reconciliation_balanced,
            "error_message": batch.error_message,
            "metrics": batch.metrics,
            "checkpoints": [
                {
                    "stage": c.stage.value,
                    "status": c.status.value,
                    "attempt": c.attempt,
                    "completed_at": c.completed_at.isoformat() if c.completed_at else None,
                    "details": c.details,
                }
                for c in checkpoints
            ],
            "lineage": [
                {
                    "output_kind": item.output_kind,
                    "output_path": item.output_path,
                    "row_count": item.row_count,
                }
                for item in lineage
            ],
        }
    if as_json:
        console.print_json(json.dumps(payload, default=str))
        return
    colour = {"COMPLETED": "green", "PARTIAL": "yellow", "FAILED": "red"}.get(
        payload["status"], "white"
    )
    console.print(
        f"[bold]Batch {payload['batch_id']}[/bold]  status=[{colour}]{payload['status']}[/{colour}]  attempt={payload['attempt']}  actor={payload['actor']}"
    )
    console.print(
        f"source={payload['source_name']} file={payload['source_filename']} sha256={payload['source_hash']}"
    )
    console.print(
        f"pipeline={payload['pipeline_version']} config_hash={payload['config_hash'][:12]}… schema={payload['schema_version']}"
    )
    console.print(f"started={payload['started_at']} completed={payload['completed_at']}")
    console.print(f"counts={payload['counts']} balanced={payload['reconciliation_balanced']}")
    if payload["error_message"]:
        console.print(f"[red]error:[/red] {payload['error_message']}")
    table = Table(title="Checkpoints", pad_edge=False)
    table.add_column("Stage")
    table.add_column("Status")
    table.add_column("Attempt", justify="right")
    table.add_column("Details")
    for c in payload["checkpoints"]:
        table.add_row(
            c["stage"],
            c["status"],
            str(c["attempt"]),
            json.dumps(c["details"], default=str)[:90] if c["details"] else "",
        )
    console.print(table)
    if payload["lineage"]:
        table = Table(title="Lineage", pad_edge=False)
        table.add_column("Output")
        table.add_column("Rows", justify="right")
        table.add_column("Path")
        for item in payload["lineage"]:
            table.add_row(item["output_kind"], f"{item['row_count']:,}", item["output_path"])
        console.print(table)


@app.command("batches")
def batches(limit: Annotated[int, typer.Option("--limit", min=1)] = 20) -> None:
    """List recent batches."""
    _ensure_schema()
    with _repo() as repo:
        rows = repo.list_batches(limit)
    table = Table(title="Recent batches", pad_edge=False)
    for col in (
        "Batch",
        "Status",
        "Source file",
        "Records",
        "Accepted",
        "Rejected",
        "Dup",
        "Balanced",
        "Started",
    ):
        table.add_column(col)
    for b in rows:
        table.add_row(
            str(b.id),
            b.status,
            b.source_filename,
            f"{b.record_count:,}",
            f"{b.accepted_count:,}",
            f"{b.rejected_count:,}",
            f"{b.duplicate_count:,}",
            str(b.reconciliation_balanced),
            b.started_at.strftime("%Y-%m-%d %H:%M:%S"),
        )
    console.print(table)


@app.command("quarantine")
def quarantine(
    batch_id: Annotated[str, typer.Option("--batch-id", "-b")],
    limit: Annotated[int, typer.Option("--limit", min=1)] = 20,
    rule: Annotated[str | None, typer.Option("--rule", help="Filter by rule id.")] = None,
    show_raw: Annotated[
        bool, typer.Option("--raw", help="Print the original raw record for each row.")
    ] = False,
) -> None:
    """Review quarantined (rejected) records of a batch from the Parquet quarantine area."""
    frame = read_quarantine(state.settings.quarantine_root, batch_id)
    if rule:
        frame = frame.filter(frame["rule_id"] == rule)
    console.print(f"{frame.height:,} quarantined records" + (f" for rule {rule}" if rule else ""))
    if frame.is_empty():
        return
    by_rule = frame.group_by("rule_id").len().sort("len", descending=True)
    table = Table(title="By rule", pad_edge=False)
    table.add_column("Rule")
    table.add_column("Records", justify="right")
    for row in by_rule.iter_rows():
        table.add_row(str(row[0]), f"{row[1]:,}")
    console.print(table)
    table = Table(title=f"First {min(limit, frame.height)} records", pad_edge=False)
    table.add_column("#", justify="right")
    table.add_column("transaction_id")
    table.add_column("Stage")
    table.add_column("Rule")
    table.add_column("Reason")
    for rec in frame.head(limit).to_dicts():
        table.add_row(
            str(rec["record_number"]),
            str(rec["transaction_id"] or ""),
            rec["stage"],
            rec["rule_id"],
            rec["reason"][:100],
        )
        if show_raw:
            console.print(f"   raw: {json.dumps(decode_raw_record(rec['raw_record']))}")
    console.print(table)


@app.command("warehouse-build")
def warehouse_build() -> None:
    """Rebuild the DuckDB star schema from all curated Parquet files and batch metadata."""
    _ensure_schema()
    builder = WarehouseBuilder(
        state.settings.duckdb_path, state.settings.curated_root, state.settings.quarantine_root
    )
    with _repo() as repo:
        summaries = repo.batch_summaries()
    try:
        counts = builder.build(summaries)
    except PipelineError as exc:
        err_console.print(f"[red]{exc}[/red]")
        raise typer.Exit(EXIT_FAILED) from None
    console.print(f"[green]warehouse rebuilt[/green] at {state.settings.duckdb_path}")
    for table_name, n in counts.items():
        console.print(f"  {table_name:<22} {n:>10,}")


@app.command("query")
def query(
    name: Annotated[
        str | None, typer.Argument(help="Query name; omit to list the available queries.")
    ] = None,
    limit: Annotated[int, typer.Option("--limit", min=1)] = 20,
    as_json: Annotated[bool, typer.Option("--json")] = False,
) -> None:
    """Run one of the bundled DuckDB analytical queries against the warehouse."""
    queries = available_queries()
    if name is None:
        for q, sql in queries.items():
            console.print(f"[bold]{q}[/bold]: {sql.splitlines()[0].lstrip('- ').strip()}")
        return
    builder = WarehouseBuilder(
        state.settings.duckdb_path, state.settings.curated_root, state.settings.quarantine_root
    )
    if not state.settings.duckdb_path.exists():
        err_console.print("[red]warehouse not built yet; run `txn-pipeline warehouse-build`[/red]")
        raise typer.Exit(EXIT_FAILED)
    try:
        frame = builder.run_query(name, limit=limit)
    except KeyError as exc:
        err_console.print(f"[red]{exc}[/red]")
        raise typer.Exit(EXIT_FAILED) from None
    if as_json:
        console.print_json(json.dumps(frame.to_dicts(), default=str))
        return
    table = Table(title=name, pad_edge=False)
    for col in frame.columns:
        table.add_column(col)
    for row in frame.iter_rows():
        table.add_row(*[str(v) for v in row])
    console.print(table)


@app.command("db-upgrade")
def db_upgrade() -> None:
    """Apply Alembic migrations to the metadata database."""
    run_migrations(state.settings.metadata_database_url)
    console.print(
        f"[green]metadata schema at revision {current_revision(state.settings.metadata_database_url)}[/green]"
    )


@app.command("demo")
def demo(
    records: Annotated[int, typer.Option("--records", "-n", min=1)] = 10_000,
    seed: Annotated[int, typer.Option("--seed")] = 42,
    fmt: Annotated[SourceFormat, typer.Option("--format", "-f")] = SourceFormat.CSV,
) -> None:
    """End-to-end demo: generate -> run -> quarantine -> reconcile -> warehouse -> queries -> reprocess."""
    _ensure_schema()
    settings = state.settings
    settings.ensure_directories()
    cfg = GeneratorConfig(records=records, seed=seed)
    rows, summary = generate(cfg)
    target = settings.incoming_root / f"demo_transactions_{seed}.{fmt.value}"
    write_rows(rows, target, fmt)
    console.rule("[bold]1. Synthetic data")
    console.print(
        f"generated {summary.total_rows:,} rows -> {target}\n"
        f"  valid {summary.valid_records:,}, invalid {summary.invalid_records:,}, "
        f"duplicates {summary.duplicate_records:,}, conflicting versions {summary.conflict_records:,} "
        f"(each conflict quarantines both versions), warning-only {summary.warning_records:,}"
    )
    console.rule("[bold]2. Pipeline run")
    orchestrator = PipelineOrchestrator(settings)
    result = orchestrator.run(target, source_name="SYNTHETIC_CORE")
    _print_result(result, as_json=False)
    if not result.succeeded:
        raise typer.Exit(EXIT_FAILED)
    console.rule("[bold]3. Quarantine review")
    q = read_quarantine(settings.quarantine_root, result.batch_id)
    console.print(f"{q.height:,} rejected records in {result.quarantine_file}")
    for row in q.head(3).iter_rows(named=True):
        console.print(f"  #{row['record_number']}: {row['rule_id']} — {row['reason'][:90]}")
    console.rule("[bold]4. DuckDB analytics")
    builder = WarehouseBuilder(
        settings.duckdb_path, settings.curated_root, settings.quarantine_root
    )
    for name in (
        "daily_value_by_currency",
        "top_transaction_types",
        "channel_distribution",
        "batch_quality_trend",
    ):
        frame = builder.run_query(name, limit=8)
        table = Table(title=name, pad_edge=False)
        for col in frame.columns:
            table.add_column(col)
        for r in frame.iter_rows():
            table.add_row(*[str(v) for v in r])
        console.print(table)
    console.rule("[bold]5. Reprocessing the same file")
    before = curated_row_count(settings.curated_root, result.batch_id)
    again = orchestrator.run(target, source_name="SYNTHETIC_CORE")
    after = curated_row_count(settings.curated_root, result.batch_id)
    console.print(
        f"second run: status={again.status.value} skipped={again.skipped}; curated rows before={before:,} after={after:,} "
        f"-> {'[green]no duplicate load[/green]' if before == after else '[red]DUPLICATED[/red]'}"
    )
    console.rule("[bold]Summary")
    c = result.counts
    console.print(
        f"Batch {result.batch_id}\n"
        f"Source records {c.source_records:,} | accepted unique {c.accepted_records:,} | rejected {c.rejected_records:,} "
        f"(conflicts {c.conflict_records:,}) | duplicates {c.duplicate_records:,}\n"
        f"Reconciliation {'PASS' if result.balanced else 'FAIL'} | curated {c.curated_records:,} rows | "
        f"quality pass rate {c.quality_pass_rate:.2%}"
    )


if __name__ == "__main__":  # pragma: no cover
    app()
