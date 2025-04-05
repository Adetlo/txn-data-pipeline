"""Stage implementations. Each stage reads and writes a :class:`BatchContext`.

Stages are plain functions so they can be unit-tested and composed; the orchestrator owns
sequencing, checkpointing, status transitions and error handling.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import polars as pl

from txn_pipeline import PIPELINE_VERSION
from txn_pipeline.config import Settings
from txn_pipeline.contracts.enums import Stage
from txn_pipeline.exceptions import OutputWriteError, ReconciliationError
from txn_pipeline.ingestion.discovery import SourceFile
from txn_pipeline.ingestion.readers import read_records
from txn_pipeline.metadata.models import PipelineBatchModel
from txn_pipeline.metadata.repository import MetadataRepository
from txn_pipeline.observability.lineage import LineageEntry, OutputKind
from txn_pipeline.observability.metrics import MetricsCollector
from txn_pipeline.reconciliation.batch import reconcile
from txn_pipeline.reconciliation.controls import ReconciliationReport
from txn_pipeline.reconciliation.reports import write_report
from txn_pipeline.transformation.frame import build_curated_frame, empty_curated_frame
from txn_pipeline.transformation.timestamps import utc_now
from txn_pipeline.validation.duplicates import DedupResult, deduplicate
from txn_pipeline.validation.quality import QualityConfig, apply_quality_rules
from txn_pipeline.validation.results import RawRecord, Rejection, RuleCounts, ValidatedRecord
from txn_pipeline.validation.schema import validate_schema
from txn_pipeline.warehouse.model import WarehouseBuilder
from txn_pipeline.warehouse.writer import (
    curated_row_count,
    quarantine_frame,
    write_curated,
    write_quarantine,
)

Adapter = Callable[[dict[str, Any]], dict[str, Any]]


@dataclass(slots=True)
class BatchContext:
    settings: Settings
    repo: MetadataRepository
    metrics: MetricsCollector
    source: SourceFile
    source_name: str
    batch: PipelineBatchModel
    correlation_id: str
    adapter: Adapter | None = None
    ingested_at: datetime = field(default_factory=utc_now)

    raw_records: list[RawRecord] = field(default_factory=list)
    schema_valid: list[ValidatedRecord] = field(default_factory=list)
    quality_accepted: list[ValidatedRecord] = field(default_factory=list)
    rejections: list[Rejection] = field(default_factory=list)
    rule_counts: RuleCounts = field(default_factory=RuleCounts)
    dedup: DedupResult = field(default_factory=DedupResult)
    curated: pl.DataFrame = field(default_factory=empty_curated_frame)
    reconciliation: ReconciliationReport | None = None
    curated_files: list[tuple[Path, int]] = field(default_factory=list)
    quarantine_file: Path | None = None
    report_file: Path | None = None
    warehouse_counts: dict[str, int] = field(default_factory=dict)
    notes: list[str] = field(default_factory=list)

    @property
    def batch_id(self) -> str:
        return str(self.batch.id)

    @property
    def accepted(self) -> list[ValidatedRecord]:
        return self.dedup.accepted

    @property
    def conflict_count(self) -> int:
        return len(self.dedup.conflicts)

    @property
    def all_rejections(self) -> list[Rejection]:
        return sorted(self.rejections + self.dedup.conflicts, key=lambda r: r.record_number)

    @property
    def warning_count(self) -> int:
        return sum(1 for r in self.accepted if r.warnings)


# --- stages ---------------------------------------------------------------------------------
def ingest(ctx: BatchContext) -> None:
    records: list[RawRecord] = []
    for raw in read_records(ctx.source.path, ctx.source.format):
        if ctx.adapter is None:
            records.append(raw)
        else:
            records.append(RawRecord(raw.record_number, ctx.adapter(raw.data), raw.raw_hash))
    ctx.raw_records = records
    ctx.metrics.set("records_processed", len(records))


def schema_validation(ctx: BatchContext) -> None:
    valid, rejected, counts = validate_schema(
        ctx.raw_records, source_timezone=ctx.settings.source_timezone
    )
    ctx.schema_valid = valid
    ctx.rejections.extend(rejected)
    ctx.rule_counts.merge(counts)


def quality_validation(ctx: BatchContext) -> None:
    cfg = QualityConfig(
        min_booking_date=ctx.settings.min_booking_date,
        max_booking_days_in_future=ctx.settings.max_booking_days_in_future,
        value_date_tolerance_days=ctx.settings.value_date_tolerance_days,
        timestamp_booking_tolerance_days=ctx.settings.timestamp_booking_tolerance_days,
    )
    accepted, rejected, counts = apply_quality_rules(
        ctx.schema_valid, cfg, fail_on_warnings=ctx.settings.fail_on_warnings
    )
    ctx.quality_accepted = accepted
    ctx.rejections.extend(rejected)
    ctx.rule_counts.merge(counts)


def deduplication(ctx: BatchContext) -> None:
    ids = sorted({item.transaction_id for item in ctx.quality_accepted})
    registry = ctx.repo.registry_lookup(ids)
    ctx.dedup = deduplicate(ctx.quality_accepted, registry=registry, current_batch_id=ctx.batch_id)
    ctx.rule_counts.merge(ctx.dedup.counts)
    ctx.metrics.set("records_accepted", len(ctx.accepted))
    ctx.metrics.set("records_rejected", len(ctx.all_rejections))
    ctx.metrics.set("records_conflict", ctx.conflict_count)
    ctx.metrics.set("records_duplicate", len(ctx.dedup.duplicates))
    ctx.metrics.set("records_warning", ctx.warning_count)


def transformation(ctx: BatchContext) -> None:
    ctx.curated = build_curated_frame(
        ctx.accepted,
        ctx.dedup.payload_hashes,
        batch_id=ctx.batch_id,
        source_filename=ctx.source.filename,
        source_file_hash=ctx.source.sha256,
        ingested_at=ctx.ingested_at,
        pipeline_version=PIPELINE_VERSION,
    )


def reconciliation(ctx: BatchContext) -> None:
    report = reconcile(
        batch_id=ctx.batch_id,
        source_records=len(ctx.raw_records),
        accepted=ctx.accepted,
        rejected_count=len(ctx.all_rejections),
        conflict_count=ctx.conflict_count,
        exact_duplicates=ctx.dedup.exact_duplicate_count,
        idempotent_duplicates=ctx.dedup.idempotent_duplicate_count,
        cross_batch_duplicates=ctx.dedup.cross_batch_duplicate_count,
        curated=ctx.curated,
        rule_counts=ctx.rule_counts.as_dict(),
    )
    report.notes.extend(ctx.notes)
    ctx.reconciliation = report
    if not report.balanced:
        raise ReconciliationError("; ".join(report.notes) or "controls did not balance")


def quarantine_write(ctx: BatchContext) -> None:
    rejections = ctx.all_rejections
    frame = quarantine_frame(
        rejections,
        batch_id=ctx.batch_id,
        source_filename=ctx.source.filename,
        source_file_hash=ctx.source.sha256,
        quarantined_at=ctx.ingested_at,
        pipeline_version=PIPELINE_VERSION,
    )
    ctx.quarantine_file = write_quarantine(frame, ctx.settings.quarantine_root, ctx.batch_id)
    ctx.repo.replace_quarantine(ctx.batch, rejections, ctx.ingested_at)


def curated_write(ctx: BatchContext) -> None:
    ctx.curated_files = write_curated(ctx.curated, ctx.settings.curated_root, ctx.batch_id)
    on_disk = curated_row_count(ctx.settings.curated_root, ctx.batch_id)
    if on_disk != ctx.curated.height:
        raise OutputWriteError(
            "CURATED_WRITE",
            f"post-write control failed: {on_disk} rows on disk, {ctx.curated.height} expected",
        )


def registry_commit(ctx: BatchContext) -> None:
    assert ctx.reconciliation is not None  # noqa: S101 - sequencing invariant
    ctx.repo.registry_upsert(ctx.batch, ctx.accepted, ctx.dedup.payload_hashes, ctx.ingested_at)
    ctx.report_file = write_report(ctx.reconciliation, ctx.settings.reports_root)
    now = utc_now()
    entries = [
        LineageEntry(
            batch_id=ctx.batch_id,
            source_filename=ctx.source.filename,
            source_hash=ctx.source.sha256,
            output_kind=OutputKind.CURATED_PARTITION,
            output_path=str(path),
            row_count=rows,
            pipeline_version=PIPELINE_VERSION,
            created_at=now,
        )
        for path, rows in ctx.curated_files
    ]
    if ctx.quarantine_file is not None:
        entries.append(
            LineageEntry(
                ctx.batch_id,
                ctx.source.filename,
                ctx.source.sha256,
                OutputKind.QUARANTINE_FILE,
                str(ctx.quarantine_file),
                len(ctx.all_rejections),
                PIPELINE_VERSION,
                now,
            )
        )
    entries.append(
        LineageEntry(
            ctx.batch_id,
            ctx.source.filename,
            ctx.source.sha256,
            OutputKind.RECONCILIATION_REPORT,
            str(ctx.report_file),
            1,
            PIPELINE_VERSION,
            now,
        )
    )
    ctx.repo.record_lineage(ctx.batch, entries)


def warehouse_build(ctx: BatchContext) -> None:
    builder = WarehouseBuilder(
        ctx.settings.duckdb_path, ctx.settings.curated_root, ctx.settings.quarantine_root
    )
    ctx.warehouse_counts = builder.build(ctx.repo.batch_summaries())
    ctx.repo.record_lineage(
        ctx.batch,
        [
            LineageEntry(
                ctx.batch_id,
                ctx.source.filename,
                ctx.source.sha256,
                OutputKind.WAREHOUSE,
                str(ctx.settings.duckdb_path),
                ctx.warehouse_counts.get("fact_transactions", 0),
                PIPELINE_VERSION,
                utc_now(),
            )
        ],
    )


StageFn = Callable[[BatchContext], None]

#: Ordered stage table used by the orchestrator (stage enum -> implementation).
STAGE_FUNCTIONS: tuple[tuple[Stage, StageFn], ...] = (
    (Stage.INGESTION, ingest),
    (Stage.SCHEMA_VALIDATION, schema_validation),
    (Stage.QUALITY_VALIDATION, quality_validation),
    (Stage.DEDUPLICATION, deduplication),
    (Stage.TRANSFORMATION, transformation),
    (Stage.RECONCILIATION, reconciliation),
    (Stage.QUARANTINE_WRITE, quarantine_write),
    (Stage.CURATED_WRITE, curated_write),
    (Stage.REGISTRY_COMMIT, registry_commit),
)
