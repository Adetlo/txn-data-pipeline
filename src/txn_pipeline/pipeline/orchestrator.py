"""The batch orchestrator: sequencing, checkpoints, status transitions, failure handling.

State machine (docs/ARCHITECTURE.md)::

    DISCOVERED -> RUNNING -> VALIDATED -> COMPLETED
                     |           |     \\-> PARTIAL  (outputs committed, warehouse refresh failed)
                     \\-----------+------> FAILED   (systemic error or unbalanced reconciliation)

Idempotency (ADR 0005):

* same source name + same SHA-256 + a COMPLETED batch -> no-op, previous result returned;
* same hash + FAILED / PARTIAL / RUNNING batch -> that batch is *resumed* (new attempt);
* same filename + different hash -> a **new** batch (content-addressed versioning).
"""

from __future__ import annotations

import uuid
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

from txn_pipeline import PIPELINE_VERSION
from txn_pipeline.config import Settings
from txn_pipeline.contracts.enums import (
    RESTARTABLE_STATUSES,
    SCHEMA_VERSION,
    BatchStatus,
    CheckpointStatus,
    Stage,
)
from txn_pipeline.exceptions import PipelineError, ReconciliationError
from txn_pipeline.ingestion.discovery import SourceFile, discover_file
from txn_pipeline.ingestion.readers import read_records
from txn_pipeline.metadata.database import build_engine
from txn_pipeline.metadata.models import PipelineBatchModel
from txn_pipeline.metadata.repository import MetadataRepository
from txn_pipeline.observability import logging as events
from txn_pipeline.observability.logging import bind_context, clear_context, get_logger
from txn_pipeline.observability.metrics import MetricsCollector
from txn_pipeline.pipeline import stages
from txn_pipeline.pipeline.result import BatchCounts, BatchResult, ValidationSummary
from txn_pipeline.pipeline.stages import STAGE_FUNCTIONS, Adapter, BatchContext
from txn_pipeline.validation.duplicates import deduplicate
from txn_pipeline.validation.quality import QualityConfig, apply_quality_rules
from txn_pipeline.validation.results import RawRecord
from txn_pipeline.validation.schema import validate_schema

Hook = Callable[[BatchContext], None]


class PipelineOrchestrator:
    def __init__(
        self,
        settings: Settings,
        *,
        repo: MetadataRepository | None = None,
        before_stage: Mapping[Stage, Hook] | None = None,
        after_stage: Mapping[Stage, Hook] | None = None,
    ) -> None:
        """``before_stage`` / ``after_stage`` hooks exist for failure-injection tests."""
        self.settings = settings
        self._repo = repo
        self._before = dict(before_stage or {})
        self._after = dict(after_stage or {})
        self.log = get_logger("txn_pipeline.orchestrator")

    # --- public API ------------------------------------------------------------------------
    def run(
        self,
        input_path: Path,
        *,
        source_name: str = "SYNTHETIC_CORE",
        fmt: str | None = None,
        adapter: Adapter | None = None,
        force: bool = False,
        build_warehouse: bool | None = None,
    ) -> BatchResult:
        self.settings.ensure_directories()
        correlation_id = uuid.uuid4().hex
        metrics = MetricsCollector()
        repo = self._repo or MetadataRepository(build_engine(self.settings.metadata_database_url))
        owns_repo = self._repo is None
        clear_context()
        bind_context(correlation_id=correlation_id, source=source_name)
        try:
            with metrics.timed(Stage.DISCOVERY.value):
                source = discover_file(input_path, fmt=fmt)
            bind_context(source_filename=source.filename)
            batch, resumed, previous = self._register(
                repo, source, source_name, correlation_id, force
            )
            if previous is not None:
                return previous
            ctx = BatchContext(
                settings=self.settings,
                repo=repo,
                metrics=metrics,
                source=source,
                source_name=source_name,
                batch=batch,
                correlation_id=correlation_id,
                adapter=adapter,
            )
            bind_context(batch_id=ctx.batch_id)
            return self._execute(ctx, resumed=resumed, build_warehouse=build_warehouse)
        finally:
            clear_context()
            if owns_repo:
                repo.close()

    def validate_only(
        self,
        input_path: Path,
        *,
        fmt: str | None = None,
        adapter: Adapter | None = None,
        sample: int = 10,
    ) -> ValidationSummary:
        """Schema + quality + intra-batch dedupe without touching metadata or outputs."""
        source = discover_file(input_path, fmt=fmt)
        raw: list[RawRecord] = []
        for record in read_records(source.path, source.format):
            if adapter is None:
                raw.append(record)
            else:
                raw.append(RawRecord(record.record_number, adapter(record.data), record.raw_hash))
        valid, rejected, counts = validate_schema(
            raw, source_timezone=self.settings.source_timezone
        )
        cfg = QualityConfig(
            min_booking_date=self.settings.min_booking_date,
            max_booking_days_in_future=self.settings.max_booking_days_in_future,
            value_date_tolerance_days=self.settings.value_date_tolerance_days,
            timestamp_booking_tolerance_days=self.settings.timestamp_booking_tolerance_days,
        )
        accepted, rejected_q, counts_q = apply_quality_rules(
            valid, cfg, fail_on_warnings=self.settings.fail_on_warnings
        )
        counts.merge(counts_q)
        dedup = deduplicate(accepted)
        counts.merge(dedup.counts)
        all_rejected = sorted(
            rejected + rejected_q + dedup.conflicts, key=lambda r: r.record_number
        )
        batch_counts = BatchCounts(
            source_records=len(raw),
            accepted_records=len(dedup.accepted),
            rejected_records=len(all_rejected),
            conflict_records=len(dedup.conflicts),
            duplicate_records=len(dedup.duplicates),
            exact_duplicates=dedup.exact_duplicate_count,
            idempotent_duplicates=dedup.idempotent_duplicate_count,
            warning_records=sum(1 for r in dedup.accepted if r.warnings),
            curated_records=len(dedup.accepted),
        )
        samples = [
            {
                "record_number": r.record_number,
                "transaction_id": r.transaction_id,
                "rule_id": r.rule_id,
                "stage": r.stage.value,
                "reason": r.reason,
            }
            for r in all_rejected[:sample]
        ]
        return ValidationSummary(
            source.filename, source.sha256, batch_counts, counts.as_dict(), samples
        )

    # --- registration ------------------------------------------------------------------------
    def _register(
        self,
        repo: MetadataRepository,
        source: SourceFile,
        source_name: str,
        correlation_id: str,
        force: bool,
    ) -> tuple[PipelineBatchModel, bool, BatchResult | None]:
        source_model = repo.register_source_file(source, source_name)
        existing = repo.find_batches_for_hash(source_name, source.sha256)
        completed = [b for b in existing if b.status == BatchStatus.COMPLETED.value]
        if completed and not force:
            batch = completed[0]
            repo.commit()
            self.log.info(
                events.BATCH_SKIPPED_IDEMPOTENT,
                batch_id=str(batch.id),
                stage=Stage.REGISTRATION.value,
                source_hash=source.sha256,
            )
            return batch, False, self._result_from_batch(batch, source, source_name, skipped=True)

        resumable = [b for b in existing if BatchStatus(b.status) in RESTARTABLE_STATUSES]
        if resumable or (completed and force):
            batch = (resumable or completed)[0]
            repo.begin_attempt(batch, correlation_id)
            repo.set_status(batch, BatchStatus.RUNNING)
            repo.commit()
            self.log.info(
                events.BATCH_RESUMED,
                batch_id=str(batch.id),
                stage=Stage.REGISTRATION.value,
                attempt=batch.attempt,
                previous_status=existing[0].status,
            )
            return batch, True, None

        batch = repo.create_batch(
            source_file=source_model,
            source_name=source_name,
            actor=self.settings.actor.value,
            pipeline_version=PIPELINE_VERSION,
            config_hash=self.settings.config_hash(),
            schema_version=SCHEMA_VERSION,
            correlation_id=correlation_id,
        )
        older = repo.previous_versions_of_filename(
            source_name, source.filename, exclude_hash=source.sha256
        )
        repo.set_status(batch, BatchStatus.RUNNING)
        repo.commit()
        self.log.info(
            events.BATCH_REGISTERED,
            batch_id=str(batch.id),
            stage=Stage.REGISTRATION.value,
            source_hash=source.sha256,
            size_bytes=source.size_bytes,
            format=source.format.value,
            previous_versions=len(older),
        )
        return batch, False, None

    # --- execution --------------------------------------------------------------------------
    def _execute(
        self, ctx: BatchContext, *, resumed: bool, build_warehouse: bool | None
    ) -> BatchResult:
        repo, batch, metrics = ctx.repo, ctx.batch, ctx.metrics
        do_warehouse = (
            self.settings.build_warehouse_on_run if build_warehouse is None else build_warehouse
        )
        self.log.info(events.INGESTION_STARTED, stage=Stage.INGESTION.value)
        try:
            for stage, fn in STAGE_FUNCTIONS:
                self._run_stage(ctx, stage, fn)
                if stage is Stage.DEDUPLICATION:
                    self._persist_validation(ctx)
                    repo.set_status(batch, BatchStatus.VALIDATED)
                    repo.commit()
                elif stage is Stage.REGISTRY_COMMIT:
                    self._persist_outputs(ctx)
                    repo.commit()
        except Exception as exc:
            return self._fail(ctx, exc, resumed)

        # Curated data, quarantine, registry and report are committed: the batch is COMPLETED
        # from a data point of view before the (derived) warehouse refresh runs.
        status = BatchStatus.COMPLETED
        repo.set_status(batch, status)
        repo.commit()
        if do_warehouse:
            try:
                self._run_stage(ctx, Stage.WAREHOUSE, stages.warehouse_build)
                repo.commit()
            except Exception as exc:
                repo.rollback()
                status = BatchStatus.PARTIAL
                ctx.notes.append(f"warehouse build failed: {exc}")
                batch = repo.require_batch(batch.id)
                ctx.batch = batch
                batch.error_message = str(exc)[:4000]
                self.log.error(events.BATCH_PARTIAL, stage=Stage.WAREHOUSE.value, error=str(exc))
        else:
            repo.checkpoint(batch, Stage.WAREHOUSE, CheckpointStatus.SKIPPED)

        metrics.finish()
        batch.metrics = metrics.summary()
        repo.set_status(batch, status)
        repo.checkpoint(
            batch, Stage.COMPLETION, CheckpointStatus.COMPLETED, details={"status": status.value}
        )
        repo.commit()
        result = self._result_from_context(ctx, status, resumed)
        self.log.info(
            events.BATCH_COMPLETED if status is BatchStatus.COMPLETED else events.BATCH_PARTIAL,
            stage=Stage.COMPLETION.value,
            status=status.value,
            **{k: v for k, v in metrics.summary().items() if k != "stage_durations_ms"},
        )
        return result

    def _run_stage(
        self, ctx: BatchContext, stage: Stage, fn: Callable[[BatchContext], None]
    ) -> None:
        repo = ctx.repo
        repo.checkpoint(ctx.batch, stage, CheckpointStatus.STARTED)
        self.log.debug(events.STAGE_STARTED, stage=stage.value)
        try:
            with ctx.metrics.timed(stage.value):
                if (before := self._before.get(stage)) is not None:
                    before(ctx)
                fn(ctx)
                if (after := self._after.get(stage)) is not None:
                    after(ctx)
        except Exception as exc:
            # Persist the FAILED checkpoint immediately so the outer handler's rollback of
            # uncommitted stage work cannot discard it.
            repo.rollback()
            ctx.batch = repo.require_batch(ctx.batch.id)
            repo.checkpoint(
                ctx.batch, stage, CheckpointStatus.FAILED, details={"error": str(exc)[:1000]}
            )
            repo.commit()
            raise
        details = self._stage_details(ctx, stage)
        repo.checkpoint(ctx.batch, stage, CheckpointStatus.COMPLETED, details=details)
        self.log.info(
            _STAGE_EVENT.get(stage, events.STAGE_COMPLETED),
            stage=stage.value,
            duration_ms=ctx.metrics.stage_durations_ms.get(stage.value),
            **details,
        )

    @staticmethod
    def _stage_details(ctx: BatchContext, stage: Stage) -> dict[str, Any]:
        if stage is Stage.INGESTION:
            return {"record_count": len(ctx.raw_records)}
        if stage is Stage.SCHEMA_VALIDATION:
            return {"valid": len(ctx.schema_valid), "rejected": len(ctx.rejections)}
        if stage is Stage.QUALITY_VALIDATION:
            return {"accepted": len(ctx.quality_accepted), "rejected_total": len(ctx.rejections)}
        if stage is Stage.DEDUPLICATION:
            return {
                "accepted": len(ctx.accepted),
                "duplicates": len(ctx.dedup.duplicates),
                "conflicts": ctx.conflict_count,
            }
        if stage is Stage.TRANSFORMATION:
            return {"curated_rows": ctx.curated.height}
        if stage is Stage.RECONCILIATION and ctx.reconciliation is not None:
            return {"balanced": ctx.reconciliation.balanced}
        if stage is Stage.QUARANTINE_WRITE:
            return {"quarantined": len(ctx.all_rejections), "path": str(ctx.quarantine_file)}
        if stage is Stage.CURATED_WRITE:
            return {"files": len(ctx.curated_files), "rows": sum(n for _, n in ctx.curated_files)}
        if stage is Stage.REGISTRY_COMMIT:
            return {"registered": len(ctx.accepted)}
        if stage is Stage.WAREHOUSE:
            return dict(ctx.warehouse_counts)
        return {}

    def _persist_validation(self, ctx: BatchContext) -> None:
        batch = ctx.batch
        batch.record_count = len(ctx.raw_records)
        batch.accepted_count = len(ctx.accepted)
        batch.rejected_count = len(ctx.all_rejections)
        batch.conflict_count = ctx.conflict_count
        batch.duplicate_count = len(ctx.dedup.duplicates)
        batch.quarantine_count = len(ctx.all_rejections)
        batch.warning_count = ctx.warning_count
        ctx.repo.replace_quality_results(batch, ctx.rule_counts.as_dict())

    def _persist_outputs(self, ctx: BatchContext) -> None:
        assert ctx.reconciliation is not None  # noqa: S101
        ctx.batch.reconciliation_balanced = ctx.reconciliation.balanced
        ctx.batch.reconciliation_report = ctx.reconciliation.as_dict()

    def _fail(self, ctx: BatchContext, exc: Exception, resumed: bool) -> BatchResult:
        repo = ctx.repo
        repo.rollback()
        ctx.metrics.finish()
        batch = repo.require_batch(ctx.batch.id)  # re-fetch after rollback
        ctx.batch = batch
        if ctx.reconciliation is not None:
            batch.reconciliation_balanced = ctx.reconciliation.balanced
            batch.reconciliation_report = ctx.reconciliation.as_dict()
        batch.metrics = ctx.metrics.summary()
        kind = "reconciliation" if isinstance(exc, ReconciliationError) else "stage"
        message = f"{type(exc).__name__}: {exc}"
        repo.set_status(batch, BatchStatus.FAILED, error=message)
        repo.checkpoint(
            batch, Stage.COMPLETION, CheckpointStatus.FAILED, details={"error": message[:1000]}
        )
        repo.commit()
        self.log.error(
            events.BATCH_FAILED, stage=Stage.COMPLETION.value, failure_kind=kind, error=message
        )
        result = self._result_from_context(ctx, BatchStatus.FAILED, resumed)
        result.error = message
        if not isinstance(exc, PipelineError | ValueError | OSError | KeyError):
            # Unexpected programming errors are still recorded, then surfaced to the caller.
            raise exc
        return result

    # --- results -------------------------------------------------------------------------------
    def _result_from_context(
        self, ctx: BatchContext, status: BatchStatus, resumed: bool
    ) -> BatchResult:
        counts = BatchCounts(
            source_records=len(ctx.raw_records),
            accepted_records=len(ctx.accepted),
            rejected_records=len(ctx.all_rejections),
            conflict_records=ctx.conflict_count,
            duplicate_records=len(ctx.dedup.duplicates),
            exact_duplicates=ctx.dedup.exact_duplicate_count,
            idempotent_duplicates=ctx.dedup.idempotent_duplicate_count,
            cross_batch_duplicates=ctx.dedup.cross_batch_duplicate_count,
            warning_records=ctx.warning_count,
            curated_records=sum(n for _, n in ctx.curated_files)
            if ctx.curated_files
            else ctx.curated.height,
        )
        return BatchResult(
            batch_id=ctx.batch_id,
            status=status,
            source_filename=ctx.source.filename,
            source_hash=ctx.source.sha256,
            source_size_bytes=ctx.source.size_bytes,
            source_format=ctx.source.format.value,
            source_name=ctx.source_name,
            attempt=ctx.batch.attempt,
            correlation_id=ctx.correlation_id,
            resumed=resumed,
            counts=counts,
            rule_counts=ctx.rule_counts.as_dict(),
            reconciliation=ctx.reconciliation.as_dict() if ctx.reconciliation else None,
            metrics=ctx.metrics.summary(),
            curated_files=[str(p) for p, _ in ctx.curated_files],
            quarantine_file=str(ctx.quarantine_file) if ctx.quarantine_file else None,
            report_file=str(ctx.report_file) if ctx.report_file else None,
            warehouse_tables=dict(ctx.warehouse_counts),
            notes=list(ctx.notes),
        )

    @staticmethod
    def _result_from_batch(
        batch: PipelineBatchModel, source: SourceFile, source_name: str, *, skipped: bool
    ) -> BatchResult:
        report = batch.reconciliation_report or {}
        counts = BatchCounts(
            source_records=batch.record_count,
            accepted_records=batch.accepted_count,
            rejected_records=batch.rejected_count,
            conflict_records=batch.conflict_count,
            duplicate_records=batch.duplicate_count,
            exact_duplicates=int(report.get("exact_duplicates", 0)),
            idempotent_duplicates=int(report.get("idempotent_duplicates", 0)),
            cross_batch_duplicates=int(report.get("cross_batch_duplicates", 0)),
            warning_records=batch.warning_count,
            curated_records=int(report.get("curated_records", batch.accepted_count)),
        )
        return BatchResult(
            batch_id=str(batch.id),
            status=BatchStatus(batch.status),
            source_filename=source.filename,
            source_hash=source.sha256,
            source_size_bytes=source.size_bytes,
            source_format=source.format.value,
            source_name=source_name,
            attempt=batch.attempt,
            correlation_id=batch.correlation_id,
            skipped=skipped,
            counts=counts,
            rule_counts=dict(report.get("rule_counts", {})),
            reconciliation=report or None,
            metrics=dict(batch.metrics or {}),
            notes=["source file already processed by a COMPLETED batch; nothing to do"],
        )


_STAGE_EVENT: dict[Stage, str] = {
    Stage.INGESTION: events.INGESTION_COMPLETED,
    Stage.SCHEMA_VALIDATION: events.SCHEMA_VALIDATION_COMPLETED,
    Stage.QUALITY_VALIDATION: events.QUALITY_CHECK_COMPLETED,
    Stage.DEDUPLICATION: events.DEDUPLICATION_COMPLETED,
    Stage.TRANSFORMATION: events.TRANSFORMATION_COMPLETED,
    Stage.RECONCILIATION: events.RECONCILIATION_COMPLETED,
    Stage.QUARANTINE_WRITE: events.QUARANTINE_WRITTEN,
    Stage.CURATED_WRITE: events.CURATED_WRITTEN,
    Stage.REGISTRY_COMMIT: events.REGISTRY_COMMITTED,
    Stage.WAREHOUSE: events.WAREHOUSE_BUILT,
}
