"""Repository over the metadata tables. One instance per pipeline run; explicit commits."""

from __future__ import annotations

import json
import uuid
from collections.abc import Iterable, Sequence
from datetime import datetime
from typing import Any, cast

from sqlalchemy import Engine, Table, delete, func, select
from sqlalchemy.dialects import postgresql, sqlite
from sqlalchemy.orm import Session

from txn_pipeline.contracts.enums import BatchStatus, CheckpointStatus, Stage
from txn_pipeline.contracts.rules import RULES
from txn_pipeline.exceptions import MetadataError
from txn_pipeline.ingestion.discovery import SourceFile
from txn_pipeline.ingestion.hashing import canonical_json
from txn_pipeline.metadata.checkpoints import Checkpoint
from txn_pipeline.metadata.database import build_session_factory
from txn_pipeline.metadata.models import (
    LineageRecordModel,
    PipelineBatchModel,
    PipelineCheckpointModel,
    QualityResultModel,
    QuarantineRecordModel,
    SourceFileModel,
    TransactionRegistryModel,
)
from txn_pipeline.observability.lineage import LineageEntry
from txn_pipeline.transformation.timestamps import utc_now
from txn_pipeline.validation.duplicates import RegistryEntry
from txn_pipeline.validation.results import Rejection, ValidatedRecord

_CHUNK = 5_000


def _uuid(value: str | uuid.UUID) -> uuid.UUID:
    if isinstance(value, uuid.UUID):
        return value
    try:
        return uuid.UUID(value)
    except ValueError as exc:
        raise MetadataError(f"not a valid batch id: {value!r}") from exc


class MetadataRepository:
    def __init__(self, engine: Engine) -> None:
        self._engine = engine
        self._factory = build_session_factory(engine)
        self._session: Session | None = None

    # --- session management --------------------------------------------------------
    @property
    def session(self) -> Session:
        if self._session is None:
            self._session = self._factory()
        return self._session

    @property
    def dialect(self) -> str:
        return self._engine.dialect.name

    def commit(self) -> None:
        if self._session is not None:
            self._session.commit()

    def rollback(self) -> None:
        if self._session is not None:
            self._session.rollback()

    def close(self) -> None:
        if self._session is not None:
            self._session.close()
            self._session = None

    def __enter__(self) -> MetadataRepository:
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        if exc_type is not None:
            self.rollback()
        self.close()

    # --- source files ----------------------------------------------------------------
    def register_source_file(self, source: SourceFile, source_name: str) -> SourceFileModel:
        existing = self.session.scalar(
            select(SourceFileModel).where(
                SourceFileModel.source_name == source_name,
                SourceFileModel.sha256 == source.sha256,
            )
        )
        if existing is not None:
            return existing
        model = SourceFileModel(
            source_name=source_name,
            filename=source.filename,
            file_path=str(source.path),
            sha256=source.sha256,
            size_bytes=source.size_bytes,
            format=source.format.value,
            discovered_at=source.discovered_at,
        )
        self.session.add(model)
        self.session.flush()
        return model

    def find_batches_for_hash(self, source_name: str, sha256: str) -> list[PipelineBatchModel]:
        rows = self.session.scalars(
            select(PipelineBatchModel)
            .where(
                PipelineBatchModel.source_name == source_name,
                PipelineBatchModel.source_hash == sha256,
            )
            .order_by(PipelineBatchModel.started_at.desc())
        )
        return list(rows)

    def previous_versions_of_filename(
        self, source_name: str, filename: str, *, exclude_hash: str
    ) -> list[SourceFileModel]:
        rows = self.session.scalars(
            select(SourceFileModel).where(
                SourceFileModel.source_name == source_name,
                SourceFileModel.filename == filename,
                SourceFileModel.sha256 != exclude_hash,
            )
        )
        return list(rows)

    # --- batches ---------------------------------------------------------------------
    def create_batch(
        self,
        *,
        source_file: SourceFileModel,
        source_name: str,
        actor: str,
        pipeline_version: str,
        config_hash: str,
        schema_version: str,
        correlation_id: str,
    ) -> PipelineBatchModel:
        batch = PipelineBatchModel(
            source_file_id=source_file.id,
            source_name=source_name,
            source_filename=source_file.filename,
            source_hash=source_file.sha256,
            status=BatchStatus.DISCOVERED.value,
            actor=actor,
            pipeline_version=pipeline_version,
            config_hash=config_hash,
            schema_version=schema_version,
            correlation_id=correlation_id,
            attempt=1,
            started_at=utc_now(),
        )
        self.session.add(batch)
        self.session.flush()
        return batch

    def get_batch(self, batch_id: str | uuid.UUID) -> PipelineBatchModel | None:
        return self.session.get(PipelineBatchModel, _uuid(batch_id))

    def require_batch(self, batch_id: str | uuid.UUID) -> PipelineBatchModel:
        batch = self.get_batch(batch_id)
        if batch is None:
            raise MetadataError(f"batch {batch_id} not found")
        return batch

    def list_batches(self, limit: int = 50) -> list[PipelineBatchModel]:
        rows = self.session.scalars(
            select(PipelineBatchModel).order_by(PipelineBatchModel.started_at.desc()).limit(limit)
        )
        return list(rows)

    def set_status(
        self, batch: PipelineBatchModel, status: BatchStatus, *, error: str | None = None
    ) -> None:
        batch.status = status.value
        if error is not None:
            batch.error_message = error[:4000]
        if status in {BatchStatus.COMPLETED, BatchStatus.FAILED, BatchStatus.PARTIAL}:
            batch.completed_at = utc_now()
        self.session.flush()

    def begin_attempt(self, batch: PipelineBatchModel, correlation_id: str) -> None:
        batch.attempt += 1
        batch.correlation_id = correlation_id
        batch.completed_at = None
        batch.error_message = None
        batch.reconciliation_balanced = None
        self.session.flush()

    # --- checkpoints -------------------------------------------------------------------
    def checkpoint(
        self,
        batch: PipelineBatchModel,
        stage: Stage,
        status: CheckpointStatus,
        *,
        details: dict[str, Any] | None = None,
        started_at: datetime | None = None,
    ) -> None:
        row = self.session.scalar(
            select(PipelineCheckpointModel).where(
                PipelineCheckpointModel.batch_id == batch.id,
                PipelineCheckpointModel.stage == stage.value,
            )
        )
        now = utc_now()
        if row is None:
            row = PipelineCheckpointModel(
                batch_id=batch.id,
                stage=stage.value,
                status=status.value,
                attempt=batch.attempt,
                started_at=started_at or now,
            )
            self.session.add(row)
        else:
            row.status = status.value
            row.attempt = batch.attempt
            if status is CheckpointStatus.STARTED:
                row.started_at = started_at or now
                row.completed_at = None
        if status is not CheckpointStatus.STARTED:
            row.completed_at = now
        if details is not None:
            row.details = details
        self.session.flush()

    def checkpoints(self, batch_id: str | uuid.UUID) -> list[Checkpoint]:
        rows = self.session.scalars(
            select(PipelineCheckpointModel)
            .where(PipelineCheckpointModel.batch_id == _uuid(batch_id))
            .order_by(PipelineCheckpointModel.started_at)
        )
        return [
            Checkpoint(
                stage=Stage(r.stage),
                status=CheckpointStatus(r.status),
                attempt=r.attempt,
                started_at=r.started_at,
                completed_at=r.completed_at,
                details=r.details,
            )
            for r in rows
        ]

    # --- quality results / quarantine ---------------------------------------------------
    def replace_quality_results(self, batch: PipelineBatchModel, counts: dict[str, int]) -> None:
        self.session.execute(
            delete(QualityResultModel).where(QualityResultModel.batch_id == batch.id)
        )
        now = utc_now()
        self.session.add_all(
            [
                QualityResultModel(
                    batch_id=batch.id,
                    rule_id=rule_id,
                    severity=RULES[rule_id].severity.value,
                    stage=RULES[rule_id].stage.value,
                    record_count=n,
                    recorded_at=now,
                )
                for rule_id, n in counts.items()
            ]
        )
        self.session.flush()

    def replace_quarantine(
        self, batch: PipelineBatchModel, rejections: Sequence[Rejection], quarantined_at: datetime
    ) -> None:
        self.session.execute(
            delete(QuarantineRecordModel).where(QuarantineRecordModel.batch_id == batch.id)
        )
        rows = [
            {
                "id": uuid.uuid4(),
                "batch_id": batch.id,
                "record_number": r.record_number,
                "transaction_id": (r.transaction_id or "")[:64] or None,
                "rule_id": r.rule_id,
                "all_rule_ids": ",".join(r.all_rule_ids)[:512],
                "severity": RULES[r.rule_id].severity.value,
                "reason": r.reason,
                "stage": r.stage.value,
                "raw_record": _jsonable(r.raw),
                "quarantined_at": quarantined_at,
            }
            for r in rejections
        ]
        for start in range(0, len(rows), _CHUNK):
            table = cast(Table, QuarantineRecordModel.__table__)
            self.session.execute(table.insert(), rows[start : start + _CHUNK])
        self.session.flush()

    def quarantine_for_batch(
        self, batch_id: str | uuid.UUID, limit: int = 100
    ) -> list[QuarantineRecordModel]:
        rows = self.session.scalars(
            select(QuarantineRecordModel)
            .where(QuarantineRecordModel.batch_id == _uuid(batch_id))
            .order_by(QuarantineRecordModel.record_number)
            .limit(limit)
        )
        return list(rows)

    def quality_results_for_batch(self, batch_id: str | uuid.UUID) -> dict[str, int]:
        rows = self.session.scalars(
            select(QualityResultModel).where(QualityResultModel.batch_id == _uuid(batch_id))
        )
        return {r.rule_id: r.record_count for r in rows}

    # --- lineage ------------------------------------------------------------------------
    def record_lineage(self, batch: PipelineBatchModel, entries: Iterable[LineageEntry]) -> None:
        for entry in entries:
            existing = self.session.scalar(
                select(LineageRecordModel).where(
                    LineageRecordModel.batch_id == batch.id,
                    LineageRecordModel.output_kind == entry.output_kind.value,
                    LineageRecordModel.output_path == entry.output_path,
                )
            )
            if existing is not None:
                existing.row_count = entry.row_count
                existing.created_at = entry.created_at
                continue
            self.session.add(
                LineageRecordModel(
                    batch_id=batch.id,
                    source_filename=entry.source_filename,
                    source_hash=entry.source_hash,
                    output_kind=entry.output_kind.value,
                    output_path=entry.output_path,
                    row_count=entry.row_count,
                    pipeline_version=entry.pipeline_version,
                    created_at=entry.created_at,
                )
            )
        self.session.flush()

    def lineage_for_batch(self, batch_id: str | uuid.UUID) -> list[LineageRecordModel]:
        rows = self.session.scalars(
            select(LineageRecordModel)
            .where(LineageRecordModel.batch_id == _uuid(batch_id))
            .order_by(LineageRecordModel.created_at)
        )
        return list(rows)

    # --- transaction registry (cross-batch idempotency) ---------------------------------
    def registry_lookup(self, transaction_ids: Sequence[str]) -> dict[str, RegistryEntry]:
        found: dict[str, RegistryEntry] = {}
        for start in range(0, len(transaction_ids), _CHUNK):
            chunk = transaction_ids[start : start + _CHUNK]
            rows = self.session.execute(
                select(
                    TransactionRegistryModel.transaction_id,
                    TransactionRegistryModel.payload_hash,
                    TransactionRegistryModel.batch_id,
                ).where(TransactionRegistryModel.transaction_id.in_(chunk))
            )
            for tx_id, payload_hash, batch_id in rows:
                found[tx_id] = RegistryEntry(payload_hash=payload_hash, batch_id=str(batch_id))
        return found

    def registry_upsert(
        self,
        batch: PipelineBatchModel,
        records: Sequence[ValidatedRecord],
        payload_hashes: dict[int, str],
        curated_at: datetime,
    ) -> int:
        """Insert registry rows; rows that already exist (same batch retry) are updated."""
        rows = [
            {
                "transaction_id": item.transaction_id,
                "source_system": item.record.source_system,
                "payload_hash": payload_hashes[item.record_number],
                "batch_id": batch.id,
                "curated_at": curated_at,
            }
            for item in records
        ]
        table = cast(Table, TransactionRegistryModel.__table__)
        for start in range(0, len(rows), _CHUNK):
            chunk = rows[start : start + _CHUNK]
            if self.dialect == "postgresql":
                stmt_pg = postgresql.insert(table).values(chunk)
                stmt_pg = stmt_pg.on_conflict_do_update(
                    index_elements=[table.c.transaction_id],
                    set_={
                        "payload_hash": stmt_pg.excluded.payload_hash,
                        "batch_id": stmt_pg.excluded.batch_id,
                        "curated_at": stmt_pg.excluded.curated_at,
                    },
                )
                self.session.execute(stmt_pg)
            else:
                stmt_lite = sqlite.insert(table).values(chunk)
                stmt_lite = stmt_lite.on_conflict_do_update(
                    index_elements=[table.c.transaction_id],
                    set_={
                        "payload_hash": stmt_lite.excluded.payload_hash,
                        "batch_id": stmt_lite.excluded.batch_id,
                        "curated_at": stmt_lite.excluded.curated_at,
                    },
                )
                self.session.execute(stmt_lite)
        self.session.flush()
        return len(rows)

    def registry_count(self) -> int:
        return int(
            self.session.scalar(select(func.count()).select_from(TransactionRegistryModel)) or 0
        )

    def registry_count_for_batch(self, batch_id: str | uuid.UUID) -> int:
        return int(
            self.session.scalar(
                select(func.count())
                .select_from(TransactionRegistryModel)
                .where(TransactionRegistryModel.batch_id == _uuid(batch_id))
            )
            or 0
        )

    # --- analytics support ------------------------------------------------------------------
    def batch_summaries(self) -> list[dict[str, Any]]:
        """Flat batch rows for the warehouse's batch-quality trend table."""
        rows = self.session.scalars(
            select(PipelineBatchModel).order_by(PipelineBatchModel.started_at)
        )
        return [
            {
                "batch_id": str(b.id),
                "source_name": b.source_name,
                "source_filename": b.source_filename,
                "source_hash": b.source_hash,
                "status": b.status,
                "actor": b.actor,
                "pipeline_version": b.pipeline_version,
                "attempt": b.attempt,
                "started_at": b.started_at,
                "completed_at": b.completed_at,
                "record_count": b.record_count,
                "accepted_count": b.accepted_count,
                "rejected_count": b.rejected_count,
                "duplicate_count": b.duplicate_count,
                "conflict_count": b.conflict_count,
                "warning_count": b.warning_count,
                "reconciliation_balanced": b.reconciliation_balanced,
            }
            for b in rows
        ]


def _jsonable(data: dict[str, Any]) -> dict[str, Any]:
    """Make a raw row JSON-serialisable (Parquet rows carry Decimal / date objects)."""
    return dict(json.loads(canonical_json(data)))
