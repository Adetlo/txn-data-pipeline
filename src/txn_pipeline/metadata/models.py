"""SQLAlchemy 2.x ORM models for pipeline metadata (schema in migrations/versions).

Target database is PostgreSQL; the types below are chosen so the same models and the
same Alembic migration also run on SQLite for zero-infrastructure local runs (ADR 0004).
Large raw files are never stored here: quarantine keeps the raw *row* (JSON text) only.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    Uuid,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

JSONType = JSON().with_variant(JSONB(), "postgresql")


class Base(DeclarativeBase):
    pass


class SourceFileModel(Base):
    """One immutable source file version, identified by its SHA-256."""

    __tablename__ = "source_files"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    source_name: Mapped[str] = mapped_column(String(64), nullable=False)
    filename: Mapped[str] = mapped_column(String(255), nullable=False)
    file_path: Mapped[str] = mapped_column(Text, nullable=False)
    sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    size_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    format: Mapped[str] = mapped_column(String(16), nullable=False)
    discovered_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    batches: Mapped[list[PipelineBatchModel]] = relationship(back_populates="source_file")

    __table_args__ = (
        UniqueConstraint("source_name", "sha256", name="uq_source_files_source_hash"),
        Index("ix_source_files_filename", "filename"),
    )


class PipelineBatchModel(Base):
    __tablename__ = "pipeline_batches"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    source_file_id: Mapped[uuid.UUID] = mapped_column(
        Uuid, ForeignKey("source_files.id", ondelete="RESTRICT"), nullable=False
    )
    source_name: Mapped[str] = mapped_column(String(64), nullable=False)
    source_filename: Mapped[str] = mapped_column(String(255), nullable=False)
    source_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    actor: Mapped[str] = mapped_column(String(32), nullable=False)
    pipeline_version: Mapped[str] = mapped_column(String(32), nullable=False)
    config_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    schema_version: Mapped[str] = mapped_column(String(64), nullable=False)
    correlation_id: Mapped[str] = mapped_column(String(64), nullable=False)
    attempt: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    record_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    accepted_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    rejected_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    duplicate_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    conflict_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    quarantine_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    warning_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    reconciliation_balanced: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    reconciliation_report: Mapped[dict[str, Any] | None] = mapped_column(JSONType, nullable=True)
    metrics: Mapped[dict[str, Any] | None] = mapped_column(JSONType, nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)

    source_file: Mapped[SourceFileModel] = relationship(back_populates="batches")
    checkpoints: Mapped[list[PipelineCheckpointModel]] = relationship(
        back_populates="batch", cascade="all, delete-orphan"
    )

    __table_args__ = (
        Index("ix_pipeline_batches_source_hash", "source_hash"),
        Index("ix_pipeline_batches_status", "status"),
        Index("ix_pipeline_batches_started_at", "started_at"),
    )


class PipelineCheckpointModel(Base):
    __tablename__ = "pipeline_checkpoints"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    batch_id: Mapped[uuid.UUID] = mapped_column(
        Uuid, ForeignKey("pipeline_batches.id", ondelete="CASCADE"), nullable=False
    )
    stage: Mapped[str] = mapped_column(String(32), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    attempt: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    details: Mapped[dict[str, Any] | None] = mapped_column(JSONType, nullable=True)

    batch: Mapped[PipelineBatchModel] = relationship(back_populates="checkpoints")

    __table_args__ = (
        UniqueConstraint("batch_id", "stage", name="uq_pipeline_checkpoints_batch_stage"),
    )


class QualityResultModel(Base):
    """Per-batch, per-rule tallies (not per record)."""

    __tablename__ = "quality_results"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    batch_id: Mapped[uuid.UUID] = mapped_column(
        Uuid, ForeignKey("pipeline_batches.id", ondelete="CASCADE"), nullable=False
    )
    rule_id: Mapped[str] = mapped_column(String(64), nullable=False)
    severity: Mapped[str] = mapped_column(String(8), nullable=False)
    stage: Mapped[str] = mapped_column(String(32), nullable=False)
    record_count: Mapped[int] = mapped_column(Integer, nullable=False)
    recorded_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        UniqueConstraint("batch_id", "rule_id", name="uq_quality_results_batch_rule"),
        Index("ix_quality_results_rule_id", "rule_id"),
    )


class QuarantineRecordModel(Base):
    __tablename__ = "quarantine_records"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    batch_id: Mapped[uuid.UUID] = mapped_column(
        Uuid, ForeignKey("pipeline_batches.id", ondelete="CASCADE"), nullable=False
    )
    record_number: Mapped[int] = mapped_column(BigInteger, nullable=False)
    transaction_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    rule_id: Mapped[str] = mapped_column(String(64), nullable=False)
    all_rule_ids: Mapped[str] = mapped_column(String(512), nullable=False)
    severity: Mapped[str] = mapped_column(String(8), nullable=False)
    reason: Mapped[str] = mapped_column(Text, nullable=False)
    stage: Mapped[str] = mapped_column(String(32), nullable=False)
    raw_record: Mapped[dict[str, Any]] = mapped_column(JSONType, nullable=False)
    quarantined_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        UniqueConstraint("batch_id", "record_number", name="uq_quarantine_batch_record"),
        Index("ix_quarantine_records_rule_id", "rule_id"),
        Index("ix_quarantine_records_transaction_id", "transaction_id"),
    )


class LineageRecordModel(Base):
    __tablename__ = "lineage_records"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    batch_id: Mapped[uuid.UUID] = mapped_column(
        Uuid, ForeignKey("pipeline_batches.id", ondelete="CASCADE"), nullable=False
    )
    source_filename: Mapped[str] = mapped_column(String(255), nullable=False)
    source_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    output_kind: Mapped[str] = mapped_column(String(32), nullable=False)
    output_path: Mapped[str] = mapped_column(Text, nullable=False)
    row_count: Mapped[int] = mapped_column(Integer, nullable=False)
    pipeline_version: Mapped[str] = mapped_column(String(32), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        UniqueConstraint("batch_id", "output_kind", "output_path", name="uq_lineage_batch_output"),
        Index("ix_lineage_records_source_hash", "source_hash"),
    )


class TransactionRegistryModel(Base):
    """Which curated version (payload hash) of each transaction_id exists, and from which batch.

    This is the cross-batch idempotency index (ADR 0005): the *first* curated version wins;
    later identical payloads are duplicates, later different payloads are conflicts.
    """

    __tablename__ = "transaction_registry"

    transaction_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    source_system: Mapped[str] = mapped_column(String(64), nullable=False)
    payload_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    batch_id: Mapped[uuid.UUID] = mapped_column(
        Uuid, ForeignKey("pipeline_batches.id", ondelete="RESTRICT"), nullable=False
    )
    curated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (Index("ix_transaction_registry_batch_id", "batch_id"),)
