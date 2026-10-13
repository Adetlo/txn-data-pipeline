"""initial pipeline metadata schema

Revision ID: 0001
Revises:
Create Date: 2026-10-13
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None

JSONType = sa.JSON().with_variant(postgresql.JSONB(), "postgresql")
TZ = sa.DateTime(timezone=True)


def upgrade() -> None:
    op.create_table(
        "source_files",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("source_name", sa.String(64), nullable=False),
        sa.Column("filename", sa.String(255), nullable=False),
        sa.Column("file_path", sa.Text(), nullable=False),
        sa.Column("sha256", sa.String(64), nullable=False),
        sa.Column("size_bytes", sa.BigInteger(), nullable=False),
        sa.Column("format", sa.String(16), nullable=False),
        sa.Column("discovered_at", TZ, nullable=False),
        sa.UniqueConstraint("source_name", "sha256", name="uq_source_files_source_hash"),
        sa.CheckConstraint("length(sha256) = 64", name="ck_source_files_sha256_length"),
        sa.CheckConstraint("size_bytes >= 0", name="ck_source_files_size"),
    )
    op.create_index("ix_source_files_filename", "source_files", ["filename"])

    op.create_table(
        "pipeline_batches",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column(
            "source_file_id",
            sa.Uuid(),
            sa.ForeignKey("source_files.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("source_name", sa.String(64), nullable=False),
        sa.Column("source_filename", sa.String(255), nullable=False),
        sa.Column("source_hash", sa.String(64), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("actor", sa.String(32), nullable=False),
        sa.Column("pipeline_version", sa.String(32), nullable=False),
        sa.Column("config_hash", sa.String(64), nullable=False),
        sa.Column("schema_version", sa.String(64), nullable=False),
        sa.Column("correlation_id", sa.String(64), nullable=False),
        sa.Column("attempt", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("started_at", TZ, nullable=False),
        sa.Column("completed_at", TZ, nullable=True),
        sa.Column("record_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("accepted_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("rejected_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("duplicate_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("conflict_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("quarantine_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("warning_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("reconciliation_balanced", sa.Boolean(), nullable=True),
        sa.Column("reconciliation_report", JSONType, nullable=True),
        sa.Column("metrics", JSONType, nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.CheckConstraint(
            "status IN ('DISCOVERED','RUNNING','VALIDATED','COMPLETED','FAILED','PARTIAL')",
            name="ck_pipeline_batches_status",
        ),
        sa.CheckConstraint("attempt >= 1", name="ck_pipeline_batches_attempt"),
        sa.CheckConstraint(
            "record_count >= 0 AND accepted_count >= 0 AND rejected_count >= 0 AND duplicate_count >= 0",
            name="ck_pipeline_batches_counts",
        ),
    )
    op.create_index("ix_pipeline_batches_source_hash", "pipeline_batches", ["source_hash"])
    op.create_index("ix_pipeline_batches_status", "pipeline_batches", ["status"])
    op.create_index("ix_pipeline_batches_started_at", "pipeline_batches", ["started_at"])

    op.create_table(
        "pipeline_checkpoints",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column(
            "batch_id",
            sa.Uuid(),
            sa.ForeignKey("pipeline_batches.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("stage", sa.String(32), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("attempt", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("started_at", TZ, nullable=False),
        sa.Column("completed_at", TZ, nullable=True),
        sa.Column("details", JSONType, nullable=True),
        sa.UniqueConstraint("batch_id", "stage", name="uq_pipeline_checkpoints_batch_stage"),
        sa.CheckConstraint(
            "status IN ('STARTED','COMPLETED','FAILED','SKIPPED')",
            name="ck_pipeline_checkpoints_status",
        ),
    )

    op.create_table(
        "quality_results",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column(
            "batch_id",
            sa.Uuid(),
            sa.ForeignKey("pipeline_batches.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("rule_id", sa.String(64), nullable=False),
        sa.Column("severity", sa.String(8), nullable=False),
        sa.Column("stage", sa.String(32), nullable=False),
        sa.Column("record_count", sa.Integer(), nullable=False),
        sa.Column("recorded_at", TZ, nullable=False),
        sa.UniqueConstraint("batch_id", "rule_id", name="uq_quality_results_batch_rule"),
        sa.CheckConstraint("severity IN ('ERROR','WARNING')", name="ck_quality_results_severity"),
        sa.CheckConstraint("record_count >= 0", name="ck_quality_results_count"),
    )
    op.create_index("ix_quality_results_rule_id", "quality_results", ["rule_id"])

    op.create_table(
        "quarantine_records",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column(
            "batch_id",
            sa.Uuid(),
            sa.ForeignKey("pipeline_batches.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("record_number", sa.BigInteger(), nullable=False),
        sa.Column("transaction_id", sa.String(64), nullable=True),
        sa.Column("rule_id", sa.String(64), nullable=False),
        sa.Column("all_rule_ids", sa.String(512), nullable=False),
        sa.Column("severity", sa.String(8), nullable=False),
        sa.Column("reason", sa.Text(), nullable=False),
        sa.Column("stage", sa.String(32), nullable=False),
        sa.Column("raw_record", JSONType, nullable=False),
        sa.Column("quarantined_at", TZ, nullable=False),
        sa.UniqueConstraint("batch_id", "record_number", name="uq_quarantine_batch_record"),
        sa.CheckConstraint("record_number >= 1", name="ck_quarantine_record_number"),
    )
    op.create_index("ix_quarantine_records_rule_id", "quarantine_records", ["rule_id"])
    op.create_index(
        "ix_quarantine_records_transaction_id", "quarantine_records", ["transaction_id"]
    )

    op.create_table(
        "lineage_records",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column(
            "batch_id",
            sa.Uuid(),
            sa.ForeignKey("pipeline_batches.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("source_filename", sa.String(255), nullable=False),
        sa.Column("source_hash", sa.String(64), nullable=False),
        sa.Column("output_kind", sa.String(32), nullable=False),
        sa.Column("output_path", sa.Text(), nullable=False),
        sa.Column("row_count", sa.Integer(), nullable=False),
        sa.Column("pipeline_version", sa.String(32), nullable=False),
        sa.Column("created_at", TZ, nullable=False),
        sa.UniqueConstraint(
            "batch_id", "output_kind", "output_path", name="uq_lineage_batch_output"
        ),
        sa.CheckConstraint("row_count >= 0", name="ck_lineage_row_count"),
    )
    op.create_index("ix_lineage_records_source_hash", "lineage_records", ["source_hash"])

    op.create_table(
        "transaction_registry",
        sa.Column("transaction_id", sa.String(64), primary_key=True),
        sa.Column("source_system", sa.String(64), nullable=False),
        sa.Column("payload_hash", sa.String(64), nullable=False),
        sa.Column(
            "batch_id",
            sa.Uuid(),
            sa.ForeignKey("pipeline_batches.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("curated_at", TZ, nullable=False),
        sa.CheckConstraint("length(payload_hash) = 64", name="ck_transaction_registry_hash_length"),
    )
    op.create_index("ix_transaction_registry_batch_id", "transaction_registry", ["batch_id"])


def downgrade() -> None:
    op.drop_table("transaction_registry")
    op.drop_table("lineage_records")
    op.drop_table("quarantine_records")
    op.drop_table("quality_results")
    op.drop_table("pipeline_checkpoints")
    op.drop_table("pipeline_batches")
    op.drop_table("source_files")
