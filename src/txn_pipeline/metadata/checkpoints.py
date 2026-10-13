"""Stage checkpoints (restartability, docs/OPERATIONS.md).

Checkpoints record which stages of a batch completed. They are *informational* for the
in-memory stages (which are deterministic and cheap to redo) and *protective* for the
write stages: every write stage is idempotent by construction (deterministic paths,
atomic replace, upserts) so a retry after any failure never duplicates output.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from txn_pipeline.contracts.enums import CheckpointStatus, Stage

#: Stages whose completion can be observed from a previous attempt of the same batch.
RESTARTABLE_STAGES: tuple[Stage, ...] = (
    Stage.INGESTION,
    Stage.SCHEMA_VALIDATION,
    Stage.QUALITY_VALIDATION,
    Stage.DEDUPLICATION,
    Stage.TRANSFORMATION,
    Stage.RECONCILIATION,
    Stage.QUARANTINE_WRITE,
    Stage.CURATED_WRITE,
    Stage.REGISTRY_COMMIT,
    Stage.WAREHOUSE,
)


@dataclass(frozen=True, slots=True)
class Checkpoint:
    stage: Stage
    status: CheckpointStatus
    attempt: int
    started_at: datetime
    completed_at: datetime | None
    details: dict[str, Any] | None = None
