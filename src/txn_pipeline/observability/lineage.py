"""Lineage records: which source produced which output, under which pipeline version.

Lineage is captured at two levels:

* **batch level** (metadata table ``lineage_records``): source filename + SHA-256 ->
  batch -> each output artefact (curated partition, quarantine file, reconciliation
  report, warehouse refresh);
* **row level**: every curated row carries ``batch_id``, ``source_file_hash``,
  ``source_filename``, ``source_record_number``, ``ingested_at`` and ``pipeline_version``.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any


class OutputKind(StrEnum):
    CURATED_PARTITION = "CURATED_PARTITION"
    QUARANTINE_FILE = "QUARANTINE_FILE"
    RECONCILIATION_REPORT = "RECONCILIATION_REPORT"
    WAREHOUSE = "WAREHOUSE"


@dataclass(frozen=True, slots=True)
class LineageEntry:
    batch_id: str
    source_filename: str
    source_hash: str
    output_kind: OutputKind
    output_path: str
    row_count: int
    pipeline_version: str
    created_at: datetime

    def as_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["output_kind"] = self.output_kind.value
        payload["created_at"] = self.created_at.isoformat()
        return payload
