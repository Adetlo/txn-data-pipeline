"""CSV reader: every value is kept as a string; the contract does the parsing."""

from __future__ import annotations

import csv
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from txn_pipeline.exceptions import SourceFileError
from txn_pipeline.ingestion.hashing import sha256_record
from txn_pipeline.validation.results import RawRecord


def read_csv(path: Path) -> Iterator[RawRecord]:
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as fh:
            reader = csv.DictReader(fh, restkey="_unparsed_extra_columns")
            if reader.fieldnames is None:
                raise SourceFileError(f"{path.name}: CSV file has no header row")
            for number, row in enumerate(reader, start=1):
                data: dict[str, Any] = dict(row)
                yield RawRecord(record_number=number, data=data, raw_hash=sha256_record(data))
    except (OSError, UnicodeDecodeError, csv.Error) as exc:
        raise SourceFileError(f"{path.name}: cannot read CSV: {exc}") from exc
