"""Parquet reader. Values arrive typed (Decimal, date, datetime) and the contract accepts them."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pyarrow.parquet as pq

from txn_pipeline.exceptions import SourceFileError
from txn_pipeline.ingestion.hashing import sha256_record
from txn_pipeline.validation.results import RawRecord

_BATCH_ROWS = 10_000


def read_parquet(path: Path) -> Iterator[RawRecord]:
    try:
        parquet_file = pq.ParquetFile(path)
    except (OSError, ValueError) as exc:  # pyarrow raises ArrowInvalid (a ValueError)
        raise SourceFileError(f"{path.name}: cannot read Parquet: {exc}") from exc
    number = 0
    for batch in parquet_file.iter_batches(batch_size=_BATCH_ROWS):
        for row in batch.to_pylist():
            number += 1
            yield RawRecord(number, row, sha256_record(row))
