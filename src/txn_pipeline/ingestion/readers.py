"""Format dispatch."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from pathlib import Path

from txn_pipeline.contracts.enums import SourceFormat
from txn_pipeline.ingestion.csv_reader import read_csv
from txn_pipeline.ingestion.json_reader import read_jsonl
from txn_pipeline.ingestion.parquet_reader import read_parquet
from txn_pipeline.validation.results import RawRecord

Reader = Callable[[Path], Iterator[RawRecord]]

READERS: dict[SourceFormat, Reader] = {
    SourceFormat.CSV: read_csv,
    SourceFormat.JSONL: read_jsonl,
    SourceFormat.PARQUET: read_parquet,
}


def reader_for(fmt: SourceFormat) -> Reader:
    return READERS[fmt]


def read_records(path: Path, fmt: SourceFormat) -> Iterator[RawRecord]:
    return reader_for(fmt)(path)
