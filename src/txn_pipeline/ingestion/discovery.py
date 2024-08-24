"""Discover source files and compute their identity (SHA-256, size, format)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from txn_pipeline.contracts.enums import SourceFormat
from txn_pipeline.exceptions import SourceFileError, UnsupportedFormatError
from txn_pipeline.ingestion.hashing import sha256_file
from txn_pipeline.transformation.timestamps import utc_now

_EXTENSIONS: dict[str, SourceFormat] = {
    ".csv": SourceFormat.CSV,
    ".jsonl": SourceFormat.JSONL,
    ".ndjson": SourceFormat.JSONL,
    ".json": SourceFormat.JSONL,
    ".parquet": SourceFormat.PARQUET,
    ".pq": SourceFormat.PARQUET,
}


@dataclass(frozen=True, slots=True)
class SourceFile:
    path: Path
    filename: str
    size_bytes: int
    sha256: str
    format: SourceFormat
    discovered_at: datetime


def detect_format(path: Path, explicit: str | None = None) -> SourceFormat:
    if explicit:
        try:
            return SourceFormat(explicit.lower())
        except ValueError as exc:
            raise UnsupportedFormatError(f"unsupported format {explicit!r}") from exc
    fmt = _EXTENSIONS.get(path.suffix.lower())
    if fmt is None:
        raise UnsupportedFormatError(
            f"cannot infer format from extension {path.suffix!r} (expected .csv/.jsonl/.parquet)"
        )
    return fmt


def discover_file(path: Path, *, fmt: str | None = None) -> SourceFile:
    if not path.exists():
        raise SourceFileError(f"source file not found: {path}")
    if not path.is_file():
        raise SourceFileError(f"source path is not a file: {path}")
    return SourceFile(
        path=path.resolve(),
        filename=path.name,
        size_bytes=path.stat().st_size,
        sha256=sha256_file(path),
        format=detect_format(path, fmt),
        discovered_at=utc_now(),
    )


def discover(path: Path, *, fmt: str | None = None) -> list[SourceFile]:
    """Discover one file or every supported file in a directory (sorted by name)."""
    if path.is_dir():
        files = sorted(p for p in path.iterdir() if p.is_file() and p.suffix.lower() in _EXTENSIONS)
        if not files:
            raise SourceFileError(f"no supported source files in {path}")
        return [discover_file(p, fmt=fmt) for p in files]
    return [discover_file(path, fmt=fmt)]
