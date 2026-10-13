"""JSON Lines reader. A line that is not a JSON object becomes an unparseable record."""

from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path

from txn_pipeline.exceptions import SourceFileError
from txn_pipeline.ingestion.hashing import sha256_record
from txn_pipeline.validation.results import RawRecord


def read_jsonl(path: Path) -> Iterator[RawRecord]:
    try:
        with path.open("r", encoding="utf-8") as fh:
            number = 0
            for line in fh:
                text = line.strip()
                if not text:
                    continue
                number += 1
                try:
                    parsed = json.loads(text)
                except json.JSONDecodeError as exc:
                    data = {"_unparseable_line": text[:2000], "_error": str(exc)}
                    yield RawRecord(number, data, sha256_record(data))
                    continue
                if not isinstance(parsed, dict):
                    data = {"_unparseable_line": text[:2000], "_error": "not a JSON object"}
                    yield RawRecord(number, data, sha256_record(data))
                    continue
                yield RawRecord(number, parsed, sha256_record(parsed))
    except (OSError, UnicodeDecodeError) as exc:
        raise SourceFileError(f"{path.name}: cannot read JSON Lines: {exc}") from exc
