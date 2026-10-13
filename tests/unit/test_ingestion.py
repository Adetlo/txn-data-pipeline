from __future__ import annotations

import hashlib
import json
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from tests.conftest import valid_row
from txn_pipeline.contracts import rules
from txn_pipeline.contracts.enums import SourceFormat
from txn_pipeline.exceptions import SourceFileError, UnsupportedFormatError
from txn_pipeline.ingestion.csv_reader import read_csv
from txn_pipeline.ingestion.discovery import detect_format, discover, discover_file
from txn_pipeline.ingestion.hashing import canonical_json, sha256_file, sha256_record
from txn_pipeline.ingestion.json_reader import read_jsonl
from txn_pipeline.ingestion.parquet_reader import read_parquet
from txn_pipeline.ingestion.readers import read_records, reader_for
from txn_pipeline.validation.schema import validate_schema


def test_sha256_file_matches_hashlib(tmp_path: Path) -> None:
    path = tmp_path / "f.bin"
    path.write_bytes(b"x" * (3 * 1024 * 1024 + 17))
    assert sha256_file(path) == hashlib.sha256(path.read_bytes()).hexdigest()


def test_sha256_record_is_key_order_independent() -> None:
    assert sha256_record({"a": 1, "b": "2"}) == sha256_record({"b": "2", "a": 1})
    assert sha256_record({"a": 1}) != sha256_record({"a": "1"})


def test_canonical_json_handles_decimal_dates_bytes() -> None:
    text = canonical_json(
        {
            "amount": Decimal("1.10"),
            "d": date(2026, 1, 2),
            "ts": datetime(2026, 1, 2, tzinfo=UTC),
            "b": b"\x01",
            "o": object(),
        }
    )
    data = json.loads(text)
    assert data["amount"] == "1.10"
    assert data["d"] == "2026-01-02"
    assert data["ts"].startswith("2026-01-02T00:00:00")
    assert data["b"] == "01"
    assert isinstance(data["o"], str)


@pytest.mark.parametrize(
    ("name", "fmt"),
    [
        ("a.csv", SourceFormat.CSV),
        ("a.CSV", SourceFormat.CSV),
        ("a.jsonl", SourceFormat.JSONL),
        ("a.ndjson", SourceFormat.JSONL),
        ("a.json", SourceFormat.JSONL),
        ("a.parquet", SourceFormat.PARQUET),
        ("a.pq", SourceFormat.PARQUET),
    ],
)
def test_detect_format(name: str, fmt: SourceFormat) -> None:
    assert detect_format(Path(name)) is fmt


def test_detect_format_explicit_and_errors() -> None:
    assert detect_format(Path("data.txt"), "CSV") is SourceFormat.CSV
    with pytest.raises(UnsupportedFormatError):
        detect_format(Path("data.txt"))
    with pytest.raises(UnsupportedFormatError):
        detect_format(Path("data.csv"), "xml")


def test_discover_file(tmp_path: Path) -> None:
    path = tmp_path / "t.csv"
    path.write_text("transaction_id\n1\n")
    source = discover_file(path)
    assert source.filename == "t.csv"
    assert source.size_bytes == path.stat().st_size
    assert source.sha256 == sha256_file(path)
    assert source.format is SourceFormat.CSV
    assert source.discovered_at.tzinfo is not None
    with pytest.raises(SourceFileError):
        discover_file(tmp_path / "missing.csv")
    with pytest.raises(SourceFileError):
        discover_file(tmp_path)


def test_discover_directory(tmp_path: Path) -> None:
    (tmp_path / "b.jsonl").write_text("{}\n")
    (tmp_path / "a.csv").write_text("x\n")
    (tmp_path / "notes.txt").write_text("skip")
    found = discover(tmp_path)
    assert [f.filename for f in found] == ["a.csv", "b.jsonl"]
    assert discover(tmp_path / "a.csv")[0].filename == "a.csv"
    empty = tmp_path / "empty"
    empty.mkdir()
    with pytest.raises(SourceFileError):
        discover(empty)


def test_csv_reader(tmp_path: Path) -> None:
    path = tmp_path / "t.csv"
    path.write_text("﻿transaction_id,amount\nT1,1.00\nT2,2.00,extra\n")
    records = list(read_csv(path))
    assert [r.record_number for r in records] == [1, 2]
    assert records[0].data == {"transaction_id": "T1", "amount": "1.00"}
    assert records[1].data["_unparsed_extra_columns"] == ["extra"]
    assert records[0].raw_hash == sha256_record(records[0].data)


def test_csv_reader_errors(tmp_path: Path) -> None:
    empty = tmp_path / "empty.csv"
    empty.write_text("")
    with pytest.raises(SourceFileError):
        list(read_csv(empty))
    binary = tmp_path / "bin.csv"
    binary.write_bytes(b"\xff\xfe\x00bad")
    with pytest.raises(SourceFileError):
        list(read_csv(binary))
    with pytest.raises(SourceFileError):
        list(read_csv(tmp_path / "missing.csv"))


def test_jsonl_reader_bad_lines_become_unparseable_records(tmp_path: Path) -> None:
    path = tmp_path / "t.jsonl"
    path.write_text(json.dumps(valid_row()) + "\n\n{not json\n[1,2]\n")
    records = list(read_jsonl(path))
    assert [r.record_number for r in records] == [1, 2, 3]
    assert "_unparseable_line" in records[1].data and "_error" in records[1].data
    assert records[2].data["_error"] == "not a JSON object"
    valid, rejected, _ = validate_schema(records)
    assert len(valid) == 1
    assert {r.rule_id for r in rejected} == {rules.DQ001_MISSING_TRANSACTION_ID}
    with pytest.raises(SourceFileError):
        list(read_jsonl(tmp_path / "missing.jsonl"))


def test_parquet_reader_typed_values(tmp_path: Path) -> None:
    path = tmp_path / "t.parquet"
    table = pa.table(
        {
            "transaction_id": ["T1", "T2"],
            "amount": pa.array([Decimal("1.5000"), Decimal("2.0000")], pa.decimal128(18, 4)),
            "booking_date": pa.array([date(2026, 1, 1), date(2026, 1, 2)], pa.date32()),
        }
    )
    pq.write_table(table, path)
    records = list(read_parquet(path))
    assert records[0].data == {
        "transaction_id": "T1",
        "amount": Decimal("1.5000"),
        "booking_date": date(2026, 1, 1),
    }
    assert records[1].record_number == 2
    bad = tmp_path / "bad.parquet"
    bad.write_bytes(b"not a parquet file")
    with pytest.raises(SourceFileError):
        list(read_parquet(bad))


def test_reader_dispatch(tmp_path: Path) -> None:
    assert reader_for(SourceFormat.CSV) is read_csv
    assert reader_for(SourceFormat.JSONL) is read_jsonl
    assert reader_for(SourceFormat.PARQUET) is read_parquet
    path = tmp_path / "t.csv"
    path.write_text("a\n1\n")
    assert [r.data for r in read_records(path, SourceFormat.CSV)] == [{"a": "1"}]
