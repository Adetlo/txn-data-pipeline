from __future__ import annotations

from datetime import UTC, date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest

from txn_pipeline.contracts.countries import (
    ISO_3166_ALPHA2,
    REGION_OTHER,
    is_valid_country,
    region_for,
)
from txn_pipeline.transformation.normalize import (
    blank_to_none,
    normalize_code,
    normalize_identifier,
    normalize_text,
    parse_bool,
)
from txn_pipeline.transformation.timestamps import (
    TimestampError,
    parse_date,
    parse_timestamp,
    resolve_timezone,
    utc_now,
)


def test_parse_timestamp_offsets_to_utc() -> None:
    expected = datetime(2026, 10, 13, 9, 31, 22, tzinfo=UTC)
    assert parse_timestamp("2026-10-13T09:31:22Z") == expected
    assert parse_timestamp("2026-10-13T09:31:22z") == expected
    assert parse_timestamp("2026-10-13T10:31:22+01:00") == expected
    assert parse_timestamp("2026-10-13T04:31:22-05:00") == expected
    assert parse_timestamp("2026-10-13 09:31:22+00:00") == expected
    assert (
        parse_timestamp(datetime(2026, 10, 13, 11, 31, 22, tzinfo=timezone(timedelta(hours=2))))
        == expected
    )
    assert parse_timestamp("2026-10-13T09:31:22.123456+00:00").microsecond == 123456


def test_parse_timestamp_naive_policy() -> None:
    with pytest.raises(TimestampError):
        parse_timestamp("2026-10-13T09:31:22")
    london = ZoneInfo("Europe/London")
    assert parse_timestamp("2026-10-13T10:31:22", london) == datetime(
        2026, 10, 13, 9, 31, 22, tzinfo=UTC
    )
    assert parse_timestamp(datetime(2026, 1, 13, 10, 31, 22), london) == datetime(
        2026, 1, 13, 10, 31, 22, tzinfo=UTC
    )


@pytest.mark.parametrize("value", ["", "   ", "yesterday", date(2026, 1, 1), 12345, None])
def test_parse_timestamp_rejects(value: object) -> None:
    with pytest.raises(TimestampError):
        parse_timestamp(value)


def test_resolve_timezone() -> None:
    assert resolve_timezone(None) is None
    assert resolve_timezone("") is None
    assert resolve_timezone("Asia/Singapore") == ZoneInfo("Asia/Singapore")
    with pytest.raises(TimestampError):
        resolve_timezone("Nowhere/Land")


def test_parse_date() -> None:
    assert parse_date("2026-10-13") == date(2026, 10, 13)
    assert parse_date(" 2026-10-13 ") == date(2026, 10, 13)
    assert parse_date(date(2026, 10, 13)) == date(2026, 10, 13)
    assert parse_date(datetime(2026, 10, 13, 5, tzinfo=UTC)) == date(2026, 10, 13)
    for bad in ("13/10/2026", "2026-02-30", 20261013):
        with pytest.raises(ValueError, match="date"):
            parse_date(bad)


def test_utc_now_is_aware() -> None:
    assert utc_now().tzinfo is UTC


def test_normalize_helpers() -> None:
    assert normalize_code(" gbp ") == "GBP"
    assert normalize_identifier("  abc ") == "abc"
    assert normalize_text("  a   b\t c ") == "a b c"
    assert blank_to_none("") is None
    assert blank_to_none("  ") is None
    assert blank_to_none("NULL") is None
    assert blank_to_none("none") is None
    assert blank_to_none(0) == 0
    assert blank_to_none("x") == "x"
    assert blank_to_none(None) is None


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("true", True),
        ("Yes", True),
        ("1", True),
        (1, True),
        (True, True),
        ("f", False),
        ("0", False),
        (0, False),
    ],
)
def test_parse_bool(value: object, expected: bool) -> None:
    assert parse_bool(value) is expected


def test_parse_bool_rejects() -> None:
    with pytest.raises(ValueError, match="boolean"):
        parse_bool("maybe")
    with pytest.raises(ValueError, match="boolean"):
        parse_bool(2)


def test_countries() -> None:
    assert len(ISO_3166_ALPHA2) == 249
    assert is_valid_country("GB")
    assert not is_valid_country("UK")
    assert region_for("GB") == "EUROPE"
    assert region_for("JP") == "ASIA_PACIFIC"
    assert region_for("US") == "NORTH_AMERICA"
    assert region_for("BR") == "LATIN_AMERICA"
    assert region_for("ZA") == "MIDDLE_EAST_AFRICA"
    assert region_for("AQ") == REGION_OTHER
