"""Timestamp parsing and UTC normalisation.

Policy (docs/DATA_CONTRACT.md, "Timezone handling"):

* input timestamps may carry any UTC offset or ``Z``;
* the analytical timestamp is always converted to UTC;
* a timestamp **without** an offset is rejected (``DQ009``) unless an explicit
  ``source_timezone`` is configured, in which case it is interpreted in that zone.
  The pipeline never guesses the local timezone of the host.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


class TimestampError(ValueError):
    """Raised when a timestamp cannot be parsed or has no usable timezone."""


def resolve_timezone(name: str | None) -> ZoneInfo | None:
    if not name:
        return None
    try:
        return ZoneInfo(name)
    except (ZoneInfoNotFoundError, ValueError) as exc:
        raise TimestampError(f"unknown source timezone {name!r}") from exc


def parse_timestamp(value: object, source_timezone: ZoneInfo | None = None) -> datetime:
    """Parse an ISO-8601 timestamp and return a timezone-aware UTC datetime."""
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, date):
        raise TimestampError("a calendar date is not a timestamp")
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            raise TimestampError("timestamp is blank")
        if text.endswith(("Z", "z")):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError as exc:
            raise TimestampError(f"not an ISO-8601 timestamp: {value!r}") from exc
    else:
        raise TimestampError(f"unsupported timestamp type {type(value).__name__}")

    if parsed.tzinfo is None or parsed.tzinfo.utcoffset(parsed) is None:
        if source_timezone is None:
            raise TimestampError("timestamp has no timezone offset and no source timezone is set")
        parsed = parsed.replace(tzinfo=source_timezone)
    return parsed.astimezone(UTC)


def parse_date(value: object) -> date:
    """Parse an ISO-8601 calendar date (``YYYY-MM-DD``)."""
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        text = value.strip()
        try:
            return date.fromisoformat(text)
        except ValueError as exc:
            raise ValueError(f"not an ISO-8601 date: {value!r}") from exc
    raise ValueError(f"unsupported date type {type(value).__name__}")


def utc_now() -> datetime:
    return datetime.now(tz=UTC)
