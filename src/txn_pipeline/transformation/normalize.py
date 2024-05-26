"""Pure string normalisation helpers used by the contract validators.

These are deliberately tiny and side-effect free so they can be property-tested.
"""

from __future__ import annotations

import re
from typing import Any

_WS = re.compile(r"\s+")


def normalize_code(value: Any) -> str:
    """Upper-case, trimmed code value (currency, country, status, type, channel)."""
    return str(value).strip().upper()


def normalize_identifier(value: Any) -> str:
    """Trimmed identifier; internal whitespace is not allowed so it is not collapsed."""
    return str(value).strip()


def normalize_text(value: Any) -> str:
    """Trim and collapse internal whitespace (free-text fields such as reference)."""
    return _WS.sub(" ", str(value).strip())


def blank_to_none(value: Any) -> Any:
    """Treat empty / whitespace-only strings (and the literal 'null') as missing."""
    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        if stripped == "" or stripped.lower() in {"null", "none", "nan"}:
            return None
    return value


_TRUE = frozenset({"true", "t", "1", "yes", "y"})
_FALSE = frozenset({"false", "f", "0", "no", "n"})


def parse_bool(value: Any) -> bool:
    """Parse the boolean spellings found in CSV / JSON exports."""
    if isinstance(value, bool):
        return value
    if isinstance(value, int) and value in (0, 1):
        return bool(value)
    text = str(value).strip().lower()
    if text in _TRUE:
        return True
    if text in _FALSE:
        return False
    raise ValueError(f"not a boolean: {value!r}")
