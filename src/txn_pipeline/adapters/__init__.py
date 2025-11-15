"""Source adapters: map differently shaped exports onto the canonical contract before validation."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from txn_pipeline.adapters.corebank import adapt_corebank_entry

Adapter = Callable[[dict[str, Any]], dict[str, Any]]

ADAPTERS: dict[str, Adapter] = {
    "corebank-ledger": adapt_corebank_entry,
}


def get_adapter(name: str | None) -> Adapter | None:
    if name is None:
        return None
    try:
        return ADAPTERS[name]
    except KeyError:
        raise KeyError(f"unknown adapter {name!r}; available: {sorted(ADAPTERS)}") from None
