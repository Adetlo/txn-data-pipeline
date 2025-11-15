"""Adapter for exports shaped like the corebank-ledger demo (github.com/Adetlo/corebank-ledger).

That project is a double-entry ledger: each *journal entry* has two or more postings
(DEBIT / CREDIT legs). This adapter flattens one journal entry into one canonical
transaction:

* ``transaction_id``   <- entry ``id``
* ``account_id``       <- account of the first DEBIT leg
* ``counterparty_id``  <- account of the first CREDIT leg
* ``transaction_type`` <- ``metadata.kind`` (``TRANSFER`` / ``DEPOSIT`` / ...), default TRANSFER
* ``amount``/``currency`` <- the debit leg
* ``transaction_timestamp`` <- ``posted_at`` (falls back to ``created_at``)
* ``is_reversal`` / ``original_transaction_id`` <- ``reversal_of``

There is **no runtime dependency** on corebank-ledger: this only maps a JSON shape.
Fields it cannot derive (``channel``, ``country``) are defaulted and documented.
"""

from __future__ import annotations

from typing import Any

DEFAULT_CHANNEL = "API"
DEFAULT_COUNTRY = "GB"
SOURCE_SYSTEM = "COREBANK_LEDGER_DEMO"


def _leg(postings: list[dict[str, Any]], direction: str) -> dict[str, Any] | None:
    for posting in postings:
        if str(posting.get("direction", "")).upper() == direction:
            return posting
    return None


def adapt_corebank_entry(entry: dict[str, Any]) -> dict[str, Any]:
    """Map one journal-entry export row to the canonical input shape (strings / None)."""
    postings = entry.get("postings") or []
    if not isinstance(postings, list):
        postings = []
    debit = _leg(postings, "DEBIT") or {}
    credit = _leg(postings, "CREDIT") or {}
    metadata = entry.get("metadata") or {}
    kind = str(metadata.get("kind") or "TRANSFER").upper()
    reversal_of = entry.get("reversal_of")
    return {
        "schema_version": "financial-transaction/1",
        "transaction_id": entry.get("id"),
        "source_system": SOURCE_SYSTEM,
        "account_id": debit.get("account_number") or debit.get("account_id"),
        "counterparty_id": credit.get("account_number") or credit.get("account_id"),
        "transaction_type": kind,
        "amount": debit.get("amount"),
        "currency": debit.get("currency") or entry.get("currency"),
        "booking_date": entry.get("booking_date"),
        "value_date": entry.get("value_date") or entry.get("booking_date"),
        "transaction_timestamp": entry.get("posted_at") or entry.get("created_at"),
        "channel": metadata.get("channel") or DEFAULT_CHANNEL,
        "country": metadata.get("country") or DEFAULT_COUNTRY,
        "reference": entry.get("reference"),
        "status": entry.get("status"),
        "merchant_category": None,
        "beneficiary_country": None,
        "is_reversal": reversal_of is not None,
        "original_transaction_id": reversal_of,
    }
