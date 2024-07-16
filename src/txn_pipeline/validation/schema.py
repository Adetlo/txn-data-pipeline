"""Schema validation stage: raw rows -> canonical records or quarantine rejections.

A row never crashes the pipeline. Every Pydantic error is mapped to a stable rule id:

* validators raise ``ValueError("<RULE_ID>: ...")`` -> that rule;
* a missing required field -> the field's dedicated DQ rule where one exists
  (``DQ001`` for ``transaction_id`` etc.), otherwise ``SCH001``;
* anything else -> ``SCH002_INVALID_FIELD_VALUE``.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from pydantic import ValidationError

from txn_pipeline.contracts import rules
from txn_pipeline.contracts.enums import Severity, Stage
from txn_pipeline.contracts.transaction import TransactionRecord, parse_rule_prefix
from txn_pipeline.validation.results import (
    Finding,
    RawRecord,
    Rejection,
    RuleCounts,
    ValidatedRecord,
)

_MISSING_FIELD_RULE: dict[str, str] = {
    "transaction_id": rules.DQ001_MISSING_TRANSACTION_ID,
    "currency": rules.DQ002_INVALID_CURRENCY,
    "amount": rules.DQ003_INVALID_AMOUNT,
    "status": rules.DQ007_UNKNOWN_STATUS,
    "country": rules.DQ008_INVALID_COUNTRY_CODE,
    "transaction_timestamp": rules.DQ009_INVALID_TIMESTAMP,
}

_PYDANTIC_PREFIX = "Value error, "


def classify_error(error: Mapping[str, Any]) -> Finding:
    """Map one Pydantic error dict to a :class:`Finding` with a stable rule id."""
    loc = error.get("loc") or ()
    field = str(loc[0]) if loc else None
    message = str(error.get("msg", "invalid value"))
    if message.startswith(_PYDANTIC_PREFIX):
        message = message[len(_PYDANTIC_PREFIX) :]
    rule_id, message = parse_rule_prefix(message)
    if rule_id is None:
        if error.get("type") == "missing":
            rule_id = _MISSING_FIELD_RULE.get(field or "", rules.SCH001_MISSING_REQUIRED_FIELD)
            message = f"{field} is missing"
        else:
            rule_id = rules.SCH002_INVALID_FIELD_VALUE
            message = f"{field}: {message}" if field else message
    return Finding(rule_id=rule_id, severity=Severity.ERROR, message=message, field=field)


def validate_record(
    raw: RawRecord, *, source_timezone: str | None = None
) -> ValidatedRecord | Rejection:
    """Validate a single raw record; returns either a ValidatedRecord or a Rejection."""
    context = {"source_timezone": source_timezone} if source_timezone else None
    try:
        record = TransactionRecord.model_validate(raw.data, context=context)
    except ValidationError as exc:
        findings = tuple(classify_error(e) for e in exc.errors())
        primary = findings[0]
        tx_id = raw.data.get("transaction_id")
        return Rejection(
            record_number=raw.record_number,
            raw=raw.data,
            rule_id=primary.rule_id,
            reason="; ".join(f"{f.rule_id}: {f.message}" for f in findings),
            stage=Stage.SCHEMA_VALIDATION,
            findings=findings,
            transaction_id=str(tx_id) if tx_id not in (None, "") else None,
        )
    return ValidatedRecord(
        record_number=raw.record_number, raw=raw.data, raw_hash=raw.raw_hash, record=record
    )


def validate_schema(
    raw_records: Iterable[RawRecord], *, source_timezone: str | None = None
) -> tuple[list[ValidatedRecord], list[Rejection], RuleCounts]:
    """Validate every raw record. Never raises for a bad row."""
    accepted: list[ValidatedRecord] = []
    rejected: list[Rejection] = []
    counts = RuleCounts()
    for raw in raw_records:
        outcome = validate_record(raw, source_timezone=source_timezone)
        if isinstance(outcome, Rejection):
            rejected.append(outcome)
            for rule_id in outcome.all_rule_ids:
                counts.add(rule_id)
        else:
            accepted.append(outcome)
    return accepted, rejected, counts
