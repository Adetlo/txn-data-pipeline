"""Catalogue of data-quality and schema rules.

Rule identifiers are **stable**: once published they are never renumbered or reused,
because quarantine records, quality results and downstream dashboards key on them.
See docs/DATA_QUALITY.md for the narrative version of this table.
"""

from __future__ import annotations

from dataclasses import dataclass

from txn_pipeline.contracts.enums import Severity, Stage


@dataclass(frozen=True, slots=True)
class RuleDefinition:
    rule_id: str
    severity: Severity
    stage: Stage
    description: str


# --- Schema stage ---------------------------------------------------------------------
SCH001_MISSING_REQUIRED_FIELD = "SCH001_MISSING_REQUIRED_FIELD"
SCH002_INVALID_FIELD_VALUE = "SCH002_INVALID_FIELD_VALUE"
SCH003_UNKNOWN_TRANSACTION_TYPE = "SCH003_UNKNOWN_TRANSACTION_TYPE"
SCH004_UNKNOWN_CHANNEL = "SCH004_UNKNOWN_CHANNEL"
SCH005_UNSUPPORTED_SCHEMA_VERSION = "SCH005_UNSUPPORTED_SCHEMA_VERSION"
SCH006_INVALID_DATE_FORMAT = "SCH006_INVALID_DATE_FORMAT"
SCH007_UNPARSEABLE_RECORD = "SCH007_UNPARSEABLE_RECORD"

# --- Data-quality rules -----------------------------------------------------------------
DQ001_MISSING_TRANSACTION_ID = "DQ001_MISSING_TRANSACTION_ID"
DQ002_INVALID_CURRENCY = "DQ002_INVALID_CURRENCY"
DQ003_INVALID_AMOUNT = "DQ003_INVALID_AMOUNT"
DQ004_BOOKING_OUTSIDE_ALLOWED_RANGE = "DQ004_BOOKING_OUTSIDE_ALLOWED_RANGE"
DQ005_VALUE_DATE_TOO_FAR_FROM_BOOKING = "DQ005_VALUE_DATE_TOO_FAR_FROM_BOOKING"
DQ006_DUPLICATE_TRANSACTION = "DQ006_DUPLICATE_TRANSACTION"
DQ007_UNKNOWN_STATUS = "DQ007_UNKNOWN_STATUS"
DQ008_INVALID_COUNTRY_CODE = "DQ008_INVALID_COUNTRY_CODE"
DQ009_INVALID_TIMESTAMP = "DQ009_INVALID_TIMESTAMP"
DQ010_REVERSAL_MISSING_ORIGINAL = "DQ010_REVERSAL_MISSING_ORIGINAL"
DQ011_TIMESTAMP_BOOKING_MISMATCH = "DQ011_TIMESTAMP_BOOKING_MISMATCH"

RULES: dict[str, RuleDefinition] = {
    r.rule_id: r
    for r in (
        RuleDefinition(
            SCH001_MISSING_REQUIRED_FIELD,
            Severity.ERROR,
            Stage.SCHEMA_VALIDATION,
            "A required contract field is absent or empty.",
        ),
        RuleDefinition(
            SCH002_INVALID_FIELD_VALUE,
            Severity.ERROR,
            Stage.SCHEMA_VALIDATION,
            "A field has the wrong type or an unparseable value not covered by a DQ rule.",
        ),
        RuleDefinition(
            SCH003_UNKNOWN_TRANSACTION_TYPE,
            Severity.ERROR,
            Stage.SCHEMA_VALIDATION,
            "transaction_type is not in the controlled vocabulary.",
        ),
        RuleDefinition(
            SCH004_UNKNOWN_CHANNEL,
            Severity.ERROR,
            Stage.SCHEMA_VALIDATION,
            "channel is not in the controlled vocabulary.",
        ),
        RuleDefinition(
            SCH005_UNSUPPORTED_SCHEMA_VERSION,
            Severity.ERROR,
            Stage.SCHEMA_VALIDATION,
            "schema_version is not supported by this pipeline version.",
        ),
        RuleDefinition(
            SCH006_INVALID_DATE_FORMAT,
            Severity.ERROR,
            Stage.SCHEMA_VALIDATION,
            "booking_date / value_date is not an ISO-8601 calendar date.",
        ),
        RuleDefinition(
            SCH007_UNPARSEABLE_RECORD,
            Severity.ERROR,
            Stage.SCHEMA_VALIDATION,
            "The source row could not be parsed into a key/value record at all.",
        ),
        RuleDefinition(
            DQ001_MISSING_TRANSACTION_ID,
            Severity.ERROR,
            Stage.SCHEMA_VALIDATION,
            "transaction_id is missing, blank or malformed.",
        ),
        RuleDefinition(
            DQ002_INVALID_CURRENCY,
            Severity.ERROR,
            Stage.SCHEMA_VALIDATION,
            "currency is not a supported ISO 4217 code.",
        ),
        RuleDefinition(
            DQ003_INVALID_AMOUNT,
            Severity.ERROR,
            Stage.SCHEMA_VALIDATION,
            "amount is unparseable, non-positive, too large or has more decimals than the "
            "currency allows.",
        ),
        RuleDefinition(
            DQ004_BOOKING_OUTSIDE_ALLOWED_RANGE,
            Severity.ERROR,
            Stage.QUALITY_VALIDATION,
            "booking_date is before the configured minimum or too far in the future.",
        ),
        RuleDefinition(
            DQ005_VALUE_DATE_TOO_FAR_FROM_BOOKING,
            Severity.WARNING,
            Stage.QUALITY_VALIDATION,
            "value_date differs from booking_date by more than the configured tolerance.",
        ),
        RuleDefinition(
            DQ006_DUPLICATE_TRANSACTION,
            Severity.ERROR,
            Stage.DEDUPLICATION,
            "transaction_id was seen before with a materially different payload (conflict).",
        ),
        RuleDefinition(
            DQ007_UNKNOWN_STATUS,
            Severity.ERROR,
            Stage.SCHEMA_VALIDATION,
            "status is not in the controlled vocabulary.",
        ),
        RuleDefinition(
            DQ008_INVALID_COUNTRY_CODE,
            Severity.ERROR,
            Stage.SCHEMA_VALIDATION,
            "country / beneficiary_country is not an ISO 3166-1 alpha-2 code.",
        ),
        RuleDefinition(
            DQ009_INVALID_TIMESTAMP,
            Severity.ERROR,
            Stage.SCHEMA_VALIDATION,
            "transaction_timestamp is unparseable or has no timezone and no source "
            "timezone is configured.",
        ),
        RuleDefinition(
            DQ010_REVERSAL_MISSING_ORIGINAL,
            Severity.ERROR,
            Stage.QUALITY_VALIDATION,
            "is_reversal is true but original_transaction_id is missing (or self-referencing).",
        ),
        RuleDefinition(
            DQ011_TIMESTAMP_BOOKING_MISMATCH,
            Severity.WARNING,
            Stage.QUALITY_VALIDATION,
            "The UTC date of transaction_timestamp differs from booking_date by "
            "more than the tolerance.",
        ),
    )
}

ERROR_RULES: frozenset[str] = frozenset(
    r.rule_id for r in RULES.values() if r.severity is Severity.ERROR
)
WARNING_RULES: frozenset[str] = frozenset(
    r.rule_id for r in RULES.values() if r.severity is Severity.WARNING
)


def rule(rule_id: str) -> RuleDefinition:
    """Look up a rule definition; raises ``KeyError`` for unknown identifiers."""
    return RULES[rule_id]


def severity_of(rule_id: str) -> Severity:
    return RULES[rule_id].severity
