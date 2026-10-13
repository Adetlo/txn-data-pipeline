"""The canonical financial transaction record (schema version ``financial-transaction/1``).

Every input format (CSV, JSON Lines, Parquet) is mapped onto this model. Validators
normalise values *and* classify failures with stable rule identifiers: a validator
raises ``ValueError("<RULE_ID>: message")`` and the schema stage maps that prefix to a
quarantine rule (see :mod:`txn_pipeline.validation.schema`).

Money is ``Decimal`` here and at every validation boundary (ADR 0001).
"""

from __future__ import annotations

import hashlib
import json
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationInfo,
    field_validator,
    model_validator,
)

from txn_pipeline.contracts import rules
from txn_pipeline.contracts.countries import is_valid_country
from txn_pipeline.contracts.enums import (
    SCHEMA_VERSION,
    SUPPORTED_SCHEMA_VERSIONS,
    Channel,
    Currency,
    TransactionStatus,
    TransactionType,
)
from txn_pipeline.transformation.currency import (
    AmountError,
    parse_amount,
    to_minor_units,
    validate_amount,
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
)

TRANSACTION_ID_MAX_LENGTH = 64
IDENTIFIER_MAX_LENGTH = 64
REFERENCE_MAX_LENGTH = 140

#: Fields that identify the business payload; used for the idempotency / conflict hash.
BUSINESS_FIELDS: tuple[str, ...] = (
    "schema_version",
    "transaction_id",
    "source_system",
    "account_id",
    "counterparty_id",
    "transaction_type",
    "amount",
    "currency",
    "booking_date",
    "value_date",
    "transaction_timestamp",
    "channel",
    "country",
    "reference",
    "status",
    "merchant_category",
    "beneficiary_country",
    "is_reversal",
    "original_transaction_id",
)


def _fail(rule_id: str, message: str) -> ValueError:
    return ValueError(f"{rule_id}: {message}")


def _identifier(value: Any, field: str, rule_id: str) -> str:
    text = normalize_identifier(value)
    if not text:
        raise _fail(rule_id, f"{field} is blank")
    if len(text) > IDENTIFIER_MAX_LENGTH:
        raise _fail(rule_id, f"{field} longer than {IDENTIFIER_MAX_LENGTH} characters")
    if any(ch.isspace() for ch in text):
        raise _fail(rule_id, f"{field} contains whitespace")
    return text


def _country(value: Any, field: str) -> str:
    code = normalize_code(value)
    if not is_valid_country(code):
        raise _fail(
            rules.DQ008_INVALID_COUNTRY_CODE, f"{field} {value!r} is not ISO 3166-1 alpha-2"
        )
    return code


class TransactionRecord(BaseModel):
    """Canonical, normalised transaction. Instances are immutable."""

    model_config = ConfigDict(
        frozen=True,
        extra="ignore",
        str_strip_whitespace=False,  # normalisation is explicit in validators
        populate_by_name=True,
    )

    schema_version: str = SCHEMA_VERSION
    transaction_id: str
    source_system: str
    account_id: str
    counterparty_id: str | None = None
    transaction_type: TransactionType
    amount: Decimal
    currency: Currency
    booking_date: date
    value_date: date
    transaction_timestamp: datetime
    channel: Channel
    country: str
    reference: str | None = None
    status: TransactionStatus
    merchant_category: str | None = None
    beneficiary_country: str | None = None
    is_reversal: bool = False
    original_transaction_id: str | None = None

    #: Original timestamp text as received, preserved for lineage (offset is visible).
    transaction_timestamp_raw: str | None = Field(default=None, repr=False)
    #: Exact integer minor units derived from ``amount`` and ``currency``.
    amount_minor: int = Field(default=0, repr=False)

    # --- pre-processing -------------------------------------------------------------
    @model_validator(mode="before")
    @classmethod
    def _prepare(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            raise _fail(rules.SCH007_UNPARSEABLE_RECORD, "record is not a key/value mapping")
        cleaned: dict[str, Any] = {k: blank_to_none(v) for k, v in data.items()}
        if cleaned.get("schema_version") is None:
            cleaned["schema_version"] = SCHEMA_VERSION
        if cleaned.get("is_reversal") is None:
            cleaned["is_reversal"] = False
        raw_ts = data.get("transaction_timestamp")
        if raw_ts is not None and not isinstance(raw_ts, str):
            raw_ts = raw_ts.isoformat() if hasattr(raw_ts, "isoformat") else str(raw_ts)
        cleaned["transaction_timestamp_raw"] = raw_ts
        cleaned.pop("amount_minor", None)
        return cleaned

    # --- field validators -----------------------------------------------------------
    @field_validator("schema_version", mode="before")
    @classmethod
    def _schema_version(cls, value: Any) -> str:
        text = normalize_identifier(value)
        if text not in SUPPORTED_SCHEMA_VERSIONS:
            raise _fail(
                rules.SCH005_UNSUPPORTED_SCHEMA_VERSION,
                f"schema_version {text!r} not in {sorted(SUPPORTED_SCHEMA_VERSIONS)}",
            )
        return text

    @field_validator("transaction_id", mode="before")
    @classmethod
    def _transaction_id(cls, value: Any) -> str:
        if value is None:
            raise _fail(rules.DQ001_MISSING_TRANSACTION_ID, "transaction_id is missing")
        text = _identifier(value, "transaction_id", rules.DQ001_MISSING_TRANSACTION_ID)
        if len(text) > TRANSACTION_ID_MAX_LENGTH:
            raise _fail(rules.DQ001_MISSING_TRANSACTION_ID, "transaction_id too long")
        return text

    @field_validator("source_system", mode="before")
    @classmethod
    def _source_system(cls, value: Any) -> str:
        if value is None:
            raise _fail(rules.SCH001_MISSING_REQUIRED_FIELD, "source_system is missing")
        return normalize_code(_identifier(value, "source_system", rules.SCH002_INVALID_FIELD_VALUE))

    @field_validator("account_id", mode="before")
    @classmethod
    def _account_id(cls, value: Any) -> str:
        if value is None:
            raise _fail(rules.SCH001_MISSING_REQUIRED_FIELD, "account_id is missing")
        return _identifier(value, "account_id", rules.SCH002_INVALID_FIELD_VALUE)

    @field_validator("counterparty_id", "original_transaction_id", mode="before")
    @classmethod
    def _optional_identifier(cls, value: Any, info: ValidationInfo) -> str | None:
        if value is None:
            return None
        return _identifier(value, str(info.field_name), rules.SCH002_INVALID_FIELD_VALUE)

    @field_validator("transaction_type", mode="before")
    @classmethod
    def _transaction_type(cls, value: Any) -> TransactionType:
        if value is None:
            raise _fail(rules.SCH001_MISSING_REQUIRED_FIELD, "transaction_type is missing")
        code = normalize_code(value).replace(" ", "_").replace("-", "_")
        try:
            return TransactionType(code)
        except ValueError:
            raise _fail(
                rules.SCH003_UNKNOWN_TRANSACTION_TYPE, f"unknown transaction_type {value!r}"
            ) from None

    @field_validator("currency", mode="before")
    @classmethod
    def _currency(cls, value: Any) -> Currency:
        if value is None:
            raise _fail(rules.DQ002_INVALID_CURRENCY, "currency is missing")
        try:
            return Currency(normalize_code(value))
        except ValueError:
            raise _fail(rules.DQ002_INVALID_CURRENCY, f"unsupported currency {value!r}") from None

    @field_validator("amount", mode="before")
    @classmethod
    def _amount(cls, value: Any) -> Decimal:
        if value is None:
            raise _fail(rules.DQ003_INVALID_AMOUNT, "amount is missing")
        try:
            return parse_amount(value)
        except AmountError as exc:
            raise _fail(rules.DQ003_INVALID_AMOUNT, str(exc)) from None

    @field_validator("booking_date", "value_date", mode="before")
    @classmethod
    def _dates(cls, value: Any, info: ValidationInfo) -> date:
        if value is None:
            raise _fail(rules.SCH001_MISSING_REQUIRED_FIELD, f"{info.field_name} is missing")
        try:
            return parse_date(value)
        except ValueError as exc:
            raise _fail(rules.SCH006_INVALID_DATE_FORMAT, f"{info.field_name}: {exc}") from None

    @field_validator("transaction_timestamp", mode="before")
    @classmethod
    def _timestamp(cls, value: Any, info: ValidationInfo) -> datetime:
        if value is None:
            raise _fail(rules.DQ009_INVALID_TIMESTAMP, "transaction_timestamp is missing")
        context = info.context or {}
        try:
            tz = resolve_timezone(context.get("source_timezone"))
            return parse_timestamp(value, tz)
        except TimestampError as exc:
            raise _fail(rules.DQ009_INVALID_TIMESTAMP, str(exc)) from None

    @field_validator("channel", mode="before")
    @classmethod
    def _channel(cls, value: Any) -> Channel:
        if value is None:
            raise _fail(rules.SCH001_MISSING_REQUIRED_FIELD, "channel is missing")
        try:
            return Channel(normalize_code(value))
        except ValueError:
            raise _fail(rules.SCH004_UNKNOWN_CHANNEL, f"unknown channel {value!r}") from None

    @field_validator("country", mode="before")
    @classmethod
    def _country_code(cls, value: Any) -> str:
        if value is None:
            raise _fail(rules.SCH001_MISSING_REQUIRED_FIELD, "country is missing")
        return _country(value, "country")

    @field_validator("beneficiary_country", mode="before")
    @classmethod
    def _beneficiary_country(cls, value: Any) -> str | None:
        return None if value is None else _country(value, "beneficiary_country")

    @field_validator("reference", mode="before")
    @classmethod
    def _reference(cls, value: Any) -> str | None:
        if value is None:
            return None
        text = normalize_text(value)
        if len(text) > REFERENCE_MAX_LENGTH:
            raise _fail(rules.SCH002_INVALID_FIELD_VALUE, "reference too long")
        return text or None

    @field_validator("status", mode="before")
    @classmethod
    def _status(cls, value: Any) -> TransactionStatus:
        if value is None:
            raise _fail(rules.DQ007_UNKNOWN_STATUS, "status is missing")
        try:
            return TransactionStatus(normalize_code(value))
        except ValueError:
            raise _fail(rules.DQ007_UNKNOWN_STATUS, f"unknown status {value!r}") from None

    @field_validator("merchant_category", mode="before")
    @classmethod
    def _merchant_category(cls, value: Any) -> str | None:
        return None if value is None else normalize_code(normalize_text(value))

    @field_validator("is_reversal", mode="before")
    @classmethod
    def _is_reversal(cls, value: Any) -> bool:
        try:
            return parse_bool(value)
        except ValueError as exc:
            raise _fail(rules.SCH002_INVALID_FIELD_VALUE, f"is_reversal: {exc}") from None

    # --- cross-field rules ----------------------------------------------------------
    @model_validator(mode="after")
    def _finalise(self) -> TransactionRecord:
        try:
            amount = validate_amount(self.amount, self.currency)
            minor = to_minor_units(amount, self.currency)
        except AmountError as exc:
            raise _fail(rules.DQ003_INVALID_AMOUNT, str(exc)) from None
        # frozen model: use object.__setattr__ for the derived fields
        object.__setattr__(self, "amount", amount)
        object.__setattr__(self, "amount_minor", minor)
        return self

    # --- derived views ------------------------------------------------------------------
    def business_payload(self) -> dict[str, Any]:
        """JSON-safe canonical payload (business fields only, no lineage fields)."""
        dumped = self.model_dump(mode="json", include=set(BUSINESS_FIELDS))
        dumped["amount"] = f"{self.amount:f}"
        return dumped

    def payload_hash(self) -> str:
        """SHA-256 over the canonical payload; equal hashes mean idempotent duplicates."""
        canonical = json.dumps(self.business_payload(), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def parse_rule_prefix(message: str) -> tuple[str | None, str]:
    """Split ``"DQ002_INVALID_CURRENCY: unsupported ..."`` into (rule_id, message)."""
    head, sep, tail = message.partition(": ")
    if sep and head in rules.RULES:
        return head, tail
    return None, message
