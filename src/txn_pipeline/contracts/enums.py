"""Controlled vocabularies used by the canonical transaction contract."""

from __future__ import annotations

from enum import StrEnum

SCHEMA_VERSION = "financial-transaction/1"
SUPPORTED_SCHEMA_VERSIONS: frozenset[str] = frozenset({SCHEMA_VERSION})


class Currency(StrEnum):
    """ISO 4217 currencies supported by the contract (minor-unit exponent in CURRENCY_EXPONENT)."""

    GBP = "GBP"
    USD = "USD"
    EUR = "EUR"
    JPY = "JPY"
    CHF = "CHF"
    SGD = "SGD"
    AUD = "AUD"


#: ISO 4217 minor-unit exponent per currency (JPY has no minor unit).
CURRENCY_EXPONENT: dict[Currency, int] = {
    Currency.GBP: 2,
    Currency.USD: 2,
    Currency.EUR: 2,
    Currency.JPY: 0,
    Currency.CHF: 2,
    Currency.SGD: 2,
    Currency.AUD: 2,
}

#: Full currency names for dim_currency (public ISO 4217 data).
CURRENCY_NAME: dict[Currency, str] = {
    Currency.GBP: "Pound Sterling",
    Currency.USD: "US Dollar",
    Currency.EUR: "Euro",
    Currency.JPY: "Yen",
    Currency.CHF: "Swiss Franc",
    Currency.SGD: "Singapore Dollar",
    Currency.AUD: "Australian Dollar",
}


class TransactionType(StrEnum):
    TRANSFER = "TRANSFER"
    PAYMENT = "PAYMENT"
    DIRECT_DEBIT = "DIRECT_DEBIT"
    CARD_PURCHASE = "CARD_PURCHASE"
    DEPOSIT = "DEPOSIT"
    WITHDRAWAL = "WITHDRAWAL"
    FEE = "FEE"
    REFUND = "REFUND"
    INTEREST = "INTEREST"


class TransactionStatus(StrEnum):
    PENDING = "PENDING"
    POSTED = "POSTED"
    REVERSED = "REVERSED"
    CANCELLED = "CANCELLED"


class Channel(StrEnum):
    ONLINE = "ONLINE"
    MOBILE = "MOBILE"
    BRANCH = "BRANCH"
    ATM = "ATM"
    API = "API"
    CARD = "CARD"
    BATCH = "BATCH"


class Direction(StrEnum):
    """Money flow relative to the booked account (enrichment, see docs/DATA_CONTRACT.md)."""

    INBOUND = "INBOUND"
    OUTBOUND = "OUTBOUND"
    INTERNAL = "INTERNAL"


#: Transaction types that always move money *into* the booked account.
INBOUND_TYPES: frozenset[TransactionType] = frozenset(
    {TransactionType.DEPOSIT, TransactionType.REFUND, TransactionType.INTEREST}
)


class AmountBand(StrEnum):
    MICRO = "MICRO"
    SMALL = "SMALL"
    MEDIUM = "MEDIUM"
    LARGE = "LARGE"


class Severity(StrEnum):
    """ERROR rejects the record (quarantine); WARNING accepts it but records the finding."""

    ERROR = "ERROR"
    WARNING = "WARNING"


class Stage(StrEnum):
    """Pipeline stages, in execution order. Also used as checkpoint names."""

    DISCOVERY = "DISCOVERY"
    REGISTRATION = "REGISTRATION"
    INGESTION = "INGESTION"
    SCHEMA_VALIDATION = "SCHEMA_VALIDATION"
    QUALITY_VALIDATION = "QUALITY_VALIDATION"
    DEDUPLICATION = "DEDUPLICATION"
    TRANSFORMATION = "TRANSFORMATION"
    RECONCILIATION = "RECONCILIATION"
    QUARANTINE_WRITE = "QUARANTINE_WRITE"
    CURATED_WRITE = "CURATED_WRITE"
    REGISTRY_COMMIT = "REGISTRY_COMMIT"
    WAREHOUSE = "WAREHOUSE"
    COMPLETION = "COMPLETION"


class BatchStatus(StrEnum):
    DISCOVERED = "DISCOVERED"
    RUNNING = "RUNNING"
    VALIDATED = "VALIDATED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    PARTIAL = "PARTIAL"


#: Statuses from which a batch may be (re)started.
RESTARTABLE_STATUSES: frozenset[BatchStatus] = frozenset(
    {BatchStatus.DISCOVERED, BatchStatus.RUNNING, BatchStatus.FAILED, BatchStatus.PARTIAL}
)


class CheckpointStatus(StrEnum):
    STARTED = "STARTED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"


class DuplicateKind(StrEnum):
    """Why a record was classified as a duplicate (not a conflict)."""

    EXACT_DUPLICATE = "EXACT_DUPLICATE"  # byte-identical raw row within the batch
    IDEMPOTENT_DUPLICATE = "IDEMPOTENT_DUPLICATE"  # same id, same canonical payload, within batch
    CROSS_BATCH_DUPLICATE = "CROSS_BATCH_DUPLICATE"  # same id + payload already curated earlier


class SourceFormat(StrEnum):
    CSV = "csv"
    JSONL = "jsonl"
    PARQUET = "parquet"


class Actor(StrEnum):
    CLI = "CLI"
    SYSTEM = "SYSTEM"
    TEST = "TEST"
