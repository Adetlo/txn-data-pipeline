"""Generate realistic-looking but entirely fictional financial transactions.

Determinism: the same ``seed`` (and anchor date) always yields byte-identical output.
Identifiers are deliberately synthetic (``ACC-SYN-``, ``CP-SYN-``, ``TXN-SYN-``) so they can
never be mistaken for live customer data.
"""

from __future__ import annotations

import csv
import json
import random
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from txn_pipeline.contracts.enums import (
    CURRENCY_EXPONENT,
    SCHEMA_VERSION,
    Channel,
    Currency,
    SourceFormat,
    TransactionStatus,
    TransactionType,
)

COLUMNS: tuple[str, ...] = (
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
    "schema_version",
)

DEFAULT_ANCHOR_DATE = date(2026, 10, 13)
SOURCE_SYSTEM = "SYNTHETIC_CORE"

_CURRENCY_WEIGHTS = {
    Currency.GBP: 45,
    Currency.EUR: 20,
    Currency.USD: 18,
    Currency.JPY: 5,
    Currency.CHF: 4,
    Currency.SGD: 4,
    Currency.AUD: 4,
}
_CURRENCY_COUNTRIES: dict[Currency, list[str]] = {
    Currency.GBP: ["GB"] * 9 + ["IE"],
    Currency.EUR: ["DE", "FR", "NL", "ES", "IT", "IE"],
    Currency.USD: ["US"] * 8 + ["CA", "MX"],
    Currency.JPY: ["JP"],
    Currency.CHF: ["CH"],
    Currency.SGD: ["SG"],
    Currency.AUD: ["AU"],
}
_TYPE_WEIGHTS = {
    TransactionType.CARD_PURCHASE: 32,
    TransactionType.PAYMENT: 20,
    TransactionType.TRANSFER: 18,
    TransactionType.DIRECT_DEBIT: 10,
    TransactionType.DEPOSIT: 8,
    TransactionType.WITHDRAWAL: 6,
    TransactionType.FEE: 3,
    TransactionType.REFUND: 2,
    TransactionType.INTEREST: 1,
}
_CHANNEL_BY_TYPE: dict[TransactionType, list[Channel]] = {
    TransactionType.CARD_PURCHASE: [Channel.CARD] * 9 + [Channel.ONLINE],
    TransactionType.PAYMENT: [Channel.ONLINE, Channel.MOBILE, Channel.MOBILE, Channel.API],
    TransactionType.TRANSFER: [Channel.ONLINE, Channel.MOBILE, Channel.BRANCH, Channel.API],
    TransactionType.DIRECT_DEBIT: [Channel.BATCH],
    TransactionType.DEPOSIT: [Channel.BRANCH, Channel.ATM, Channel.BATCH],
    TransactionType.WITHDRAWAL: [Channel.ATM] * 4 + [Channel.BRANCH],
    TransactionType.FEE: [Channel.BATCH],
    TransactionType.REFUND: [Channel.CARD, Channel.ONLINE],
    TransactionType.INTEREST: [Channel.BATCH],
}
_STATUS_WEIGHTS = {
    TransactionStatus.POSTED: 90,
    TransactionStatus.PENDING: 6,
    TransactionStatus.REVERSED: 3,
    TransactionStatus.CANCELLED: 1,
}
_MERCHANT_CATEGORIES = ["GROCERY", "FUEL", "RESTAURANT", "TRAVEL", "UTILITIES", "RETAIL", "DIGITAL"]
_OFFSETS = ["+00:00", "+00:00", "+00:00", "+01:00", "+02:00", "+08:00", "+09:00", "-05:00", "Z"]

INVALID_KINDS: tuple[str, ...] = (
    "missing_transaction_id",
    "invalid_currency",
    "invalid_amount_text",
    "negative_amount",
    "amount_scale_too_fine",
    "booking_before_range",
    "unknown_status",
    "invalid_country",
    "naive_timestamp",
    "reversal_missing_original",
    "invalid_date_format",
    "unknown_transaction_type",
)


@dataclass(slots=True)
class GenerationSummary:
    records_requested: int
    valid_records: int
    invalid_records: int
    duplicate_records: int
    conflict_records: int
    warning_records: int
    total_rows: int
    invalid_by_kind: dict[str, int] = field(default_factory=dict)

    def as_dict(self) -> dict[str, object]:
        return {
            "records_requested": self.records_requested,
            "valid_records": self.valid_records,
            "invalid_records": self.invalid_records,
            "duplicate_records": self.duplicate_records,
            "conflict_records": self.conflict_records,
            "warning_records": self.warning_records,
            "total_rows": self.total_rows,
            "invalid_by_kind": dict(sorted(self.invalid_by_kind.items())),
        }


@dataclass(frozen=True, slots=True)
class GeneratorConfig:
    records: int = 10_000
    seed: int = 42
    invalid_rate: float = 0.02
    duplicate_rate: float = 0.01
    conflict_rate: float = 0.005
    warning_rate: float = 0.01
    anchor_date: date = DEFAULT_ANCHOR_DATE
    days_back: int = 90
    reversal_rate: float = 0.02

    def __post_init__(self) -> None:
        if self.records < 0:
            raise ValueError("records must be >= 0")
        for name in (
            "invalid_rate",
            "duplicate_rate",
            "conflict_rate",
            "warning_rate",
            "reversal_rate",
        ):
            value = getattr(self, name)
            if not 0.0 <= value <= 1.0:
                raise ValueError(f"{name} must be between 0 and 1")


Row = dict[str, str]


def _amount(rng: random.Random, currency: Currency, txn_type: TransactionType) -> Decimal:
    mu = {
        TransactionType.FEE: 1.5,
        TransactionType.INTEREST: 1.0,
        TransactionType.TRANSFER: 5.0,
    }.get(txn_type, 3.6)
    value = rng.lognormvariate(mu, 1.1)
    if currency is Currency.JPY:
        value *= 150
    exponent = CURRENCY_EXPONENT[currency]
    quantum = Decimal(1).scaleb(-exponent)
    amount = Decimal(f"{value:.4f}").quantize(quantum)
    if amount <= 0:
        amount = quantum
    return amount


def _valid_row(
    rng: random.Random, seq: int, cfg: GeneratorConfig, n_accounts: int, earlier_ids: list[str]
) -> Row:
    currency = rng.choices(list(_CURRENCY_WEIGHTS), weights=list(_CURRENCY_WEIGHTS.values()))[0]
    txn_type = rng.choices(list(_TYPE_WEIGHTS), weights=list(_TYPE_WEIGHTS.values()))[0]
    channel = rng.choice(_CHANNEL_BY_TYPE[txn_type])
    status = rng.choices(list(_STATUS_WEIGHTS), weights=list(_STATUS_WEIGHTS.values()))[0]
    country = rng.choice(_CURRENCY_COUNTRIES[currency])
    booking = cfg.anchor_date - timedelta(days=rng.randint(0, cfg.days_back))
    value_gap = rng.choices([0, 1, 2], weights=[80, 15, 5])[0]
    value_date = booking + timedelta(days=value_gap)
    offset = rng.choice(_OFFSETS)
    ts = datetime.combine(booking, datetime.min.time()) + timedelta(
        hours=rng.randint(6, 22), minutes=rng.randint(0, 59), seconds=rng.randint(0, 59)
    )
    timestamp = ts.isoformat() + ("Z" if offset == "Z" else offset)
    account = f"ACC-SYN-{rng.randint(1, n_accounts):06d}"
    if txn_type is TransactionType.TRANSFER and rng.random() < 0.6:
        counterparty = f"ACC-SYN-{rng.randint(1, n_accounts):06d}"
    elif txn_type in {TransactionType.FEE, TransactionType.INTEREST}:
        counterparty = ""
    else:
        counterparty = f"CP-SYN-{rng.randint(1, max(100, n_accounts * 3)):06d}"
    is_reversal = bool(earlier_ids) and rng.random() < cfg.reversal_rate
    original = rng.choice(earlier_ids) if is_reversal else ""
    beneficiary = (
        country
        if rng.random() < 0.85
        else rng.choice([c for cs in _CURRENCY_COUNTRIES.values() for c in cs])
    )
    return {
        "transaction_id": f"TXN-SYN-{booking:%Y%m%d}-{seq:06d}",
        "source_system": SOURCE_SYSTEM,
        "account_id": account,
        "counterparty_id": counterparty,
        "transaction_type": txn_type.value,
        "amount": f"{_amount(rng, currency, txn_type):f}",
        "currency": currency.value,
        "booking_date": booking.isoformat(),
        "value_date": value_date.isoformat(),
        "transaction_timestamp": timestamp,
        "channel": channel.value,
        "country": country,
        "reference": f"SYN-{txn_type.value[:3]}-{seq:06d}",
        "status": status.value,
        "merchant_category": rng.choice(_MERCHANT_CATEGORIES)
        if txn_type is TransactionType.CARD_PURCHASE
        else "",
        "beneficiary_country": beneficiary
        if txn_type in {TransactionType.PAYMENT, TransactionType.TRANSFER}
        else "",
        "is_reversal": "true" if is_reversal else "false",
        "original_transaction_id": original,
        "schema_version": SCHEMA_VERSION,
    }


def _corrupt(rng: random.Random, row: Row, kind: str) -> Row:
    bad = dict(row)
    if kind == "missing_transaction_id":
        bad["transaction_id"] = ""
    elif kind == "invalid_currency":
        bad["currency"] = rng.choice(["GBX", "XXX", "EURO", "12"])
    elif kind == "invalid_amount_text":
        bad["amount"] = rng.choice(["abc", "12.3.4", "", "1e400x"])
    elif kind == "negative_amount":
        bad["amount"] = "-" + bad["amount"]
    elif kind == "amount_scale_too_fine":
        bad["amount"] = bad["amount"] + ("5" if bad["currency"] != "JPY" else ".5")
    elif kind == "booking_before_range":
        bad["booking_date"] = "1999-12-31"
        bad["value_date"] = "1999-12-31"
    elif kind == "unknown_status":
        bad["status"] = rng.choice(["DONE", "OK", "SETTLED?"])
    elif kind == "invalid_country":
        bad["country"] = rng.choice(["UKX", "XX", "GBR", "1"])
    elif kind == "naive_timestamp":
        bad["transaction_timestamp"] = bad["transaction_timestamp"].rstrip("Z")[:19]
    elif kind == "reversal_missing_original":
        bad["is_reversal"] = "true"
        bad["original_transaction_id"] = ""
    elif kind == "invalid_date_format":
        y, m, d = bad["booking_date"].split("-")
        bad["booking_date"] = f"{d}/{m}/{y}"
    elif kind == "unknown_transaction_type":
        bad["transaction_type"] = rng.choice(["WIRE", "CHEQUE", "???"])
    else:  # pragma: no cover - guarded by INVALID_KINDS
        raise ValueError(f"unknown invalid kind {kind}")
    return bad


def _warning_variant(rng: random.Random, row: Row) -> Row:
    warm = dict(row)
    booking = date.fromisoformat(row["booking_date"])
    warm["value_date"] = (booking + timedelta(days=rng.randint(10, 40))).isoformat()
    return warm


def _idempotent_variant(row: Row) -> Row:
    """Same business payload, different raw bytes (case / whitespace only)."""
    variant = dict(row)
    variant["currency"] = row["currency"].lower()
    variant["reference"] = "  " + row["reference"] + "  "
    variant["status"] = row["status"].capitalize()
    return variant


def _conflict_variant(rng: random.Random, row: Row) -> Row:
    conflict = dict(row)
    amount = Decimal(row["amount"])
    quantum = Decimal(1).scaleb(-CURRENCY_EXPONENT[Currency(row["currency"])])
    conflict["amount"] = f"{(amount + quantum * rng.randint(1, 500)):f}"
    return conflict


def generate(cfg: GeneratorConfig) -> tuple[list[Row], GenerationSummary]:
    rng = random.Random(cfg.seed)  # noqa: S311  # nosec B311
    n_accounts = max(50, cfg.records // 40)
    invalid_target = round(cfg.records * cfg.invalid_rate)
    warning_target = round(cfg.records * cfg.warning_rate)
    duplicate_target = round(cfg.records * cfg.duplicate_rate)
    conflict_target = round(cfg.records * cfg.conflict_rate)

    rows: list[Row] = []
    valid_rows: list[Row] = []
    earlier_ids: list[str] = []
    invalid_by_kind: dict[str, int] = {}

    # Decide upfront which sequence numbers are corrupted / warned so output is deterministic.
    indices = list(range(cfg.records))
    rng.shuffle(indices)
    invalid_idx = set(indices[:invalid_target])
    warning_idx = set(indices[invalid_target : invalid_target + warning_target])

    for seq in range(1, cfg.records + 1):
        row = _valid_row(rng, seq, cfg, n_accounts, earlier_ids)
        if seq - 1 in invalid_idx:
            kind = rng.choice(INVALID_KINDS)
            invalid_by_kind[kind] = invalid_by_kind.get(kind, 0) + 1
            rows.append(_corrupt(rng, row, kind))
            continue
        if seq - 1 in warning_idx:
            row = _warning_variant(rng, row)
        rows.append(row)
        valid_rows.append(row)
        if row["is_reversal"] == "false" and len(earlier_ids) < 5_000:
            earlier_ids.append(row["transaction_id"])

    duplicates: list[Row] = []
    conflicts: list[Row] = []
    if valid_rows:
        for i in range(duplicate_target):
            source = rng.choice(valid_rows)
            duplicates.append(_idempotent_variant(source) if i % 3 == 2 else dict(source))
        conflict_sources = rng.sample(valid_rows, k=min(conflict_target, len(valid_rows)))
        conflicts = [_conflict_variant(rng, s) for s in conflict_sources]

    extras = duplicates + conflicts
    # Interleave extras at random positions after their originals would typically appear.
    for extra in extras:
        rows.insert(rng.randint(0, len(rows)), extra)

    summary = GenerationSummary(
        records_requested=cfg.records,
        valid_records=len(valid_rows),
        invalid_records=sum(invalid_by_kind.values()),
        duplicate_records=len(duplicates),
        conflict_records=len(conflicts),
        warning_records=len(warning_idx),
        total_rows=len(rows),
        invalid_by_kind=invalid_by_kind,
    )
    return rows, summary


# --- writers -------------------------------------------------------------------------------
def write_csv(rows: Sequence[Row], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=COLUMNS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def write_jsonl(rows: Sequence[Row], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for row in rows:
            record: dict[str, str | bool | None] = {
                k: (v if v != "" else None) for k, v in row.items()
            }
            record["is_reversal"] = row["is_reversal"] == "true"
            fh.write(json.dumps(record, separators=(",", ":")) + "\n")


def _typed_or_string(values: list[str], typed: pa.DataType, convert: object) -> pa.Array:
    """Use the typed Arrow column when every value converts; otherwise keep strings."""
    try:
        converted = [None if v == "" else convert(v) for v in values]  # type: ignore[operator]
        return pa.array(converted, type=typed)
    except (ValueError, ArithmeticError, TypeError, pa.ArrowInvalid):
        return pa.array([None if v == "" else v for v in values], type=pa.string())


def write_parquet(rows: Sequence[Row], path: Path) -> None:
    """Typed columns (decimal / date / bool) when the data is clean; strings otherwise."""
    path.parent.mkdir(parents=True, exist_ok=True)
    columns: dict[str, pa.Array] = {}
    for name in COLUMNS:
        values = [r[name] for r in rows]
        if name == "amount":
            columns[name] = _typed_or_string(values, pa.decimal128(18, 4), Decimal)
        elif name in {"booking_date", "value_date"}:
            columns[name] = _typed_or_string(values, pa.date32(), date.fromisoformat)
        elif name == "is_reversal":
            columns[name] = _typed_or_string(
                values, pa.bool_(), lambda v: {"true": True, "false": False}[v]
            )
        else:
            columns[name] = pa.array([None if v == "" else v for v in values], type=pa.string())
    pq.write_table(pa.table(columns), path, compression="zstd")


def write_rows(rows: Sequence[Row], path: Path, fmt: SourceFormat) -> None:
    writers: dict[SourceFormat, Callable[[Sequence[Row], Path], None]] = {
        SourceFormat.CSV: write_csv,
        SourceFormat.JSONL: write_jsonl,
        SourceFormat.PARQUET: write_parquet,
    }
    writers[fmt](rows, path)
