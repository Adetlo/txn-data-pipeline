"""Money representation rules.

* Contract and validation boundaries use :class:`decimal.Decimal` (ADR 0001).
* The analytical frame carries ``amount`` as ``decimal(18,4)`` **and** ``amount_minor``
  as an exact ``int64`` in ISO 4217 minor units. Sums are done on ``amount_minor`` and
  converted back with the currency exponent, so no binary float ever touches money.
"""

from __future__ import annotations

from decimal import ROUND_HALF_EVEN, Decimal, InvalidOperation

from txn_pipeline.contracts.enums import CURRENCY_EXPONENT, Currency

#: Storage precision of the curated ``amount`` column: decimal(18, 4).
AMOUNT_PRECISION = 18
AMOUNT_SCALE = 4
MAX_AMOUNT = Decimal(10) ** (AMOUNT_PRECISION - AMOUNT_SCALE) - Decimal(1)


class AmountError(ValueError):
    """Raised when an amount cannot be represented under the contract rules."""


def parse_amount(value: object) -> Decimal:
    """Parse an amount into a finite Decimal without going through float."""
    if isinstance(value, bool):
        raise AmountError("boolean is not an amount")
    if isinstance(value, float):
        # Floats are accepted only from typed sources (Parquet); convert via repr so
        # 0.1 becomes Decimal('0.1') and not the binary expansion.
        value = repr(value)
    if isinstance(value, Decimal):
        amount = value
    elif isinstance(value, int | str):
        text = str(value).strip().replace(",", "") if isinstance(value, str) else str(value)
        if not text:
            raise AmountError("amount is blank")
        try:
            amount = Decimal(text)
        except InvalidOperation as exc:
            raise AmountError(f"amount is not numeric: {value!r}") from exc
    else:
        raise AmountError(f"unsupported amount type {type(value).__name__}")
    if not amount.is_finite():
        raise AmountError("amount is not finite")
    return amount


def exponent_for(currency: Currency) -> int:
    return CURRENCY_EXPONENT[currency]


def validate_amount(amount: Decimal, currency: Currency) -> Decimal:
    """Enforce sign, magnitude and currency-specific scale; return a normalised Decimal."""
    if amount <= 0:
        raise AmountError("amount must be strictly positive")
    if amount > MAX_AMOUNT:
        raise AmountError(f"amount exceeds the maximum representable value {MAX_AMOUNT}")
    exponent = exponent_for(currency)
    quantum = Decimal(1).scaleb(-exponent)
    if amount != amount.quantize(quantum, rounding=ROUND_HALF_EVEN):
        raise AmountError(
            f"amount {amount} has more decimal places than {currency.value} allows ({exponent})"
        )
    return amount.quantize(quantum)


def to_minor_units(amount: Decimal, currency: Currency) -> int:
    """Exact conversion to integer minor units (e.g. GBP 12.34 -> 1234, JPY 500 -> 500)."""
    exponent = exponent_for(currency)
    scaled = amount.scaleb(exponent)
    if scaled != scaled.to_integral_value():
        raise AmountError(f"{amount} {currency.value} is not a whole number of minor units")
    return int(scaled)


def from_minor_units(minor: int, currency: Currency) -> Decimal:
    exponent = exponent_for(currency)
    return (Decimal(minor).scaleb(-exponent)).quantize(Decimal(1).scaleb(-exponent))


def format_amount(amount: Decimal, currency: Currency) -> str:
    """Render with exactly the currency's minor-unit digits (JPY -> no decimals)."""
    exponent = exponent_for(currency)
    return f"{amount.quantize(Decimal(1).scaleb(-exponent)):f}"
