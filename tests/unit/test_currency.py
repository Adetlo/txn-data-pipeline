"""Decimal money rules: parsing, scale validation, minor units, formatting."""

from __future__ import annotations

from decimal import Decimal

import pytest

from txn_pipeline.contracts.enums import Currency
from txn_pipeline.transformation.currency import (
    MAX_AMOUNT,
    AmountError,
    format_amount,
    from_minor_units,
    parse_amount,
    to_minor_units,
    validate_amount,
)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("250.00", Decimal("250.00")),
        (" 1,250.50 ", Decimal("1250.50")),
        (250, Decimal("250")),
        (Decimal("0.01"), Decimal("0.01")),
        (0.1, Decimal("0.1")),
        (1e-2, Decimal("0.01")),
    ],
)
def test_parse_amount(value: object, expected: Decimal) -> None:
    assert parse_amount(value) == expected


@pytest.mark.parametrize("value", ["abc", "", "   ", "NaN", "inf", True, None, [1]])
def test_parse_amount_rejects(value: object) -> None:
    with pytest.raises(AmountError):
        parse_amount(value)


def test_validate_amount_quantizes_to_currency_scale() -> None:
    assert validate_amount(Decimal("5"), Currency.GBP) == Decimal("5.00")
    assert str(validate_amount(Decimal("5"), Currency.GBP)) == "5.00"
    assert validate_amount(Decimal("1500.00"), Currency.JPY) == Decimal("1500")


@pytest.mark.parametrize(
    ("amount", "currency"),
    [
        (Decimal("0"), Currency.GBP),
        (Decimal("-1"), Currency.GBP),
        (Decimal("1.005"), Currency.GBP),
        (Decimal("1.5"), Currency.JPY),
        (MAX_AMOUNT + 1, Currency.USD),
    ],
)
def test_validate_amount_rejects(amount: Decimal, currency: Currency) -> None:
    with pytest.raises(AmountError):
        validate_amount(amount, currency)


def test_minor_units_round_trip() -> None:
    assert to_minor_units(Decimal("12.34"), Currency.GBP) == 1234
    assert to_minor_units(Decimal("500"), Currency.JPY) == 500
    assert from_minor_units(1234, Currency.GBP) == Decimal("12.34")
    assert from_minor_units(500, Currency.JPY) == Decimal("500")
    assert from_minor_units(0, Currency.EUR) == Decimal("0.00")
    with pytest.raises(AmountError):
        to_minor_units(Decimal("12.345"), Currency.GBP)


def test_format_amount_respects_minor_units() -> None:
    assert format_amount(Decimal("1050025.5"), Currency.GBP) == "1050025.50"
    assert format_amount(Decimal("1500"), Currency.JPY) == "1500"
    assert format_amount(Decimal("0"), Currency.CHF) == "0.00"


def test_no_binary_float_artefacts() -> None:
    total = sum((Decimal("0.10") for _ in range(10)), Decimal("0"))
    assert total == Decimal("1.00")
    assert to_minor_units(total, Currency.GBP) == 100
