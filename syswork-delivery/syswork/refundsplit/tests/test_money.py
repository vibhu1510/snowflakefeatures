from decimal import Decimal

import pytest

from refundsplit.money import MoneyError, format_money, from_cents, to_cents


def test_accepts_decimal_and_string():
    assert to_cents(Decimal("100.00")) == 10000
    assert to_cents("0.07") == 7
    assert to_cents("-12.34") == -1234


def test_rejects_floats():
    with pytest.raises(MoneyError):
        to_cents(0.1 + 0.2)


def test_rejects_sub_cent_precision():
    with pytest.raises(MoneyError):
        to_cents("0.005")


def test_round_trips():
    assert from_cents(10000) == Decimal("100.00")
    assert format_money(10000) == "$100.00"
    assert format_money(-1) == "-$0.01"
