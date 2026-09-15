"""Exact money handling.

Money is held as an integer number of cents everywhere inside this package.
Decimal values only exist at the boundary, where humans and JSON hand us
strings. Floats are rejected outright: 0.1 + 0.2 is not 0.3, and a refund
system that quietly disagrees with a bank statement is worse than one that
refuses to start.
"""

from __future__ import annotations

from decimal import Decimal, InvalidOperation

CENT = Decimal("0.01")


class MoneyError(ValueError):
    """Raised when a value cannot be represented exactly as whole cents."""


def to_cents(amount: Decimal | str) -> int:
    """Convert a Decimal or decimal string to a whole number of cents.

    Accepts Decimal and str only. Floats are rejected because they cannot
    represent most decimal fractions exactly. Values with sub-cent precision
    are rejected rather than silently rounded -- the caller has to decide what
    rounding means for their case, so we refuse to guess.
    """
    if isinstance(amount, bool) or isinstance(amount, float):
        raise MoneyError(f"floats are not accepted in the money path: {amount!r}")
    if isinstance(amount, Decimal):
        value = amount
    elif isinstance(amount, str):
        try:
            value = Decimal(amount.strip())
        except InvalidOperation as exc:
            raise MoneyError(f"not a decimal amount: {amount!r}") from exc
    else:
        raise MoneyError(f"expected Decimal or str, got {type(amount).__name__}")

    if not value.is_finite():
        raise MoneyError(f"amount must be finite: {amount!r}")

    scaled = value * 100
    if scaled != scaled.to_integral_value():
        raise MoneyError(f"amount has sub-cent precision: {amount!r}")
    return int(scaled)


def from_cents(cents: int) -> Decimal:
    """Convert whole cents back to a two-place Decimal."""
    return (Decimal(cents) / 100).quantize(CENT)


def format_money(cents: int) -> str:
    """Render whole cents for display, e.g. 10000 -> '$100.00'."""
    sign = "-" if cents < 0 else ""
    return f"{sign}${from_cents(abs(cents))}"
