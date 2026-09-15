"""Split a refund across the order lines it applies to.

Public API -- callers in the billing service depend on these names and
signatures:

    Line(line_id, amount_cents)
    Share(line_id, amount_cents)
    allocate(total_cents, lines) -> list[Share]

`allocate` returns one Share per Line, in the same order as the input.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass


@dataclass(frozen=True)
class Line:
    """One order line a refund can be spread across."""

    line_id: str
    amount_cents: int


@dataclass(frozen=True)
class Share:
    """The portion of the refund assigned to one line."""

    line_id: str
    amount_cents: int


def allocate(total_cents: int, lines: Sequence[Line]) -> list[Share]:
    """Spread `total_cents` across `lines` in proportion to their amounts.

    The shares always sum to `total_cents` exactly. Rounding each line
    independently cannot promise that -- three equal lines of a $100.00 refund
    round to $33.33 each and lose a cent -- so instead each line gets the whole
    number of cents its proportion earns, and the cents left over by that
    flooring are handed out one each, to the lines with the largest discarded
    fraction first.

    The arithmetic is integer throughout: no Decimal, no float, no rounding
    mode to get wrong.

    Ties in the discarded fraction are settled on `line_id`, so the same order
    and refund always produce the same per-line shares no matter what order the
    caller passes the lines in.

    The one exception to the sum guarantee: if every line has an amount of
    zero there is no proportion to divide by, and every share is zero. See
    docs/ALLOCATION-POLICY.md.
    """
    if not lines:
        return []

    if any(line.amount_cents < 0 for line in lines):
        raise ValueError("line amounts must be non-negative")

    line_ids = [line.line_id for line in lines]
    if len(set(line_ids)) != len(line_ids):
        raise ValueError("line_ids must be unique: ties are broken on line_id")

    basis = sum(line.amount_cents for line in lines)
    if basis == 0:
        return [Share(line.line_id, 0) for line in lines]

    # Each line's exact share is total_cents * amount / basis. Take the whole
    # cents of that (divmod floors, which keeps the remainder non-negative even
    # when total_cents is negative -- a refund reversal) and keep the remainder
    # to rank who is owed the leftovers.
    whole_cents: list[int] = []
    ranking: list[tuple[int, str, int]] = []
    for index, line in enumerate(lines):
        share, remainder = divmod(total_cents * line.amount_cents, basis)
        whole_cents.append(share)
        ranking.append((remainder, line.line_id, index))

    # Flooring every line leaves 0..len(lines)-1 cents unassigned. Give them to
    # the largest discarded fractions first. Equal fractions are settled on
    # line_id -- never on position in the caller's list, or two services
    # iterating the same order in different orders would disagree about which
    # line owns the cent. See docs/ALLOCATION-POLICY.md.
    leftover = total_cents - sum(whole_cents)
    ranking.sort(key=lambda entry: (-entry[0], entry[1]))
    for _remainder, _line_id, index in ranking[:leftover]:
        whole_cents[index] += 1

    return [Share(line.line_id, cents) for line, cents in zip(lines, whole_cents, strict=True)]
