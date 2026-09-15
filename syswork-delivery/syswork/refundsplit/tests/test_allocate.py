import pytest

from refundsplit.allocate import Line, Share, allocate
from refundsplit.money import to_cents

WEIGHTED = [
    Line("L-001", to_cents("50.00")),
    Line("L-002", to_cents("30.00")),
    Line("L-003", to_cents("20.00")),
]


def test_splits_in_proportion_to_line_totals():
    shares = allocate(to_cents("100.00"), WEIGHTED)
    assert shares == [
        Share("L-001", to_cents("50.00")),
        Share("L-002", to_cents("30.00")),
        Share("L-003", to_cents("20.00")),
    ]


def test_returns_one_share_per_line_in_input_order():
    shares = allocate(to_cents("75.00"), WEIGHTED)
    assert [s.line_id for s in shares] == ["L-001", "L-002", "L-003"]


def test_no_lines_means_no_shares():
    assert allocate(to_cents("10.00"), []) == []


def test_lines_with_no_value_get_nothing():
    zeroed = [Line("L-001", 0), Line("L-002", 0)]
    assert allocate(to_cents("10.00"), zeroed) == [Share("L-001", 0), Share("L-002", 0)]


def test_partial_refund_is_proportional():
    shares = allocate(to_cents("50.00"), WEIGHTED)
    assert [s.amount_cents for s in shares] == [2500, 1500, 1000]


# --- ENG-4172 ------------------------------------------------------------

EQUAL_THIRDS = [
    Line("L-001", to_cents("10.00")),
    Line("L-002", to_cents("10.00")),
    Line("L-003", to_cents("10.00")),
]


def test_eng_4172_shares_sum_to_the_refund_total():
    """The reported case: $100.00 across three equal lines must not lose a cent."""
    total = to_cents("100.00")
    shares = allocate(total, EQUAL_THIRDS)
    assert sum(s.amount_cents for s in shares) == total


def test_eng_4172_the_extra_cent_goes_somewhere():
    shares = allocate(to_cents("100.00"), EQUAL_THIRDS)
    assert sorted(s.amount_cents for s in shares) == [3333, 3333, 3334]


def test_refund_smaller_than_the_line_count():
    lines = [Line(f"L-{i:03d}", to_cents("10.00")) for i in range(5)]
    shares = allocate(to_cents("0.02"), lines)
    assert sum(s.amount_cents for s in shares) == 2
    assert sorted(s.amount_cents for s in shares) == [0, 0, 0, 1, 1]


def test_refund_reversal_is_also_exact():
    """Negative totals happen when a refund is clawed back."""
    total = -to_cents("100.00")
    shares = allocate(total, EQUAL_THIRDS)
    assert sum(s.amount_cents for s in shares) == total


def test_negative_line_amounts_are_rejected():
    with pytest.raises(ValueError, match="non-negative"):
        allocate(to_cents("10.00"), [Line("L-001", -1)])


def test_the_same_lines_in_a_different_order_get_the_same_shares():
    """Raised in review of the first-pass fix -- see docs/REVIEW.md."""
    total = to_cents("100.00")
    as_entered = {s.line_id: s.amount_cents for s in allocate(total, EQUAL_THIRDS)}
    reordered = {s.line_id: s.amount_cents for s in allocate(total, list(reversed(EQUAL_THIRDS)))}
    assert as_entered == reordered == {"L-001": 3334, "L-002": 3333, "L-003": 3333}


def test_duplicate_line_ids_are_rejected():
    with pytest.raises(ValueError, match="unique"):
        allocate(to_cents("10.00"), [Line("L-001", 100), Line("L-001", 100)])
