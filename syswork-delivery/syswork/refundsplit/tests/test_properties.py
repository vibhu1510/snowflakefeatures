"""Property-based tests: assertions that must hold for *every* input.

Hypothesis generates the inputs, including the ones nobody would think to
type into a test by hand.
"""

from hypothesis import assume, given, settings
from hypothesis import strategies as st

from refundsplit.allocate import Line, allocate

line_amounts = st.lists(st.integers(min_value=0, max_value=1_000_000), min_size=1, max_size=12)
totals = st.integers(min_value=0, max_value=1_000_000)


@given(total_cents=totals, amounts=line_amounts)
def test_one_share_per_line_in_the_same_order(total_cents, amounts):
    lines = [Line(f"L-{i:03d}", amount) for i, amount in enumerate(amounts)]
    shares = allocate(total_cents, lines)
    assert [s.line_id for s in shares] == [line.line_id for line in lines]


@settings(max_examples=500)
@given(total_cents=totals, amounts=line_amounts)
def test_shares_always_sum_to_the_refund_total(total_cents, amounts):
    """ENG-4172. The invariant the ticket exists to protect.

    Carve-out: an order whose lines are all worth zero has no proportion to
    divide by, so every share is zero. `assume` skips those inputs; the
    behaviour is deliberate and documented in docs/ALLOCATION-POLICY.md.
    """
    assume(sum(amounts) > 0)
    lines = [Line(f"L-{i:03d}", amount) for i, amount in enumerate(amounts)]
    shares = allocate(total_cents, lines)
    assert sum(s.amount_cents for s in shares) == total_cents


@settings(max_examples=500)
@given(total_cents=totals, amounts=line_amounts, data=st.data())
def test_allocation_does_not_depend_on_the_order_lines_are_passed_in(total_cents, amounts, data):
    """Raised in review of the ENG-4172 fix -- see docs/REVIEW.md.

    Two callers holding the same order and the same refund must agree on what
    each line gets, whatever order they happen to iterate the lines in. The
    sum invariant cannot see this: the total is right either way.
    """
    assume(sum(amounts) > 0)
    lines = [Line(f"L-{i:03d}", amount) for i, amount in enumerate(amounts)]
    shuffled = data.draw(st.permutations(lines))

    as_entered = {s.line_id: s.amount_cents for s in allocate(total_cents, lines)}
    reordered = {s.line_id: s.amount_cents for s in allocate(total_cents, shuffled)}
    assert as_entered == reordered
