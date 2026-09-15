# Review — ENG-4172 fix (the largest-remainder commit)

**Reviewer** _sign here before you present this_
**Verdict** Request changes. The reported bug is fixed and the arithmetic is
right. One thing the tests cannot see needs addressing before this ships.

> **Provenance, stated plainly.** This document was drafted during preparation
> by the same agent that wrote the fix, on a second pass whose only instruction
> was to attack the change rather than defend it. The finding is real — it was
> reproduced by running the code before it was written up, and the failing
> property is captured in `../../evidence/04-order-dependence-fails.txt`. But a
> model reviewing its own work is not the same thing as human review. Read the
> diff, confirm the finding yourself, and sign it before you put your name on
> it.

## What I checked, and how

I read the diff before running anything, then ran the suite, then went looking
for what the tests were *not* asserting. That last step is where this finding
came from — not from the diff being wrong, but from the test file being
complete about one invariant and silent about another.

## Accepted

- **The sum invariant.** Largest-remainder is the right approach, and doing it
  in integer cents removes the whole class of rounding-mode arguments. The
  `divmod` choice is doing real work on negative totals: flooring keeps the
  remainder non-negative, so the leftover count stays in `0..n-1` instead of
  going negative on a clawback. A comment says so, which I appreciated.
- **The failing test came first,** and the commit message records the failure
  it started from (`9999 != 10000`). I can verify the claim rather than trust it.
- **Rejecting negative line amounts.** Out of the ticket's literal scope, but
  the old code accepted them and produced nonsense, so closing it here is right.
- **The zero-basis carve-out is declared** in the commit message instead of
  being left for someone to discover. That is the behaviour I want.

## Finding 1 — the allocation depends on the order the lines are passed in

**Blocking.**

When several lines have the same discarded fraction — which is every equal
split, the most common shape we have — the tie is broken by position in the
input list. So the extra cent lands on whichever line the caller happened to
put first:

```
as entered   L-001=$33.34 L-002=$33.33 L-003=$33.33
reordered    L-001=$33.33 L-002=$33.33 L-003=$33.34
```

Same order, same refund, same lines. Different answer per line.

This matters because our callers do not agree on line order. The billing
service iterates lines as they come back from the database, which is
`ORDER BY` nothing. The reconciliation job sorts by `line_id`. Two systems
allocating the same refund will disagree about which line owns the cent, and
that is precisely the kind of disagreement ENG-4172 was filed about — we would
be fixing the total and introducing a per-line mismatch.

**Why no test caught it.** The property asserts `sum(parts) == total`. The sum
is correct in both orderings. The invariant that fails here is one nobody
wrote down. That is not a criticism of the property — it is the reason review
still exists after the suite is green.

**What I want:** break ties on something stable that belongs to the line
itself, not on where it sits in a list. `line_id` is the obvious candidate. And
write the rule down, because "who gets the leftover cent" is a policy question
that finance may have an opinion about, not an implementation detail.

## Finding 2 — duplicate line IDs

**Non-blocking, but cheap to close now.** Once ties break on `line_id`, two
lines sharing an ID are ambiguous again. Reject them at the door.

## Not checked

Concurrency (the allocator is pure, so I did not look), the HTML page beyond
confirming the badge reflects the API, and performance — the largest order we
have is 200-odd lines and this is O(n log n).
