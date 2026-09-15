# Allocation policy

How a refund is divided, stated in full, because "who gets the leftover cent"
is a business rule and not an implementation detail.

## The rule

1. Each line's exact entitlement is `refund_total × line_total ÷ order_total`.
2. Each line is credited the **whole cents** of its entitlement.
3. Flooring in step 2 leaves between zero and (number of lines − 1) cents
   unassigned. Those are credited **one each** to the lines with the largest
   discarded fraction.
4. When two lines have the **same** discarded fraction, the cent goes to the
   line whose `line_id` sorts first.

Step 4 is the part that is easy to get wrong. An earlier version of this code
broke ties by position in the caller's list, which meant two services holding
the same order could allocate the same refund differently depending on how
they happened to iterate it. Ties now break on the line's own identity, so the
answer is the same everywhere.

## Guarantees

- The shares always sum to the refund total exactly. No tolerance.
- The same order and the same refund always produce the same per-line shares,
  regardless of the order the lines are passed in.
- No line receives a negative share when the refund is positive.

## Deliberate limits

- **An order whose lines are all worth zero** has no proportion to divide by.
  Every share is zero, and the sum guarantee does not apply. This is the one
  documented exception; if it ever happens in production it is a data problem
  upstream, not a rounding question.
- **Negative line amounts are rejected.** Discounts belong in the line total,
  not as a separate negative line.
- **Duplicate line IDs are rejected**, because step 4 would be ambiguous.
- **Consistently favouring the lowest `line_id`** does bias the leftover cent
  toward the earliest line of an order across many refunds. The bias is at most
  one cent per refund per line and it is deterministic and auditable, which is
  worth more here than statistical fairness. If finance ever wants it spread,
  the place to change it is step 4 and nowhere else.
