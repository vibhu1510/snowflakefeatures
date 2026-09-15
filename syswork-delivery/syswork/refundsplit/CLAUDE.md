# Working agreement for this repository

This file is the standing brief for any agent or engineer changing this code.
It is not a prompt someone typed once and forgot — it is version-controlled, it
is reviewed, and it applies to every change.

## What this service does

`refundsplit` takes a refund total and the order lines it applies to, and
decides how much of the refund lands on each line. Downstream, those per-line
numbers become ledger entries. Finance reconciles them against the payment
processor. If the parts do not sum to the refund, the books do not balance.

## Rules

1. **Integer cents only.** Money is an `int` number of cents everywhere inside
   the package. `Decimal` exists only at the boundary, in `money.py`. Floats are
   never accepted in the money path — `to_cents` raises on them deliberately.
2. **The sum invariant is absolute.** For every input,
   `sum(share.amount_cents) == total_cents`. There is no tolerance, no epsilon.
   A cent that vanishes is a reconciliation break, not a rounding preference.
3. **The public API is frozen.** `Line`, `Share` and `allocate(total_cents,
   lines)` are imported by the billing service. Do not change names, parameter
   order, or return shapes. Internals are yours.
4. **A behaviour change ships with a test that failed before it.** Write the
   failing test first, watch it fail, then fix. A test written after a green run
   proves only that you can describe what the code already does.
5. **No third-party runtime dependencies.** `python3 -m refundsplit` must start
   on a clean machine with no install step and no network. `pytest`,
   `hypothesis` and `ruff` are development-only.
6. **State what you did not check.** If a change is verified for positive
   amounts only, say so in the commit message. Silence reads as coverage.

## Before you call a change done

```bash
ruff check .            # must be clean
python -m pytest -q     # must be green
python3 -m refundsplit  # must serve http://127.0.0.1:8000
```
