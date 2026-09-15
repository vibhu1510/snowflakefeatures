# Runbook — "Ten minutes, one real task"

A timed script for a 10-minute session. Everything in it has been run; the
output it tells you to expect is in `evidence/`, captured from the real thing.

## Before you start

```bash
cd refundsplit
git checkout main            # the "before" state
python3 -m refundsplit       # http://127.0.0.1:8000 — leave this running
```

In a second terminal, in the same directory:

```bash
python3 -m venv .venv && .venv/bin/pip install -q pytest hypothesis
```

Three windows on screen: the browser, a terminal for tests, a terminal (or
editor) for the diff. The browser is the story; the terminal is the proof.

Time travel through the demo with git:

| State | Command | What it is |
|---|---|---|
| Before | `git checkout main` | `834da14` — the bug is live |
| First pass | `git checkout fix/ENG-4172-penny-loss~1` | `9aa6cbc` — the fix |
| After review | `git checkout fix/ENG-4172-penny-loss` | `6191efc` — the finished change |

---

## 0:00–1:00 · The problem, in one sentence

**On screen:** the browser at `http://127.0.0.1:8000`, showing the red badge —
**$99.99 of $100.00**.

**Land this:** a customer is refunded $100.00. The ledger records $99.99. One
cent, every refund that doesn't divide cleanly. The ticket (`docs/ENG-4172.md`)
puts August at $403.61 across 40,361 refunds. That is not a rounding curiosity,
it is a reconciliation break, and it is the kind of bug that survives for years
because every number on the screen looks plausible.

**Do:** change a line total to `$50.00` so the split divides cleanly and the
badge goes green. Then put it back. That is *why nobody caught it* — the case
you check by hand is the case that works.

---

## 1:00–2:30 · The constraints, and where they live

**On screen:** `refundsplit/CLAUDE.md`.

**Land this:** the interesting part of working with an agent is not the prompt.
It is this file. Six rules — integer cents, the sum invariant is absolute, the
public API is frozen, a behaviour change ships with a test that failed first,
no runtime dependencies, say what you did not check. It is committed, it is
reviewed, and it applies to every change whether a person or an agent makes it.

**Say out loud:** "I did not type these constraints into a chat box. They were
in the repository before the task existed. That is the difference between
steering an agent and hoping."

---

## 2:30–3:30 · Reproduce before you fix

**Do:** show the reproduction test, then run it *before* any fix exists.

```bash
.venv/bin/python -m pytest -q tests/test_allocate.py::test_eng_4172_shares_sum_to_the_refund_total
```

**Expect:** `assert 9999 == 10000` — `evidence/02-repro-fails.txt`.

**Land this:** the failure is now a fact in the repository, not a claim in a
ticket. Everything after this is measured against it. If you take one habit
from this session, take this one: make the agent prove the bug before it
proposes a cure.

---

## 3:30–5:00 · The change

**Do:** `git checkout fix/ENG-4172-penny-loss~1`, then walk the diff:

```bash
git diff main..HEAD -- refundsplit/allocate.py    # or: open evidence/06-diffs.txt
```

**Land these three things, and no others:**

1. **Integer cents.** `Decimal` and every rounding mode are gone from the
   allocation path entirely. You cannot pick the wrong rounding mode if you
   never round.
2. **Largest remainder.** Each line takes the whole cents its proportion earns;
   the cents left over by that flooring go one each to the lines with the
   largest discarded fraction. The sum is exact by construction, not by luck.
3. **`divmod` on negatives.** Flooring keeps the remainder non-negative, so a
   clawback (a negative refund) still lands exactly. There is a comment saying
   so, which is the kind of thing you want to see in a diff you did not write.

**Say out loud:** the public API did not move. `Line`, `Share` and
`allocate()` are byte-identical across all three commits — production callers
were never in play.

---

## 5:00–6:30 · Validation

```bash
.venv/bin/python -m pytest -q
.venv/bin/python -m pytest tests/test_properties.py -q --hypothesis-show-statistics
```

**Expect:** 20 passed; the sum property reports **500 passing** generated cases
— `evidence/03-after-first-pass.txt`.

**In the browser:** refresh. **$100.00 of $100.00**, green.

**Land this:** a property test is not a test of one example, it is a claim
about every input, and Hypothesis spends the run trying to break it. This one
says: whatever the refund, whatever the lines, the parts sum to the total.

**Then say the important sentence:** "That property is true. It is also not
enough — and the next two minutes are the actual point of this session."

---

## 6:30–8:00 · Human review

**Do:** in the browser, hit **Shuffle line order**. The shares move — the extra
cent jumps to a different line. Hit it again. It moves again. The badge stays
green the whole time.

**Land this:** the total was right every single time, so the property could
never have caught it. But our callers disagree about line order — the billing
service reads them in database order, the reconciliation job sorts by
`line_id`. Two systems, same refund, different per-line answer. We would have
fixed the total and introduced a per-line mismatch.

**Do:** show the finding written up in `refundsplit/docs/REVIEW.md`, then the
property that encodes it, run against the first pass:

```bash
.venv/bin/python -m pytest -q tests/test_properties.py -k does_not_depend
```

**Expect:** a failure shrunk by Hypothesis to `total_cents=1, amounts=[1, 1]`
— one cent across two one-cent lines — `evidence/04-order-dependence-fails.txt`.

**Do:** `git checkout fix/ENG-4172-penny-loss`, rerun. 23 passed. Shuffle in the
browser: nothing moves now.

**Land the lesson, in these words or close to them:** "The tests proved the
invariant we thought to write down. Review found the one we didn't. That is not
an argument against the agent — the agent wrote both properties in seconds once
someone asked the right question. It is an argument about where human attention
is worth spending."

**Be honest about the review's provenance.** The finding in `REVIEW.md` was
drafted by the same agent on a second, adversarial pass. It is a real finding
about real code — it was reproduced by running it — but a model reviewing its
own work is not human review, and the document says so at the top. Say it out
loud too.

---

## 8:00–10:00 · Adoption

**On screen:** `ADOPTION.md`.

Cover, briefly: what to try on Monday (one bounded ticket that already has a
reproduction, constraints written down *before* prompting, failing test first),
who owns what, and the two indicator tables — what was actually measured here
on one task, versus what the team would need to measure over a quarter to know
whether this is working. Do not blur those two. Close on the escaped-defect
rate: that is the number that decides whether this scales or stops.

---

## If the demo breaks

- **Port 8000 busy:** `python3 -m refundsplit --port 8080`.
- **`No module named refundsplit`:** you are not in the `refundsplit/`
  directory. There is no install step; the package is at the repository root.
- **`pytest: command not found`:** use `.venv/bin/python -m pytest`, not bare
  `pytest`.
- **Detached HEAD warnings after checkout:** expected and harmless; the commits
  are the demo.
- **Anything live fails:** every state is captured in `evidence/`. Open the file
  and keep talking — `01-before.txt` through `06-diffs.txt` follow the same
  order as this runbook.
