# From one task to a team habit

The demo proves a workflow on one bug. This is what it would take to find out
whether it works on a hundred, and how we would know if it didn't.

## What to try first

Not "use the agent more." One specific shape of task, because it is the shape
where this workflow is safe and its result is checkable:

> **A bug that already has a reproduction, in a single module, where "fixed" is
> a thing a test can assert.**

The three habits that made the demo work, in order of how much they matter:

1. **Write the constraints down before you prompt.** `CLAUDE.md` in the
   repository, not a paragraph in a chat box. It survives the session, it gets
   reviewed, and it applies to humans too.
2. **Make it prove the bug first.** A failing test, run and recorded, before any
   fix exists. Then the fix has something to be measured against.
3. **Review the diff for the invariant nobody wrote down.** The suite tells you
   what somebody thought to assert. Your job is the rest.

What not to start with: open-ended refactors, anything touching a public
contract, and anything where you cannot state what "correct" means before you
begin.

## Ownership

Roles, not a rota. Fill in the names before you present this — they are
deliberately blank, because volunteering people in a slide is how adoption
efforts die.

| What | Who | What they actually do |
|---|---|---|
| Keeping `CLAUDE.md` honest in each repo | repo maintainer | reject changes that contradict it; update it when the team's rules genuinely change |
| Reviewing agent-authored diffs | the same people who review human diffs | no separate process, no lighter standard |
| Collecting the indicators below | one named person, ~30 min/month | pulls the numbers, publishes them unedited, including the bad ones |
| Calling it off | engineering lead | owns the stop decision, against the criteria below |

## Follow-up

Dates are relative to session day (D).

- **D+7 — open office hour.** Anyone who tried it brings their diff, working or
  not. Failures are the useful half; make it explicit that bringing a mess is
  the point.
- **D+30 — read the first ten.** Pull every agent-authored diff from the month
  and read them as a group. The question is not "did it work" but "what did
  review have to catch, and is that list getting longer or shorter?"
- **D+90 — the scope decision.** Widen (more task shapes), hold, or stop, judged
  against the proposed indicators. Whatever the numbers say, they get published
  as they are.

---

## Indicators

Two tables. They are kept apart on purpose, because mixing "we measured this"
with "we intend to measure this" is how a demo turns into a claim it cannot
support.

### Observed — this one task, N = 1

Measured in the session that produced this repository. Every row can be
re-derived from `evidence/` or from `git log`. **One task is an anecdote, not a
result** — this table exists to show what "measured" looks like, not to prove
anything about the team.

| What | Measurement | Where it comes from |
|---|---|---|
| Agent wall-clock, baseline → first-pass fix | **3 min 24 s** (01:39:09 → 01:42:33) | `git log --date=format:%T` |
| Agent wall-clock, first pass → reviewed fix | **2 min 22 s** (01:42:33 → 01:44:55) | same |
| Blast radius of the fix | 3 files, **+93 / −13**; `allocate.py` alone **+35 / −12** | `git show --stat 9aa6cbc` |
| Blast radius of the review fix | 5 files, **+167 / −4**, of which 121 lines are documentation; `allocate.py` **+15 / −4**, and three of those are logic | `git show --stat 6191efc` |
| Public API changed | **No** — `Line`, `Share`, `allocate()` identical across all three commits | `evidence/06-diffs.txt` |
| Tests before → after | **14 → 23** | `evidence/01-before.txt`, `05-final.txt` |
| Tests that failed before their fix | **2 of 2** — the reproduction and the order property | `evidence/02-`, `04-` |
| Generated cases per property run | **500** each, 0 failing | `evidence/05-final.txt` |
| **Review findings the tests did not catch** | **1 blocking, 1 non-blocking** | `refundsplit/docs/REVIEW.md` |
| Final state | 23 passed, `ruff` clean, app serves on localhost | `evidence/05-final.txt` |

**What these numbers are not.** The wall-clock figures are commit timestamps in
one uninterrupted session; they exclude the time spent building the baseline
app, capturing evidence, and writing these documents. No human review time is
in them at all, because the review pass was agent-run — that is disclosed at the
top of `REVIEW.md` and it is the single biggest caveat on this table. Treat the
timings as "how long the mechanical part took," nothing more.

### Proposed — team level, not yet measured

None of these have a value yet. Each needs a baseline drawn from the three
months *before* adoption, or the first reading is meaningless.

| Indicator | How it would be collected | Why this one | What would mean stop |
|---|---|---|---|
| **Escaped-defect rate on agent-authored diffs** vs the team's existing rate | tag the commit trailer; count defects found after merge per 100 merged diffs, 90-day window | the only indicator that measures whether the work is *good* rather than *fast* | any sustained gap above the human baseline — this one alone is grounds to stop |
| **Review findings per agent-authored PR**, tracked over time | count review comments that request a change, by PR author type | early warning: rising means scope has outrun what review can catch | a rising trend over two consecutive months as scope widens |
| **Median time from ticket to mergeable PR**, for bounded bug-fix tickets only | ticket tracker timestamps, filtered to the task shape above | the efficiency claim, restricted to the class where it is plausible | no improvement over baseline by D+90 — the workflow costs more than it returns |
| **Share of eligible tickets actually done this way**, monthly | count against tickets matching the shape | adoption itself; a great workflow nobody uses is a failed rollout | below ~20% at D+90 means the workflow does not fit how people work — fix the workflow, not the people |

Two of these are outcome measures and two are adoption measures, and they fail
differently. Low adoption is a process problem you can fix. A rising escaped-
defect rate is a stop signal, and the honest thing to do is stop.

## The argument this demo actually makes

Not "the agent is a good programmer." The claim is narrower and more useful:
**for a bounded task with a written-down definition of correct, an agent will
produce a reviewable, tested diff fast enough that the bottleneck becomes review
— so the leverage is in what you write down beforehand and what you look for
afterward.** The order-dependence finding is the whole case in one example: the
agent wrote a correct fix and a correct property, and it took a human question
to notice which property was missing.
