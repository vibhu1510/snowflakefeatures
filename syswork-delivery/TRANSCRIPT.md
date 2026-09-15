# Transcript — "Ten minutes, one real task"

What to say, more or less word for word. Stage directions are in `[brackets]`.
Roughly 1,300 spoken words, which lands near ten minutes once you account for
the pauses where something is running.

Two rules while you deliver it. Never read a bullet list aloud. And when a
command is running, stop talking and let people watch it.

---

## 0:00 — The problem

`[Browser already open at localhost:8000. Big red badge on screen. Say nothing
for two seconds and let them read it.]`

A customer returns an order. We refund them a hundred dollars. Their card gets
a hundred dollars.

And our books record ninety-nine dollars and ninety-nine cents.

`[Point at the badge.]`

One cent. Gone. Not on this refund — on every refund where the division doesn't
come out clean. Finance filed this. In August the payment processor said we
refunded one point two eight million dollars; our ledger said four hundred and
three dollars less. Across forty thousand refunds. Almost exactly one cent each.

Now watch why nobody caught this for years.

`[Change the first line total to 50.00. Badge goes green.]`

Fifty, thirty, twenty. Divides cleanly. Perfect. Every number reconciles.

`[Change it back. Badge goes red.]`

Three equal lines. Thirty-three thirty-three, three times. Ninety-nine
ninety-nine. The case you'd check by hand is the case that works. That's the
whole reason this survived.

So that's the task. It's small, it's real, and "fixed" is something a computer
can check — which is exactly the kind of task I'd hand to an agent.

## 1:00 — What the agent is working under

`[Open refundsplit/CLAUDE.md.]`

Before I show you the fix, I want to show you the part that actually matters,
because it isn't the prompt.

This is a file in the repository called CLAUDE.md. It's the standing brief.
Money is integer cents — no floating point, ever. The parts must sum to the
total, with no tolerance. The public API is frozen, because production code
calls it. Any change in behaviour ships with a test that failed before it. And
the last one — say what you didn't check, because silence reads as coverage.

`[Scroll slowly. Don't read every rule aloud.]`

I didn't type these into a chat box. They were committed to this repository
before the task existed. They get reviewed like code. They apply to humans too.

That's the difference between steering an agent and hoping.

## 2:30 — Prove the bug first

`[Terminal.]`

First rule of the brief: the failing test comes first. So before there's any
fix, here's a test that says the shares must add up.

`[Run it. Wait. Let the red output sit there.]`

Assert nine thousand nine hundred ninety-nine equals ten thousand. Those are
cents. That's our missing penny, and it is now a fact recorded in the
repository instead of a claim in a ticket.

Everything after this gets measured against that failure.

If you take one habit away today, take this one. Make it prove the bug before
it proposes the cure. Otherwise you can't tell a fix from a plausible-looking
edit.

## 3:30 — The change

`[Check out the fix commit. Show the diff for allocate.py.]`

Here's what came back. Three things worth your attention.

One. Decimal is gone. Every rounding mode is gone. It's integer arithmetic all
the way through. You cannot pick the wrong rounding mode if you never round.

Two. This is the actual idea. Every line gets the whole number of cents its
proportion earns — that's the floor. Flooring always leaves a few cents
unallocated. Those leftover cents get handed out, one each, to the lines with
the largest discarded fraction. The total is exact because of how it's built,
not because the rounding happened to work out.

Three — and this is the one I'd have got wrong. `[Point at the divmod line and
its comment.]` A negative refund. A clawback. Flooring toward negative infinity
keeps the remainder positive, so the leftover count stays sane instead of going
negative. There's a comment explaining it. In a diff I didn't write, that
comment is worth more than the code.

And the public API never moved. Line, Share, allocate — byte for byte identical
across all three commits. Production callers were never in play, because the
brief said they were frozen.

## 5:00 — Does it actually work

`[Run the full suite.]`

Twenty tests. Green.

`[Run the property tests with statistics.]`

This one's different, and it's worth ten seconds of explanation. A normal test
checks one example. This is a property — a claim about *every* input. It says:
whatever the refund, whatever the lines, the parts sum to the total. Then the
library spends the run generating inputs specifically trying to break it.

Five hundred generated cases. Zero failures.

`[Refresh the browser. Green badge.]`

A hundred dollars of a hundred dollars. Ticket closed.

`[Beat.]`

Except that property is true and it is not enough. And the next ninety seconds
are the actual point of this session.

## 6:30 — What review found

`[In the browser, click Shuffle line order.]`

Same three lines. Same refund. I've only changed the order they're passed in.

`[Point at the shares. Click shuffle again. And again.]`

Watch which line gets the extra cent. It moves. It follows whichever line
happens to go first.

And look at the badge — green the entire time. The total is correct on every
single one of those. So the property could never have caught this. The sum is
right either way.

Here's why it matters for us. Our callers don't agree on line order. The billing
service reads lines out of the database in whatever order they come back. The
reconciliation job sorts them by line ID. Same order, same refund, two different
answers about which line owns the cent. We'd have fixed the total and quietly
introduced a per-line mismatch — which is a version of the same bug we started
with.

`[Run the order-independence property against the first-pass fix.]`

So we write that down as a property too. And it fails — shrunk all the way down
to one cent across two one-cent lines. That's the smallest possible version of
the bug, and the library found it on its own.

`[Check out the final commit. Rerun. Twenty-three passing. Shuffle in the
browser — nothing moves.]`

The fix itself is three lines of logic — break ties on the line's own ID
instead of its position in a list. Everything else in that commit is the comment
explaining why, a guard against duplicate IDs, and a policy document, because who
gets the leftover cent turns out to be a business rule, not an implementation
detail.

Here's the sentence I want you to leave with. The tests proved the invariant we
thought to write down. Review found the one we didn't.

That's not an argument against the agent. Once someone asked the right question,
it wrote the property and the fix in about two minutes. It's an argument about
where your attention is worth spending.

And one disclosure, because it matters. That review was done by the same agent,
on a second pass, told to attack its own work. The finding is real — you just
watched it reproduce. But a model reviewing itself is not human review, and the
document says so at the top.

## 8:00 — What to do with this

`[Open ADOPTION.md.]`

If you want to try this on Monday, pick one shape of task. A bug that already
has a reproduction, in one module, where "fixed" is something a test can
assert. Not a refactor. Not anything touching a public contract.

Three habits, in order of how much they matter. Write the constraints down
before you prompt. Make it prove the bug first. Then review the diff for the
invariant nobody wrote down.

`[Scroll to the tables.]`

Two tables here, and I've kept them apart deliberately. This one is what we
actually measured — on one task. Three and a half minutes to a fix. Twenty-three
tests, up from fourteen. Public API unchanged. One blocking review finding the
tests couldn't see. One task is an anecdote, not a result, and the timings don't
include a minute of human review, because there wasn't any.

This second table is what we'd need to measure over a quarter to know if this
works. Two adoption numbers, two outcome numbers. The one that matters is
escaped defects on agent-written diffs against our existing rate. If that gap
opens up, we stop. Not "review harder" — stop.

Because the claim I'm making today is narrow. For a bounded task with a written
definition of correct, you get a reviewable, tested change fast enough that
review becomes the bottleneck. So the leverage is in what you write down before,
and what you look for after.

Everything you just watched is in the repository, including the failures.

`[Stop. Take questions.]`
