# Transcript — the slide deck

The spoken script for `Ten-Minutes-One-Real-Task.pptx`, one section per slide. This is
the same text sitting in each slide's speaker-notes pane, so if you change one, change
the other.

It's written to be said out loud, not read: short sentences, contractions, the odd
deliberate fragment. Say it in your own words where it doesn't sound like you — the
numbers are the part that has to stay exact.

Timings are cumulative. If you reach slide 8 at around six minutes, you're on pace.

**There are two transcripts in this folder.** `TRANSCRIPT.md` is for driving the demo
live on a laptop. This one is for the deck. Don't pick up the wrong one ten seconds
before you start.

---

## Slide 1 · Title

**0:00-0:20**

Morning. Ten minutes, one bug. I'm not going to demo a tool — I'm going to walk you through one task end to end, because the interesting part isn't that the agent fixed it. The interesting part is what we found afterwards.

## Slide 2 · A $100 refund. $99.99 on the books.

**0:20-1:20**

Here's the bug. We refund a customer a hundred dollars. Their card gets a hundred dollars. Our books record ninety-nine ninety-nine.

One cent — and it isn't one order. August came up four hundred and three dollars short across forty thousand refunds. Almost exactly a penny each, always in the same direction.

The customer never notices. Finance notices every month, because reconciliation is the one process where a cent and a million dollars are equally wrong.

## Slide 3 · The case you check by hand works

**1:20-2:05**

Quick sanity check on why nobody caught this for years.

Fifty, thirty, twenty. Divides cleanly, sums exactly, looks perfect. That's the example somebody wrote into the test file.

Three tens is the one that breaks, and you only hit it when the division leaves a remainder — which across a real order book is most of them, but never the one you check by hand.

So the suite was green the whole time. Hold onto that, because it happens again later in this story.

## Slide 4 · The interesting part isn't the prompt

**2:05-3:00**

Before any code — this is the part I'd actually argue is the point.

This file was in the repository before the ticket existed. Money is whole cents. The parts must equal the total, and that one has no tolerance at all. The public API is frozen because production calls it. Every change ships with a test that failed first. And my favourite: say what you didn't check, because silence reads as coverage.

I didn't type that into a chat box. It's committed, it's reviewed, and it binds me as much as the agent. That's the difference between steering and hoping.

## Slide 5 · Prove the bug before you fix it

**3:00-3:50**

First thing that happened: a failing test, before any fix existed.

Assert nine thousand nine hundred ninety-nine equals ten thousand. Those are cents — that's our penny, stated by a machine instead of by a person in a ticket.

And this is the habit I'd push hardest. Make it prove the bug first. Otherwise you're reviewing a change with no way to tell a real fix from something that merely looks reasonable — and looking reasonable is precisely what these models are best at.

## Slide 6 · Largest remainder, in whole cents

**3:50-5:00**

Here's what came back. Three things.

Each line gets the whole number of cents its share definitely earns — 3,333 each, which uses 9,999 of our 10,000. Count what's left. One cent. Hand it to the line with the largest discarded fraction. The total is now exact because of how it's built, not because the rounding happened to land.

Third one I'd have got wrong myself: it's integer arithmetic throughout. Decimal is gone, every rounding mode is gone. You can't pick the wrong rounding mode if you never round.

And the public API never moved — same signature across all three commits, because the brief said production callers were frozen.

## Slide 7 · Green — including inputs nobody typed

**5:00-5:55**

Tests green. Twenty of them. And the interesting one is the property test.

A normal test checks one example you thought of. A property states a rule that has to hold for every input — whatever the refund, whatever the lines, the parts sum to the total — and then the library spends the run inventing inputs to try to break it. Five hundred generated cases, zero failures.

In the browser, a hundred of a hundred. Ticket closed. By every signal available, we're done.

Except that property is true and it is not enough, and the next ninety seconds are the actual reason I'm up here.

## Slide 8 · A question no test had asked

**5:55-7:05**

Someone read the diff and asked what none of the tests asked: does each line get the same share regardless of what order the lines arrive in?

It doesn't. The leftover cent follows whichever line goes first. Same three lines, same refund, reordered — a different item owns the penny.

Now look at the sums. Correct both times. So the property could never have caught this, because it checked the total, and the total was never wrong.

And it matters for us specifically. Billing reads lines in whatever order the database returns. Reconciliation sorts by line ID. Two systems, same refund, disagreeing about which item owns the cent — while both report a perfect total. We'd have fixed the headline number and introduced a subtler version of the same bug.

## Slide 9 · Tests prove the invariant you thought to write down

**7:05-7:45**

This is the sentence I'd like you to leave with. The tests proved the invariant we thought to write down. Review found the one we didn't.

That is not an argument against working this way. Once somebody asked the right question, the agent wrote the property that catches it and the fix that satisfies it in about two minutes — three lines of logic, break ties on the line's own ID rather than its position in a list. Twenty-three tests, both properties green.

It's an argument about where your attention is worth spending. The agent is fast at the part you can specify. You are still the only one who notices what nobody specified.

## Slide 10 · Two things I'm not claiming

**7:45-8:15**

Two quick disclosures, because I'd rather you hear them from me.

First: that review was done by the same agent on a second pass, told to attack its own work. The finding is real — you watched it reproduce — but a model checking its own homework is not a colleague checking it, and the write-up says exactly that at the top.

Second: three and a half minutes to a fix is one task, off commit timestamps. It's the mechanical part only, it contains no human review time, and one task is an anecdote. Take the shape of it, not the number.

## Slide 11 · What to try on Monday

**8:15-9:00**

So what do you do with this.

Don't start with "use the agent more." Start with one shape of task: a bug that already has a reproduction, in one module, where fixed is something a test can assert. That's where this is safe and the result is checkable.

Three habits, in order of importance. Write the constraints down before you prompt — in the repo, not the chat box. Make it prove the bug first. Then review the diff for the invariant nobody wrote down.

Not for: open-ended refactors, public contracts, or anything where you can't say what correct means before you begin.

## Slide 12 · Who owns it, and when we look again

**9:00-9:30**

Ownership, briefly, because a plan without names is a wish.

The repo maintainer keeps the brief honest. Agent diffs get reviewed by the same people to the same standard — no separate track. One person spends half an hour a month pulling the numbers and publishes them unedited, including the bad ones. The engineering lead owns the decision to stop.

Three checkpoints. A week out, an open hour — bring your diff whether it worked or not, the failures are the useful half. A month out, we read the first ten together. Ninety days, we widen, hold, or stop.

## Slide 13 · Measured, and not yet measured

**9:30-9:50**

Two tables, kept apart on purpose.

Left is what we measured, on one task. Three and a half minutes. Fourteen tests to twenty-three, and both new ones failed before they passed. One blocking review finding the tests couldn't see.

Right is what we'd need over a quarter, and none of it has a value yet. The one that matters is the first: escaped defects on agent diffs against our existing rate. Low adoption is a process problem you can fix. A rising defect rate is a stop signal — and the honest response to a stop signal is to stop, not to review harder.

## Slide 14 · The claim is narrower than the hype

**9:50-10:00**

So the claim I'm making is narrower than the hype. For a bounded task with a written definition of correct, you get a reviewable, tested change fast enough that review becomes the bottleneck — which means the leverage sits in what you write down beforehand and what you look for afterwards.

All of it ships together, including the run where it was wrong. Happy to take questions.

---

## Pacing

1,322 spoken words. At a normal presenting pace — 145 to 150 words a minute — that's
about 8.9 minutes of talking, which leaves roughly a minute of the ten for
the pauses. The pauses are doing work: two seconds of silence on slide 2 while people
read the red number, and a real beat before "true, and not enough" on slide 7. Don't
fill them.

Running long? Compress slides 3 and 12; neither carries an argument you can't summarise
in a sentence. Running short? Slide 8 is the one that rewards going slower — it's the
only place in the deck where the audience has to follow an actual reversal.

## If someone interrupts

**"Couldn't you just round up?"** Then you'd over-refund by a cent instead of under by
one, and finance would still be reconciling a gap — just in the other direction. The
point isn't the direction, it's that the parts have to equal the whole.

**"Why not use floats?"** Because 0.1 plus 0.2 isn't 0.3 in binary floating point, and
a refund system that quietly disagrees with a bank statement is worse than one that
refuses to start.

**"How do we know the agent didn't just fake the tests?"** Because the reproduction ran
red before the fix existed, and that run is captured. Making the test pass by weakening
it would have shown up in the diff — that's what review is for, and it's why the failing
run is recorded rather than described.

**"Is this really only three lines?"** The tie-break fix is. The commit is bigger because
it also carries the comment explaining why, a guard against duplicate IDs, and a policy
document. Writing down who receives the leftover cent was the expensive part, not
changing the sort key.
