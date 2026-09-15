# Everything in that demo, explained from zero

You do not need to have written code to follow this. It explains every idea the
ten-minute session uses, in the order the session uses them, and it explains
each one by pointing at the actual thing in the demo rather than describing it
in the abstract.

If you only read one section, read **"Why 'the tests pass' is not the same as
'the code is right'"** near the end. That is what the session is really about.

---

## 1. The thing we are looking at

**Software** is a set of written instructions a computer follows. Someone types
those instructions as text, in a language the computer can read. The demo's
instructions are written in **Python**, and there are about three hundred lines
of them — small enough to read in full.

The demo program does one job. You tell it: *a customer is getting a $100
refund, and their order had three items on it.* It tells you how much of that
refund belongs to each item. Accounting needs that breakdown, because "we
refunded $100" is not enough detail to put in a ledger.

## 2. A repository, a commit, a branch

A **repository** (everyone says "repo") is a folder of code with a memory. The
memory is called **git**. Git does not just keep the current version of every
file — it keeps every version there has ever been, and who changed what, and
why.

A **commit** is one saved point in that history. It is a snapshot of every file
plus a written explanation of what changed and why. Good commit messages are
worth more than most documentation, because they explain intent at the exact
moment somebody had it.

The demo has exactly three commits, and they are the three parts of the story:

| Commit | What it saved |
|---|---|
| `834da14` | the program as it was: working, tested, and quietly losing a cent |
| `9aa6cbc` | the fix for the missing cent |
| `6191efc` | a second fix, for a problem that only a human question surfaced |

A **branch** is a separate line of history, so you can work on a change without
disturbing the version everyone else is using. The demo has two: `main` (the
original) and `fix/ENG-4172-penny-loss` (the work). Think of it as making a copy
to scribble on, with a plan to merge the scribbles back once they are good.

A **diff** is the difference between two commits — the precise list of lines
added and removed. When engineers say "review the diff," they mean: look only at
what changed, not the whole program.

## 3. What actually went wrong (the arithmetic, longhand)

An order has three items, $10.00 each. The customer is refunded $100.00 of
merchandise value. How much of that refund belongs to each item?

Each item is one third of the order, so each gets one third of the refund:

```
$100.00 ÷ 3 = $33.333333…
```

Money cannot have fractions of a cent, so the program rounded each share to the
nearest cent — $33.33. Then:

```
$33.33 + $33.33 + $33.33 = $99.99
```

The customer's card was charged back $100.00. The books say $99.99. **One cent
has no home.** Every system downstream that tries to match those two numbers now
reports a mismatch.

The error is tiny and it is *always in the same direction* — rounding down three
times never cancels out. Across 40,361 refunds in one month it came to $403.61.

**The fix.** Stop rounding each share on its own. Instead:

1. Give each line the whole cents it definitely earns: 3,333 cents each
   (that is $33.33), using 9,999 of the 10,000 cents.
2. Count what is left over: 1 cent.
3. Hand the leftover cent to the line with the largest discarded fraction.

```
$33.34 + $33.33 + $33.33 = $100.00
```

Now the total is correct *by construction* — not because the rounding happened
to work out this time. This method has a name, **largest remainder**, and it is
the same arithmetic used to allocate parliamentary seats from vote shares.

One more detail worth knowing: the fixed version does all of this in **whole
cents** (integers) rather than in decimal dollars. Computers are exact with whole
numbers and famously inexact with decimal fractions. Working in cents removes an
entire category of mistake.

## 4. Tests, and what "green" means

A **test** is code that checks other code. It sets up a situation, runs the
program, and asserts what the answer should be. If the answer differs, the test
fails.

The demo's reproduction test says, in effect: *split $100.00 across three equal
lines; the pieces must add up to $100.00.* Run against the broken version it
prints:

```
assert 9999 == 10000
```

Those are cents. That one line is the bug, stated by a machine.

Engineers say a suite is **green** when every test passes and **red** when any
fails. The demo goes 14 tests green → 1 red (the reproduction) → 20 green
(fixed) → 1 red again (the review's finding) → 23 green.

**A property test** is the interesting kind, and it is worth understanding
because the session turns on it. A normal test checks one example you thought
of. A property test states a rule that must hold for *every* input, and then a
library called **Hypothesis** generates hundreds of inputs trying to break it —
including absurd ones a person would never type. The demo's rule is "the parts
always sum to the total," and Hypothesis checks 500 generated cases per run.

When Hypothesis does find a failure, it does something clever: it **shrinks** it,
narrowing down to the simplest input that still breaks. In the demo it reduced a
messy seven-line example to *one cent split across two one-cent lines* — the
smallest possible statement of the problem.

## 5. Linters, and continuous integration

A **linter** checks style and obvious mistakes without running the program —
unused variables, inconsistent formatting, suspicious patterns. The demo uses one
called **ruff**. It is not about beauty; it is about removing arguments from code
review so reviewers can spend their attention on logic.

**Continuous integration** (CI) is a robot that runs the tests and the linter
automatically every time someone proposes a change, on a clean machine that is
not yours. It exists because "it works on my laptop" is not evidence. The demo
ships a CI configuration file ready to run, though it has not been run yet — the
project has not been uploaded to a hosting service.

## 6. Code review, and pull requests

**Code review** is another engineer reading your change before it joins the main
version. They are not proofreading syntax — the linter does that, and the tests
catch broken behaviour. They are looking for the thing neither of those can see:
what the change *means*, what it assumes, and what it will do in situations
nobody thought to test.

A **pull request** (PR) is the wrapper around that conversation on a hosting
service like GitHub: here is my branch, here is the diff, please look. The demo
is local only, so its review lives in a file, `docs/REVIEW.md`, instead.

That review document is honest about something important, and you should be too
if you present this: the review was written by the same AI that wrote the fix,
on a second pass where it was told to attack its own work. The finding it
produced is genuinely real — you can watch it reproduce — but an AI checking its
own homework is not the same as a colleague checking it.

## 7. Localhost

When you open a normal website, your browser fetches it from a computer
somewhere else. **Localhost** — the address `127.0.0.1`, or `http://localhost:8000`
— means *this computer, right here*. The demo starts a small web server on your
own machine, and your browser talks to it without touching the internet at all.
Nothing is published, nothing is uploaded, and it works on a plane.

The `8000` is the **port**: one computer can run many servers at once, so each
picks a numbered door. If something else is already using door 8000, you use
another one.

## 8. What an AI coding agent is, and is not

An **AI coding agent** is a language model that has been given tools — it can
read files, write files, run commands, and see what happens. You give it a goal;
it works in a loop of reading, changing, running, and reacting, the same loop an
engineer uses.

What it genuinely did in this demo: read the bug report and the constraints,
wrote a test that reproduced the bug, changed the allocation logic, ran the
tests, wrote a second kind of test to check the rule held for generated inputs,
and explained its reasoning in the commit messages — including what it had *not*
verified.

What it is not:

- **Not a guarantee of correctness.** It produces plausible code. Plausible is
  not the same as right, which is the entire reason for the tests and the review.
- **Not a substitute for knowing what you want.** The most valuable artifact in
  the demo is `CLAUDE.md`, the file listing the rules — and a human wrote that.
- **Not accountable.** The person who merges the change owns it. That does not
  transfer.

## 9. Why "the tests pass" is not the same as "the code is right"

This is the point of the session.

After the fix, everything was green. Twenty tests. A property checked against
500 generated inputs, all passing. By every available signal, done.

Then a human asked a question no test had asked: *does each line get the same
share regardless of what order the lines are handed over in?*

It did not. The leftover cent went to whichever line happened to be passed
first. Feed the same three lines in a different order and a different item got
the extra cent.

The total was still correct every single time — so the property test, which
checked the total, was blind to it. And it mattered in a way you can only see if
you know the business: two different systems at the company read those lines in
different orders, so they would have disagreed with each other about which item
owned the cent, while both reported a perfectly correct total.

**A test suite is a list of the mistakes someone already thought of.** It is an
extremely good list, it runs in seconds, and it never gets bored. But it cannot
contain the mistake nobody has imagined yet. That gap is what review is for, and
it is why "the AI made the tests pass" is a beginning, not an ending.

## 10. Run it yourself

You need a Mac or Linux machine with Python 3 (`python3 --version` in Terminal
will tell you; anything 3.10 or newer works).

```bash
cd ~/Documents/syswork/refundsplit

# see the bug
git checkout main
python3 -m refundsplit
```

Open `http://localhost:8000`. The badge is red: **$99.99 of $100.00**. Press
`Ctrl-C` in the terminal to stop it.

```bash
# see the fix
git checkout fix/ENG-4172-penny-loss
python3 -m refundsplit
```

Refresh the browser. Green: **$100.00 of $100.00**. Press "Shuffle line order" a
few times — the numbers hold steady. Then repeat that on the middle commit
(`git checkout fix/ENG-4172-penny-loss~1`) and watch the extra cent wander,
which is the bug review caught.

To run the tests:

```bash
python3 -m venv .venv && .venv/bin/pip install -q pytest hypothesis
.venv/bin/python -m pytest -q
```

A **virtual environment** (`venv`) is a private, disposable folder of extra
libraries for this project, so installing test tools cannot disturb anything else
on your machine. The program itself needs none of it — that is why running the
app takes no install step at all.

---

## Glossary

| Term | Plain meaning |
|---|---|
| **agent** | an AI that can read files, run commands, and act in a loop, not just chat |
| **branch** | a parallel line of history for working without disturbing the main version |
| **CI** | a robot that runs the tests on every proposed change, on a clean machine |
| **commit** | one saved snapshot of the code, with an explanation of why |
| **diff** | the exact lines added and removed between two versions |
| **green / red** | all tests passing / at least one failing |
| **Hypothesis** | a library that invents hundreds of inputs to try to break a rule |
| **integer cents** | storing $10.00 as `1000` so the computer never deals in fractions |
| **invariant** | something that must be true no matter what — here, "the parts sum to the total" |
| **largest remainder** | give out whole units first, then hand leftovers to the biggest fractions |
| **linter** | a style-and-mistake checker that reads code without running it |
| **localhost** | this computer; a web address that never leaves your machine |
| **port** | the numbered door a server listens on (`8000` here) |
| **property test** | a test of a rule across many generated inputs, not one example |
| **pull request** | the online wrapper around "please review my branch" |
| **regression** | a thing that used to work and now doesn't |
| **repository** | a folder of code that remembers its whole history |
| **shrinking** | narrowing a failure down to the simplest input that still breaks |
| **suite** | all the tests, run together |
| **venv** | a disposable private folder of libraries for one project |
