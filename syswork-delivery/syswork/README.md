# syswork — "Ten minutes, one real task"

Materials for a 10-minute session on working with a coding agent. The session
is not a product tour. It follows one bounded bug from a finance complaint to a
reviewed fix, and then asks what it would take for a team to work this way.

## Read in this order

| File | What it is |
|---|---|
| `RUNBOOK.md` | the timed script — what is on screen, what to type, what to say |
| `ADOPTION.md` | what to try next, who owns it, and the indicators (measured vs proposed) |
| `EVIDENCE.md` | every claim mapped to the capture that backs it |
| `evidence/` | the raw terminal output and screenshots, captured from real runs |
| `refundsplit/` | the demo project — a real git repository with three commits |

Two ways to deliver it, and a script for each:

| File | Use it when |
|---|---|
| `../Ten-Minutes-One-Real-Task.pptx` | you're presenting slides — 14 of them, speaker notes in every one |
| `../TRANSCRIPT-SLIDES.md` | the spoken script for that deck, plus answers to the questions people ask |
| `../TRANSCRIPT.md` | the spoken script for driving the demo live on a laptop instead |
| `../SDLC-FOR-NOVICES.md` | someone in the room has never seen a repository before |

The deck and the live demo tell the same story with the same numbers. The deck is safer
in a room you don't control; the live demo lands harder when the wifi behaves.

## The 10 minutes

| Time | Beat |
|---|---|
| 0:00–1:00 | A refund of $100.00 is recorded as $99.99. One cent, every refund. |
| 1:00–2:30 | The constraints the agent works under, and where they live. |
| 2:30–3:30 | Reproduce before fixing: the failing test comes first. |
| 3:30–5:00 | The change: integer cents, largest remainder, public API untouched. |
| 5:00–6:30 | Validation: the suite, and a property over 500 generated inputs. |
| 6:30–8:00 | Review finds what the tests could not. This is the point of the session. |
| 8:00–10:00 | Adoption: what to try, who owns it, what we count. |

## Run the demo

```bash
cd refundsplit
python3 -m refundsplit      # http://127.0.0.1:8000, no install step
```

## The one sentence, if you only remember one

The tests proved the invariant we thought to write down; review found the one we
didn't — and the agent wrote both in seconds once someone asked the right
question.
