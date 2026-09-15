# Session materials — "Ten minutes, one real task"

These files were built to live in `~/Documents` on a Mac. They are stored here
so they survive the session that created them; copy them across like this:

```
syswork-delivery/TRANSCRIPT.md         ->  ~/Documents/TRANSCRIPT.md
syswork-delivery/SDLC-FOR-NOVICES.md   ->  ~/Documents/SDLC-FOR-NOVICES.md
syswork-delivery/syswork/              ->  ~/Documents/syswork/
```

The relative links between them assume exactly that layout.

## What this is

A 10-minute session that teaches a workflow rather than touring a product. It
follows one bounded bug — a refund of $100.00 recorded as $99.99 — from a
finance complaint through a fix, a property test, a review finding that the
tests could not catch, and a second fix. Then it asks what adopting this would
take, with measured and proposed indicators kept strictly apart.

| File | What it is |
|---|---|
| `TRANSCRIPT.md` | the spoken script, word for word, with stage directions |
| `SDLC-FOR-NOVICES.md` | every concept in the session explained from zero |
| `syswork/README.md` | start here for the materials themselves |
| `syswork/RUNBOOK.md` | the timed script: what to type, what to say |
| `syswork/ADOPTION.md` | ownership, follow-up, and the two indicator tables |
| `syswork/EVIDENCE.md` | every claim mapped to the capture that backs it |
| `syswork/evidence/` | raw terminal output and screenshots from real runs |
| `syswork/refundsplit/` | the demo project — runs on localhost with no install |

## The demo project's git history

`refundsplit` is a git repository in its own right, and its three commits are
the three beats of the talk. A nested repository cannot be stored inside this
one, so its `.git` directory is not here — the full history is preserved in
`syswork/refundsplit-history.bundle`. See `syswork/HISTORY.md` to restore it:

```bash
git clone syswork/refundsplit-history.bundle refundsplit-restored
```

## Run the demo

```bash
cd syswork/refundsplit      # or the restored clone
python3 -m refundsplit      # http://127.0.0.1:8000
```

No install step, no dependencies, no network.
