# refundsplit

Splits a refund across the order lines it applies to, to the cent.

This is a teaching repository. It is small on purpose, and its git history is
the point: three commits that walk one real bug from report to reviewed fix.

## Run it

No install step, no virtualenv, no network:

```bash
python3 -m refundsplit
```

Then open <http://127.0.0.1:8000>. Edit the refund total or the line totals and
watch the reconciliation badge.

## Run the tests

```bash
python3 -m venv .venv && .venv/bin/pip install -q pytest hypothesis
.venv/bin/python -m pytest -q
```

## Layout

| Path | What it is |
|---|---|
| `CLAUDE.md` | the standing constraints every change works under |
| `docs/ENG-4172.md` | the bug report this repository exists to fix |
| `docs/REVIEW.md` | the human review of the fix, and what it caught |
| `docs/ALLOCATION-POLICY.md` | how the last cent is assigned, and why |
| `refundsplit/allocate.py` | the allocator — the only file the fix changes |
| `refundsplit/money.py` | decimal ↔ integer-cent conversion |
| `refundsplit/web.py` | the localhost demo surface |
| `tests/` | unit, HTTP and property-based tests |
