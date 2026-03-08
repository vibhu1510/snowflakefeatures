---
title: "9 Data Engineering Challenges That Kill Pipelines in Production (And How I Tackled Them With Pure Snowflake SQL)"
published: false
description: "Late-arriving data, PII leaks, stale dashboards, revenue that doesn't tie out — here's a pipeline that tackles the real problems data engineers face, with honest trade-offs included."
tags: dataengineering, snowflake, sql, datapipeline
cover_image:
---

Every data engineering tutorial starts the same way: ingest some CSV files, run a few transformations, load a table, done. Ship it.

Then production happens.

Payments arrive three days after the order. An analyst screenshots a customer's email from a dashboard and posts it in Slack. Finance calls because revenue is off by $47,000 — turns out nobody subtracted refunds. Legal sends a GDPR deletion request and nobody knows which tables contain PII. The pipeline silently fails on a Saturday and nobody notices until Monday.

These are the problems that actually consume a data engineer's time. Not the ingestion. Not the transformations. The **messy, unglamorous edge cases** that turn a working demo into a production nightmare.

I built an end-to-end e-commerce data pipeline on Snowflake that tackles nine of these challenges head-on — using only SQL and native Snowflake features. No dbt, no Airflow, no Python glue. Here's what I built, what worked, and where the trade-offs are.

---

## The Pipeline at a Glance

Before diving into the challenges, here's what the pipeline does:

- **Ingests** batch data (customers, orders, payments, returns) and clickstream events (JSON) through two independent paths
- **Transforms** through a medallion architecture: Bronze (raw) → Silver (clean) → Gold (analytics-ready), with each transition driven by Snowflake Streams
- **Produces** dimensional models with SCD Type 2 history (temporal joins), session-level clickstream aggregations, conversion funnels, revenue reconciliation views, and both cash-basis and accrual-basis daily sales aggregates
- **Enforces** PII masking, row-level security, three-layer GDPR deletion with a suppression list, and 20 automated data quality checks with email and webhook alerting

The orchestration runs on a **parallel Snowflake Task DAG** — batch data (hourly) and event data (every 5 minutes) flow through independent paths so a batch failure never blocks event freshness.

```
Batch (hourly):
  INGEST → MERGE_SILVER_BATCH → BUILD_GOLD_BATCH → DQ_CHECKS

Events (every 5 min):
  INGEST → MERGE_SILVER_EVENTS → BUILD_GOLD_EVENTS
```

Now, the challenges.

---

## Challenge 1: Late-Arriving Data Corrupts Your Analytics

**The problem:** In the real world, data doesn't arrive in order. A payment record might land three days after the order it belongs to. If your pipeline blindly overwrites existing records with whatever arrives latest, an old payment status can overwrite the current one.

**How the pipeline handles it:**

The silver layer uses a timestamp-aware MERGE pattern:

```sql
WHEN MATCHED AND NVL(src.updated_at, src.load_ts)
              >= NVL(tgt.updated_at, tgt.load_ts)
THEN UPDATE SET ...
```

This single condition prevents older records from overwriting newer data. If a payment file arrives late, the `updated_at` timestamp reveals its true age, and the MERGE skips the update.

I tested this with a dedicated `late_arriving_payments.csv` file to confirm the behavior under realistic conditions.

**The trade-off:** This depends on the source system populating `updated_at` reliably. If the source doesn't set business timestamps, you lose this protection and fall back to `load_ts` (ingestion time) — which is better than nothing, but doesn't solve the core problem. Always push back on source teams to provide accurate business timestamps.

**Takeaway:** Never trust file arrival order. Always compare business timestamps, not ingestion timestamps, when deciding whether to update.

---

## Challenge 2: Duplicate Records Slip Through Every Crack

**The problem:** Network retries, file redelivery, and source system bugs all produce duplicates. If you're doing `INSERT INTO ... SELECT`, you're stacking duplicates. If you're doing `CREATE OR REPLACE TABLE`, you're losing history.

**How the pipeline handles it:**

Every silver MERGE starts with deduplication:

```sql
ROW_NUMBER() OVER (
  PARTITION BY business_key
  ORDER BY updated_at DESC, ingestion_ts DESC
) = 1
```

Combined with `MERGE` (not INSERT), this gives **per-table idempotency** — the same batch can be reprocessed without creating duplicates for dimension and transactional tables.

**Clickstream event dedup:** Events use `INSERT INTO` (not MERGE) because they're immutable append-only facts. Within a single stream batch, `ROW_NUMBER` handles duplicates. But events can also arrive in *future* batches (upstream replays, redelivery). To prevent cross-batch duplicates, the events procedure includes a guard:

```sql
WHERE event_id NOT IN (SELECT event_id FROM SILVER.EVENTS)
```

This ensures an event that was already successfully loaded is never re-inserted, even if it reappears in a later file.

**Transaction safety:** Both silver procedures (`SP_MERGE_SILVER_BATCH` and `SP_MERGE_SILVER_EVENTS`) are wrapped in explicit transactions with error handling:

```sql
BEGIN TRANSACTION;
  -- all MERGEs / INSERTs for this procedure
COMMIT;
EXCEPTION WHEN OTHER THEN
  ROLLBACK;
  -- log failure to DQ_RESULTS for alerting
```

This means either all batch tables (customers, products, orders, payments, returns) commit together, or none do. Same for events. No more cross-table inconsistency from partial failures. If a run fails mid-way, the next run replays the full stream cleanly.

**The trade-off:** The `NOT IN` subquery on SILVER.EVENTS scales linearly. At billions of events, you'd want to switch to a Bloom filter, partitioned dedup log, or time-bounded check (`WHERE event_id NOT IN (SELECT event_id FROM SILVER.EVENTS WHERE event_ts > DATEADD('day', -7, CURRENT_TIMESTAMP()))`). At the scale this pipeline targets (millions of events), the subquery is fine.

**Takeaway:** Idempotency isn't a nice-to-have. Design for it from day one — including cross-batch dedup for append-only facts.

---

## Challenge 3: Revenue Numbers That Don't Tie Out

**The problem:** Finance asks "what was revenue last month?" and gets three different answers from three different dashboards. The root cause is almost always the same: someone is reporting gross revenue without accounting for refunds, partial payments, or pending settlements.

**How the pipeline handles it:**

The gold layer includes a **revenue reconciliation view** that classifies every order into one of six statuses:

- `FULLY_PAID` — settled payments match order total, no returns
- `PAID_WITH_RETURN` — fully paid but has refunds
- `PARTIAL_PENDING` — some payments still pending
- `UNDERPAID` — settled amount less than order total
- `OVERPAID` — settled amount exceeds order total (edge case, but it happens)
- `NO_PAYMENT` — order exists with no payment record

**Two aggregate tables for two audiences:**

- **`AGG_DAILY_SALES`** (cash-basis): Refunds attributed to the *return processing date*. A refund on March 10 for an order placed March 1 reduces March 10's net revenue. This matches bank statements and cash-flow reporting.
- **`AGG_DAILY_SALES_ACCRUAL`** (accrual-basis): Refunds attributed back to the *original order date*. That same refund reduces March 1's net revenue. This is required for GAAP/IFRS reporting and answers the question "how well did sales actually perform on day X."

The accrual aggregate joins `FACT_RETURNS` → `FACT_ORDERS` to trace each refund back to its original order date:

```sql
LEFT JOIN GOLD.FACT_ORDERS fo ON fr.order_id = fo.order_id
GROUP BY fo.order_date  -- attribute refund to original order date
```

**What's not covered:** Currency conversion, multi-currency orders, discount codes, payment provider fees, and tax remittance. Real e-commerce reconciliation gets significantly more complex. This pipeline handles the structural pattern (gross vs net with status classification, plus both accounting views), which is the hard part to retrofit later.

**Takeaway:** If your pipeline reports gross revenue as "revenue," you're misleading every downstream consumer. Build reconciliation into the pipeline, not into a spreadsheet — and give finance both views, because they will ask.

---

## Challenge 4: PII Leaking to Downstream Consumers

**The problem:** An analyst runs `SELECT * FROM customers` and now has access to every customer's email, phone number, and full name. They export it, share it, and suddenly you have a data governance incident.

**How the pipeline handles it:**

Snowflake Dynamic Data Masking policies are applied directly to the customer dimension:

| Column | Engineer sees | Analyst sees |
|--------|--------------|--------------|
| email | jane@example.com | j\*\*\*@\*\*\*.com |
| phone | 555-0100 | \*\*\*-\*\*00 |
| first_name | Jane | J\*\*\* |

The masking is **role-aware and transparent**. Engineers and admins see real values. Analysts see masked data. No code changes needed in any query — the policy is applied at the column level.

On top of that, a **row access policy** restricts which regions an analyst can query, controlled via Snowflake object tags. A US_WEST analyst literally cannot see EU data, no matter what query they write.

**Takeaway:** Data masking should be enforced at the platform level, not in the BI tool. If the policy depends on someone remembering to add a `WHERE` clause, it will fail.

---

## Challenge 5: GDPR "Right to Delete" With No Plan

**The problem:** Legal sends over a deletion request for customer C001. You need to remove their data from every table — but you also can't break your fact tables, corrupt your aggregates, or lose the audit trail proving you complied.

**How the pipeline handles it:**

A stored procedure `SP_GDPR_DELETE` performs a **three-layer deletion cascade**:

1. **Hard-delete from Bronze** — purge the customer's raw records from all 5 raw tables (`CUSTOMERS_RAW`, `ORDERS_RAW`, `PAYMENTS_RAW`, `RETURNS_RAW`, `EVENTS_RAW`)
2. **Hard-delete from Silver** — remove from all 5 cleaned tables
3. **Anonymize in Gold** — replace PII with `'REDACTED'` but preserve the surrogate key

Why anonymize instead of delete in gold? Because `FACT_ORDERS.customer_sk` references `DIM_CUSTOMER`. Deleting the dimension row would break every join and corrupt historical aggregates. By keeping the surrogate key with redacted PII, revenue totals stay accurate — you just can't identify who the customer was.

**Suppression list:** After deletion, the customer ID is added to `GDPR_SUPPRESSION_LIST`. This prevents re-ingestion — both `SP_INGEST_BATCH` and `SP_INGEST_EVENTS` run post-COPY cleanup against the suppression list to purge any re-delivered data for deleted customers. The silver MERGE procedures also filter out suppressed IDs, providing defense-in-depth.

Every action is logged to `GDPR_AUDIT_LOG` with the request ID, layer, table name, rows affected, and execution timestamp.

**The Time Travel caveat:** Even after deletion, Snowflake retains data in Time Travel (`DATA_RETENTION_TIME_IN_DAYS`, default 7 days) and Fail-Safe (7 additional days, Snowflake-controlled). That's up to 14 days where deleted PII is technically recoverable. This must be documented in your Data Processing Agreement (DPA). To minimize the exposure window, set `DATA_RETENTION_TIME_IN_DAYS = 1` on PII-containing tables after deletion. The procedure logs a `TIME_TRAVEL_WARNING` entry to the audit log as a reminder.

**What's not covered:** Cross-region data residency requirements, deletion from external stages (S3/GCS files), and deletion from analytics exports (e.g., data already shipped to a BI tool). These are organizational process problems more than pipeline problems, but they should be part of your deletion runbook.

**Takeaway:** GDPR compliance requires a strategy, not just a `DELETE` statement. Plan the deletion path across every layer — bronze, silver, gold, Time Travel, and re-ingestion — before the first request arrives.

---

## Challenge 6: Silent Pipeline Failures

**The problem:** The pipeline fails on Saturday. Nobody checks it. Monday morning, an executive opens a dashboard showing Friday's numbers and makes decisions based on stale data. The pipeline's been broken for 48 hours.

**How the pipeline handles it:**

Three layers of defense:

**20 automated data quality checks** run after every batch gold build, organized by category:

| Category | Checks |
|----------|--------|
| **Row counts & freshness** | Silver/Gold table emptiness, order freshness within 24h |
| **Uniqueness** | Order ID dedup, event ID dedup (catches dedup failures) |
| **Referential integrity** | Orders→Customers, Payments→Orders, Returns→Orders FK checks |
| **Business rules** | Refund ≤ order total, payment amount range (0-100K) |
| **Cross-layer reconciliation** | Silver vs Gold order count within 1% tolerance |
| **Dimension integrity** | NULL customer_sk detection, SCD2 overlapping validity ranges |
| **Volume anomaly** | Today's event count vs 7-day rolling average (±50% threshold) |
| **Stream health** | Staleness check on all Bronze and Silver streams via `SHOW STREAMS` |
| **Schema drift** | Column count validation on all Bronze and Silver tables against expected DDL |

Results are stored in `DQ_RESULTS` with timestamps for trend analysis. Three dashboard views (`VW_DQ_FAILURE_TREND`, `VW_DQ_LATEST_STATUS`, `VW_DQ_SUMMARY`) surface the data by category, trend, and latest status.

**Multi-channel alerting** — A Snowflake ALERT checks for DQ failures every 15 minutes:
- **Email** via `SYSTEM$SEND_EMAIL` for baseline coverage
- **Webhook** templates for Slack and PagerDuty integration, using Snowflake's notification integration feature. The Slack template sends formatted failure lists; the PagerDuty template creates critical-severity incidents for on-call escalation.

**Auto-suspend on failure** — Tasks are configured with `SUSPEND_TASK_AFTER_NUM_FAILURES`. Batch tasks suspend after 3 consecutive failures; event tasks after 5 (higher tolerance since they run every 5 minutes). This prevents runaway retries from burning credits while preserving the stream offset for recovery.

**The trade-off:** The schema drift check monitors column *counts*, not column types or names. If a source renames a column without changing the total count, the check passes silently. For proactive type/name drift detection, you'd use `INFER_SCHEMA()` on staged files before `COPY INTO` and compare against table DDL. The column count check catches the most common scenario (added/dropped columns) with zero compute cost (it queries `INFORMATION_SCHEMA`).

**Takeaway:** A pipeline without monitoring is a liability. Twenty checks isn't comprehensive for a production system with 15+ tables — but the framework (procedure that logs results + multi-channel alerting on failures) scales to hundreds of checks.

---

## Challenge 7: Raw Clickstream Events Don't Answer Business Questions

**The problem:** You have millions of raw clickstream events — page views, product views, add-to-carts, purchases. But raw events don't answer the questions product teams actually ask: "What's our conversion rate? Where do users drop off? What's the average session duration?"

**How the pipeline handles it:**

The gold layer includes a `FACT_SESSIONS` table that aggregates raw events into session-level metrics:

- **Metrics:** session duration, page views, product views, add-to-carts, checkouts started, purchases
- **Flags:** `is_bounce` (single-event sessions), `is_conversion` (session with at least one purchase), `is_computed_session` (see below)
- **Arrays:** `products_viewed` stored as a Snowflake ARRAY for flexible downstream analysis

On top of sessions, two analytical views:

**Conversion funnel** — step-by-step drop-off broken down by device type:
```
page_view → product_view → add_to_cart → checkout_start → purchase
```

**Session KPIs** — bounce rate, conversion rate, average session duration, and average page views per session, sliced by date and device.

**Computed sessionization:** Not all event sources provide a `session_id`. Rather than requiring one, the pipeline includes `VW_EVENTS_WITH_COMPUTED_SESSION` — a view that uses a 30-minute inactivity timeout to assign session boundaries when the source `session_id` is NULL:

```sql
CASE WHEN DATEDIFF('minute',
       LAG(event_ts) OVER (PARTITION BY customer_id ORDER BY event_ts),
       event_ts) > 30
     OR LAG(event_ts) OVER (...) IS NULL
     THEN 1 ELSE 0
END AS new_session_flag
```

Events with a pre-supplied `session_id` pass through unchanged. Computed sessions get an ID like `COMPUTED_<customer_sk>_<group>` and are flagged with `is_computed_session = TRUE` in `FACT_SESSIONS`, so downstream consumers can filter or segment by session source.

**The trade-off:** The 30-minute timeout is an industry convention (Google Analytics uses the same default), but it's arbitrary. A user who browses for 29 minutes, pauses for 31 minutes, then returns will be split into two sessions. For long-form content sites, you might increase the timeout. For mobile apps with push notifications, you might decrease it. The timeout is a single constant in the view — easy to tune.

**Takeaway:** Raw events are an intermediate artifact, not a deliverable. Product teams need session-level metrics — and the pipeline should handle both pre-assigned and computed sessions, because you'll encounter both.

---

## Challenge 8: No Historical Tracking on Dimensions

**The problem:** A customer changes their email address. Your dimension overwrites the old value. Three months later, finance asks "what email did this customer use when they placed order #9001?" and you can't answer.

**How the pipeline handles it:**

`DIM_CUSTOMER` is built as an **SCD Type 2** dimension with `valid_from`, `valid_to`, `is_current`, and a `hash_diff` column using SHA2-256:

```sql
hash_diff = SHA2(CONCAT_WS('|',
    COALESCE(first_name, ''),
    COALESCE(last_name, ''),
    COALESCE(email, ''),
    COALESCE(phone, ''),
    COALESCE(region, '')), 256)
```

When any tracked attribute changes, the current record is closed (`valid_to` set, `is_current = FALSE`) and a new record is inserted. The dimension preserves the full history of customer attribute changes.

**Point-in-time temporal joins:** Fact tables join to `DIM_CUSTOMER` using range-based temporal predicates, not `is_current = TRUE`:

```sql
FROM SILVER.ORDERS_STREAM o
JOIN GOLD.DIM_CUSTOMER d
  ON o.customer_id = d.customer_id
  AND o.order_ts >= d.valid_from
  AND o.order_ts < d.valid_to
```

This ensures that an order placed on January 15 joins to the customer record that was active on January 15 — not whatever the customer looks like today. This is critical for accurate retention cohorts, regional revenue attribution, and any analysis that depends on historical customer attributes.

`DIM_PRODUCT` uses Type 1 (simple overwrite) because product corrections should replace old values — there's no meaningful history in fixing a typo in a product name.

**Why SHA2 over MD5?** MD5 has known collision vulnerabilities. For change detection on a handful of columns, the collision probability is astronomically low either way — but SHA2-256 eliminates the "but MD5 is broken" conversation entirely, and the compute overhead is negligible (~5-10% on the hash step, which is a tiny fraction of overall MERGE cost).

**A DQ check validates SCD2 integrity:** The pipeline includes a check that detects overlapping `valid_from`/`valid_to` ranges for the same `customer_id`. If the SCD2 logic ever produces conflicting time ranges (a bug that's easy to introduce and hard to debug), the DQ check catches it immediately.

**Takeaway:** Not every dimension needs SCD2, but customer dimensions almost always do. Build the history, join with temporal predicates from day one, and validate the ranges — retrofitting any of these onto a running pipeline is painful.

---

## Challenge 9: Cost Spiraling Out of Control

**The problem:** Snowflake bills by compute time. A poorly designed pipeline that does full-table scans, runs on oversized warehouses, or reprocesses everything on every run can burn through credits fast.

**How the pipeline handles it:**

Cost optimization is baked into multiple layers:

- **Two-tier incremental processing** — Bronze→Silver uses Snowflake Streams. Silver→Gold *also* uses streams (six of them), with each dimension/fact guarded by `SYSTEM$STREAM_HAS_DATA()`. If a stream is empty (no changes since last run), the corresponding MERGE is skipped entirely — no warehouse spin-up, no table scan, no cost. On a quiet day, the gold build does almost nothing.
- **Parallel task DAG** — Batch (hourly) and events (every 5 minutes) run independently. Events are processed 12x more frequently without inflating batch costs. A batch failure doesn't block event freshness. Each path only wakes its warehouses for the brief execution window.
- **Right-sized warehouses** — XSMALL for ingestion, SMALL for transforms, SMALL for analytics. Three separate warehouses prevent resource contention and enable cost attribution by function.
- **Aggressive auto-suspend** — 60 seconds for ETL warehouses, 300 seconds for analytics (longer because analysts run ad hoc queries with gaps between them).
- **Clustering** — Fact tables clustered by date columns for partition pruning on time-range queries.
- **Search optimization** — Enabled on the events table for fast equality lookups on `user_id` and `session_id`.
- **Conditional aggregates** — `AGG_DAILY_SALES` and `AGG_DAILY_SALES_ACCRUAL` are only rebuilt when their upstream fact tables were actually updated in the current run, controlled by boolean flags in the procedure.
- **7-day time travel retention** — Balances recovery capability with storage cost.

**Stream staleness as a cost risk:** Snowflake streams become stale if not consumed within the `DATA_RETENTION_TIME_IN_DAYS` window (7 days). Recovery from a stale stream requires recreating it and doing a full re-process — expensive. The pipeline monitors stream staleness via DQ checks and auto-pauses tasks after repeated failures to preserve the stream offset. The events path (every 5 minutes) is well within the window, but the batch path should be monitored during extended outages or holiday freezes.

The monitoring layer includes views for warehouse credit consumption and query cost attribution, so you can track exactly where credits are going.

**Takeaway:** Cost control isn't something you add after the bill arrives. Make every layer incremental — ingestion *and* transformation — and decouple fast paths from slow paths so you're not paying hourly prices for 5-minute freshness.

---

## Why Pure Snowflake (No dbt, No Airflow)?

This pipeline deliberately avoids external tools. Not because dbt and Airflow aren't excellent — they are — but to demonstrate that Snowflake's native feature set (Streams, Tasks, MERGE, Dynamic Data Masking, Row Access Policies, Alerts, Notification Integrations) can handle production-grade requirements without adding infrastructure complexity.

The trade-offs are real:

- **No unit tests for transformation logic.** dbt's testing framework (schema tests, custom tests, source freshness) is genuinely better than hand-rolled DQ checks. The 20-check DQ procedure here is a solid framework — dbt gives you a mature ecosystem with community-maintained packages.
- **Limited orchestration visibility.** Snowflake Tasks don't offer the backfill capabilities, DAG visualization, or cross-system dependencies that Airflow/Dagster provide. For a Snowflake-only pipeline, Tasks work. For multi-system orchestration, you need a real orchestrator.
- **No version-controlled deployments.** Without CI/CD tooling (Schemachange, Terraform, or dbt), schema migrations are manual. This is fine for a reference project but not for a team of engineers.

In a real organization, you'd likely layer dbt on top for model management and testing, and potentially Airflow for cross-platform orchestration. But the underlying patterns — incremental stream-driven MERGE, timestamp-aware updates, SCD2 temporal joins, three-layer GDPR deletion, reconciliation views, DQ frameworks — remain the same regardless of tooling. **The patterns are the point, not the tool choices.**

---

## The Full Stack

For reference, here's every Snowflake feature the pipeline uses:

| Feature | Purpose |
|---------|---------|
| Streams (Bronze→Silver, Silver→Gold) | Two-tier incremental CDC |
| Tasks (parallel DAG) | Independent batch + event orchestration with retry logic |
| MERGE | Idempotent upserts with transaction wrapping |
| SCD Type 2 + temporal joins | Historical dimension tracking with point-in-time accuracy |
| Dynamic Data Masking | Role-based PII protection |
| Row Access Policies | Regional data filtering |
| ALERT + Notification Integration | Multi-channel alerting (email + webhook for Slack/PagerDuty) |
| Search Optimization | Fast point lookups on event data |
| Clustering | Partition pruning on date columns |
| VARIANT | Semi-structured JSON for clickstream events |
| Time Travel | Point-in-time recovery |
| Zero-Copy Clone | Instant dev/test environments |
| Stored Procedures (TRY/CATCH) | Transaction-safe, rerunnable transformation logic |
| INFORMATION_SCHEMA | Schema drift detection |
| SHOW STREAMS | Stream staleness monitoring |
| Window Functions (LAG) | Computed sessionization with inactivity timeout |

---

## What I'd Add Next

No pipeline is ever "done." Here's what's on the roadmap:

1. **Snowpipe** for continuous ingestion from an external stage (S3/GCS) — replacing the internal stage + PUT approach
2. **Dynamic Tables** as an alternative to the stream-driven stored procedures, making the gold build declarative
3. **CI/CD with Schemachange** for versioned, automated SQL deployments
4. **Streamlit in Snowflake** for an operational dashboard showing DQ status, pipeline health, and cost trends
5. **Column-level schema drift** using `INFER_SCHEMA()` on staged files to detect type/name changes, not just column count
6. **Partitioned event dedup** with time-bounded lookups for billion-row scale
7. **Cross-region GDPR** — deletion from external stages and downstream exports

---

## Try It Yourself

The full pipeline is open source on GitHub: **[snowflake-end-to-end-pipeline](https://github.com/vibhugupta/snowflake-end-to-end-pipeline)**

It includes sample data files, a 16-step setup sequence, and detailed documentation covering design decisions, the data model, and cost optimization strategies. You can have it running in a Snowflake trial account in under 30 minutes.

If you're building data pipelines and want to go beyond the basics, start with the problems, not the tools. Late-arriving data, PII governance, revenue reconciliation, silent failures — these are the challenges that separate a demo from production. Solve them first, and the rest falls into place.

---

*What data engineering challenges keep you up at night? I'd love to hear about the edge cases you've encountered — drop a comment or connect with me to discuss.*
