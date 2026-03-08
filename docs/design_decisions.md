# Design Decisions

## 1. Business Domain: E-commerce
E-commerce provides a realistic blend of **batch** (orders, customers, payments) and **event-style** (clickstream) data. It also naturally supports fact/dimension modeling and SCD use cases.

## 2. Layered Architecture (Bronze → Silver → Gold)
- **Bronze** stores raw data with metadata to preserve lineage and support reprocessing.
- **Silver** cleanses and standardizes types, enabling consistent joins and deduplication.
- **Gold** delivers analytics-ready facts and dimensions optimized for BI workloads.

This separation enables repeatable backfills and makes troubleshooting easier when issues arise.

## 3. Streams + Tasks (Over Dynamic Tables)
Streams capture **incremental changes** with low overhead and are easy to reason about for DML-heavy sources. Tasks provide a clear dependency chain for orchestration. The pipeline reflects common enterprise patterns where control and auditability matter more than auto-refresh simplicity.

## 4. SCD2 Customer Dimension
Customers change (email, phone). SCD2 preserves historical accuracy for revenue attribution and retention analysis. Product dimension is Type 1 because corrections usually replace old values.

## 5. Cost Controls
Smaller warehouses are used for ingestion and transformations, with auto-suspend enabled. Incremental merges prevent unnecessary full refreshes, keeping compute use predictable.

## 6. Observability First
Monitoring SQL is stored alongside transformations to make troubleshooting **discoverable**. Query history and task history act as operational runbooks. A Snowflake ALERT monitors DQ failures every 15 minutes with email notification, and trend views show failure patterns over time.

## 7. Returns & Net Revenue
Every e-commerce platform deals with refunds. `FACT_RETURNS` tracks return reasons and refund amounts. `AGG_DAILY_SALES` was upgraded to show **gross revenue, refunds, and net revenue** — which is what finance actually cares about. The revenue reconciliation view flags mismatches between orders and payments (partial, missing, overpaid).

## 8. PII Masking & Row-Level Security
Enterprise governance requires controlling who sees what. Dynamic data masking policies on `email`, `phone`, and names ensure that `DE_ANALYST_ROLE` sees masked values while `DE_TRANSFORM_ROLE` sees the real data. A row access policy with region tags enables regional sales managers to only see their customers.

## 9. GDPR Right-to-Delete
Compliance is non-negotiable. `SP_GDPR_DELETE` performs a three-layer deletion cascade:
- **Bronze**: Hard-delete from all 5 raw tables (`CUSTOMERS_RAW`, `ORDERS_RAW`, `PAYMENTS_RAW`, `RETURNS_RAW`, `EVENTS_RAW`).
- **Silver**: Hard-delete from all 5 cleaned tables.
- **Gold**: Anonymize `DIM_CUSTOMER` (replace PII with `REDACTED`, preserve surrogate keys so aggregates remain valid).

A **suppression list** (`GDPR_SUPPRESSION_LIST`) prevents re-ingestion: `SP_INGEST_BATCH` and `SP_INGEST_EVENTS` run post-COPY cleanup against it, and `SP_MERGE_SILVER_BATCH` / `SP_MERGE_SILVER_EVENTS` filter suppressed IDs during MERGE/INSERT. Every action is logged to `GDPR_AUDIT_LOG` for regulatory proof.

**Time Travel & Fail-Safe caveat**: Snowflake retains deleted data in Time Travel (`DATA_RETENTION_TIME_IN_DAYS = 7`) and Fail-Safe (additional 7 days, Snowflake-controlled). Total: up to 14 days before physical deletion. This must be documented in your Data Processing Agreement (DPA). To reduce exposure, set `DATA_RETENTION_TIME_IN_DAYS = 1` on PII-containing tables after deletion.

## 10. Clickstream Sessionization
Raw events are aggregated into `FACT_SESSIONS` with duration, bounce detection, and conversion flags. `VW_CONVERSION_FUNNEL` provides step-by-step funnel analysis (page_view → product_view → add_to_cart → checkout → purchase) with drop-off rates by device type.

**Computed sessionization**: Not all event sources provide a `session_id`. `VW_EVENTS_WITH_COMPUTED_SESSION` uses a 30-minute inactivity timeout (via `LAG` window function) to assign session boundaries when the source `session_id` is NULL. Events with a pre-supplied `session_id` pass through unchanged. Computed sessions are flagged with `is_computed_session = TRUE` in `FACT_SESSIONS` for downstream filtering.

## 11. Schema Evolution Limitations
Bronze ingestion uses `ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE` to handle extra columns gracefully. However, this only covers additive changes. It does **not** protect against:
- Column type changes (e.g., `STRING` → `NUMBER`)
- Column reordering (positional CSV parsing breaks)
- Column renames (old column appears dropped, new one added)

**Mitigation**: Schema drift DQ checks (`schema_drift_*`) monitor column counts for each Bronze raw table. For proactive detection, consider `INFER_SCHEMA()` on staged files before COPY INTO to compare against table DDL. For critical sources, pin the file format to a named schema version and validate before loading.

## 12. Stream Staleness Risk
Snowflake streams become stale if not consumed within the table's `DATA_RETENTION_TIME_IN_DAYS` (7 days for this database). A stale stream loses its offset and cannot be used — recovery requires recreating the stream (which means a full re-process for that table).

**Mitigation**:
- DQ checks (`stream_staleness_bronze_*`, `stream_staleness_silver_*`) monitor all streams via `SHOW STREAMS` and flag stale ones as FAIL.
- Tasks use `SUSPEND_TASK_AFTER_NUM_FAILURES` to prevent runaway retries while preserving the stream offset.
- Events path runs every 5 minutes (well within the 7-day window), but batch path should be monitored during long holidays or outages.

## 13. Incremental Processing & SCD2 Temporal Joins
The gold build is fully stream-driven: each dimension/fact only processes when `SYSTEM$STREAM_HAS_DATA()` returns TRUE for its corresponding Silver→Gold stream. This eliminates unnecessary full-table scans on quiet periods.

**SCD2 temporal joins**: Fact tables join to `DIM_CUSTOMER` using point-in-time range joins (`order_ts >= valid_from AND order_ts < valid_to`) instead of `is_current = TRUE`. This ensures historical orders retain the customer attributes that were active at the time of the transaction, not the customer's current attributes. This is critical for accurate retention cohorts and revenue attribution.

## 14. Accrual vs Cash-Basis Revenue
Two aggregate tables serve different stakeholders:
- **`AGG_DAILY_SALES`** (cash-basis): Refunds counted on the date the return was processed. Matches bank statements.
- **`AGG_DAILY_SALES_ACCRUAL`** (accrual-basis): Refunds attributed back to the original order date. Required for GAAP/IFRS reporting and gives a more accurate picture of "how well did sales perform on day X."

## 15. Parallel Task DAG
Batch and event paths are fully decoupled:
- **Batch** (hourly): `TASK_INGEST_BATCH → TASK_MERGE_SILVER_BATCH → TASK_BUILD_GOLD_BATCH → TASK_DQ_CHECKS`
- **Events** (every 5 min): `TASK_INGEST_EVENTS → TASK_MERGE_SILVER_EVENTS → TASK_BUILD_GOLD_EVENTS`

Events reach gold within ~5-7 minutes of file landing instead of waiting up to 60 minutes for the next batch cycle. This decoupling also prevents a batch failure from blocking event processing and vice versa.
