# Data Model

## Bronze (Raw)
- `BRONZE.CUSTOMERS_RAW`: Raw customer records with metadata columns.
- `BRONZE.PRODUCTS_RAW`: Raw product catalog records.
- `BRONZE.ORDERS_RAW`: Raw orders with monetary fields.
- `BRONZE.PAYMENTS_RAW`: Raw payment transactions.
- `BRONZE.RETURNS_RAW`: Raw return/refund requests.
- `BRONZE.EVENTS_RAW`: Semi-structured clickstream events in VARIANT payload.

**Bronze Streams** (Bronze → Silver CDC):
- One stream per raw table (e.g., `BRONZE.CUSTOMERS_RAW_STREAM`), consumed by `SP_MERGE_SILVER_BATCH` / `SP_MERGE_SILVER_EVENTS`.

## Silver (Clean)
- `SILVER.CUSTOMERS`: Deduplicated customers with `region` (latest `updated_at`).
- `SILVER.PRODUCTS`: Clean product catalog.
- `SILVER.ORDERS`: Clean orders with consistent types.
- `SILVER.PAYMENTS`: Clean payment facts.
- `SILVER.RETURNS`: Clean return/refund records.
- `SILVER.EVENTS`: Parsed events with searchable fields.

**Silver Streams** (Silver → Gold CDC):
- `SILVER.CUSTOMERS_STREAM`, `SILVER.PRODUCTS_STREAM`, `SILVER.ORDERS_STREAM`, `SILVER.PAYMENTS_STREAM`, `SILVER.RETURNS_STREAM` — standard CDC streams.
- `SILVER.EVENTS_STREAM` — `APPEND_ONLY = TRUE` (events are immutable, no updates/deletes).

## Gold (Analytics)

### Dimensions
- `GOLD.DIM_CUSTOMER` (SCD2, PII masked for analysts)
  - Grain: one row per customer version
  - Keys: `customer_sk` (surrogate), `customer_id` (business)
  - SCD2 columns: `valid_from`, `valid_to`, `is_current`, `hash_diff` (SHA2-256)
  - Includes `region` for row-level security filtering
- `GOLD.DIM_PRODUCT` (Type 1)
  - Grain: one row per product
  - Keys: `product_sk`, `product_id`

### Facts
- `GOLD.FACT_ORDERS`
  - Grain: one row per order
  - Keys: `order_id` with `customer_sk` (SCD2 temporal join: `order_ts >= valid_from AND order_ts < valid_to`)
- `GOLD.FACT_PAYMENTS`
  - Grain: one row per payment
  - Keys: `payment_id`, `order_id`, `customer_sk` (SCD2 temporal join)
- `GOLD.FACT_EVENTS`
  - Grain: one row per event
  - Keys: `event_id`, `customer_sk`
- `GOLD.FACT_RETURNS`
  - Grain: one row per return
  - Keys: `return_id`, `order_id`, `customer_sk` (SCD2 temporal join)
- `GOLD.FACT_SESSIONS`
  - Grain: one row per session
  - Metrics: duration, page views, bounce/conversion flags
  - Includes `is_computed_session` flag — TRUE when session was derived from 30-min inactivity timeout, FALSE when source-provided

### Aggregates
- `GOLD.AGG_DAILY_SALES` (cash-basis)
  - Grain: one row per day
  - Metrics: total orders, gross revenue, refunds (by return processing date), net revenue, avg order value
- `GOLD.AGG_DAILY_SALES_ACCRUAL` (accrual-basis)
  - Grain: one row per day
  - Metrics: same as above, but refunds attributed back to the original order date (GAAP/IFRS view)

### Views
- `GOLD.VW_REVENUE_RECONCILIATION`
  - Order-level reconciliation: orders vs payments vs refunds
  - Flags: FULLY_PAID, UNDERPAID, OVERPAID, PARTIAL_PENDING, NO_PAYMENT
- `GOLD.VW_CONVERSION_FUNNEL`
  - Session-level funnel: page_view → product_view → add_to_cart → checkout → purchase
  - Drop-off percentages per step, by device type
- `GOLD.VW_EVENTS_WITH_COMPUTED_SESSION`
  - Enriches events with computed `session_id` using 30-min inactivity timeout (LAG window function)
  - Falls back to `COALESCE(original_session_id, 'COMPUTED_' || ...)` — source-provided session IDs pass through unchanged

## Utility (UTIL)
- `UTIL.DQ_RESULTS`: Data quality check results (20 checks: row counts, freshness, referential integrity, volume anomaly, stream staleness, schema drift, SCD2 overlap, cross-layer reconciliation)
- `UTIL.GDPR_AUDIT_LOG`: Audit trail for every GDPR deletion request (timestamped, per-layer entries)
- `UTIL.GDPR_SUPPRESSION_LIST`: Customer IDs that have been GDPR-deleted — prevents re-ingestion during COPY INTO and re-merge during silver processing
- `UTIL.DQ_FAILURES`: View filtering DQ_RESULTS to FAILs only
- `UTIL.VW_DQ_FAILURE_TREND`: 7-day rolling failure trend by check
- `UTIL.VW_DQ_LATEST_STATUS`: Most recent status for each DQ check
- `UTIL.VW_DQ_SUMMARY`: Category-level summary (Stream Health, Schema Drift, Referential Integrity, etc.)

## Procedures

### Ingestion
- `UTIL.SP_INGEST_BATCH()`: COPY INTO for batch tables + post-COPY GDPR suppression cleanup
- `UTIL.SP_INGEST_EVENTS()`: COPY INTO for events + post-COPY GDPR suppression cleanup

### Silver (transaction-wrapped, TRY/CATCH with ROLLBACK)
- `UTIL.SP_MERGE_SILVER_BATCH()`: MERGE for customers, products, orders, payments, returns — with GDPR suppression filtering
- `UTIL.SP_MERGE_SILVER_EVENTS()`: INSERT for events — with cross-batch dedup guard (`NOT IN SELECT event_id`) and GDPR suppression filtering
- `UTIL.SP_MERGE_SILVER()`: Convenience wrapper calling both

### Gold (stream-driven, SYSTEM$STREAM_HAS_DATA guards)
- `UTIL.SP_BUILD_GOLD_BATCH()`: Dimensions (SCD2 + Type 1), FACT_ORDERS/PAYMENTS/RETURNS, AGG_DAILY_SALES, AGG_DAILY_SALES_ACCRUAL
- `UTIL.SP_BUILD_GOLD_EVENTS()`: FACT_EVENTS, FACT_SESSIONS (with computed sessionization)
- `UTIL.SP_BUILD_GOLD()`: Convenience wrapper calling both

### Monitoring & Compliance
- `UTIL.SP_RUN_DQ_CHECKS()`: 20 data quality checks across silver, gold, streams, and schema
- `UTIL.SP_GDPR_DELETE(customer_id)`: Three-layer deletion (bronze → silver → gold anonymization) + suppression list + audit log

## Orchestration (Task DAG)
Two independent parallel paths:

```
Batch (hourly):
  TASK_INGEST_BATCH → TASK_MERGE_SILVER_BATCH → TASK_BUILD_GOLD_BATCH → TASK_DQ_CHECKS

Events (every 5 min):
  TASK_INGEST_EVENTS → TASK_MERGE_SILVER_EVENTS → TASK_BUILD_GOLD_EVENTS
```

Events reach gold within ~5-7 minutes of file landing. Batch failures do not block event processing.
