# Cost & Performance Optimization

## Warehouse Sizing
- **INGEST_WH (XSMALL)**: `COPY INTO` and stage reads are light and parallelizable. XSMALL is cost-efficient and sufficient for small to moderate batch loads.
- **TRANSFORM_WH (SMALL)**: `MERGE` operations and aggregations benefit from slightly more compute to reduce latency.
- **ANALYTICS_WH (SMALL)**: Enables interactive BI without competing with ETL workloads.

All warehouses use **auto-suspend** to prevent idle spend.

## Incremental Processing
- **Bronze → Silver**: Streams on raw tables capture only new/changed rows. `SP_MERGE_SILVER_BATCH` and `SP_MERGE_SILVER_EVENTS` consume these streams incrementally.
- **Silver → Gold**: Six Silver→Gold streams feed the gold build. Each dimension/fact is guarded by `SYSTEM$STREAM_HAS_DATA()` — if a stream is empty (no changes), the corresponding MERGE/INSERT is skipped entirely. This avoids unnecessary full-table scans on quiet periods.
- **Aggregations**: `AGG_DAILY_SALES` and `AGG_DAILY_SALES_ACCRUAL` are only rebuilt when their upstream fact tables were actually updated in the current run, controlled by boolean flags in the procedure.

## Parallel Task DAG
The task DAG is split into two independent paths:
- **Batch (hourly)**: dimensions + transaction facts + aggregates + DQ checks.
- **Events (every 5 min)**: clickstream events + sessionization.

This means:
- Events are processed 12x more frequently without inflating batch costs.
- A batch failure does not block event freshness (and vice versa).
- Warehouses auto-suspend between runs — the 5-min event path only wakes `INGEST_WH` and `TRANSFORM_WH` briefly per cycle.

## Clustering & Search Optimization
- Fact tables are clustered by date columns (`order_date`, `event_date`, `return_date`, `payment_ts`) for partition pruning on time-range queries.
- `DIM_CUSTOMER` is clustered on `(customer_id, is_current)` for fast current-record lookups.
- Event lookups use **Search Optimization** on `user_id` and `session_id` for faster equality filters.

## Hashing
SHA2-256 is used for SCD2 `hash_diff` computation (replacing MD5). SHA2 is slightly more expensive per row but avoids collision risk at scale. The overhead is negligible — SHA2 adds ~5-10% to the hash computation step, which is a tiny fraction of overall MERGE cost.

## Time Travel & Retention
Retention is set to **7 days** to balance recovery options with storage costs. This supports quick rollback of accidental data changes without keeping long history unnecessarily.

**GDPR consideration**: After a GDPR deletion, the deleted data remains accessible via Time Travel for up to 7 days + 7 days Fail-Safe. To minimize PII exposure, consider setting `DATA_RETENTION_TIME_IN_DAYS = 1` on PII-containing tables (`CUSTOMERS_RAW`, `SILVER.CUSTOMERS`, `DIM_CUSTOMER`) after executing `SP_GDPR_DELETE`.

## Stream Staleness Prevention
Streams become stale if not consumed within the `DATA_RETENTION_TIME_IN_DAYS` window (7 days). Recovery from a stale stream requires recreating it and doing a full re-process — expensive.

**Prevention**:
- Events path runs every 5 minutes (well within the 7-day window).
- Batch path runs hourly — safe under normal operations, but monitor during extended outages or holiday freezes.
- DQ checks (`stream_staleness_bronze_*`, `stream_staleness_silver_*`) proactively detect approaching staleness.
- `SUSPEND_TASK_AFTER_NUM_FAILURES` prevents runaway retries that could advance the stream offset without successful processing.

## Trade-offs
- Streams and tasks are chosen for explicit control. Dynamic Tables could reduce maintenance but are less transparent for operational debugging.
- SCD2 adds storage overhead but enables accurate historical analytics with temporal joins.
- The parallel DAG doubles the number of tasks (7 vs 5) but eliminates the coupling bottleneck where events waited for batch completion.
- 20 DQ checks add ~30-60 seconds per batch run. The `SHOW STREAMS` checks are particularly lightweight. Schema drift checks query `INFORMATION_SCHEMA.COLUMNS` which is a metadata operation with no warehouse cost.
