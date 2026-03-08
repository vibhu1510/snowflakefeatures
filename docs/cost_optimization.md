# Cost & Performance Optimization

## Warehouse Sizing
- **INGEST_WH (XSMALL)**: `COPY INTO` and stage reads are light and parallelizable. XSMALL is cost-efficient and sufficient for small to moderate batch loads.
- **TRANSFORM_WH (SMALL)**: `MERGE` operations and aggregations benefit from slightly more compute to reduce latency.
- **ANALYTICS_WH (SMALL)**: Enables interactive BI without competing with ETL workloads.

All warehouses use **auto-suspend** to prevent idle spend.

## Incremental Processing
- Streams ensure only new/changed rows are processed.
- Merges into silver and gold avoid full refreshes.
- Aggregations are limited to a rolling 30-day window to reduce scan cost.

## Clustering & Search Optimization
- Fact tables are clustered by date for partition pruning.
- Event lookups use **Search Optimization** on `user_id` and `session_id` for faster equality filters.

## Time Travel & Retention
Retention is set to **7 days** to balance recovery options with storage costs. This supports quick rollback of accidental data changes without keeping long history unnecessarily.

## Trade-offs
- Streams and tasks are chosen for explicit control. Dynamic Tables could reduce maintenance but are less transparent for operational debugging.
- SCD2 adds storage overhead but enables accurate historical analytics.
