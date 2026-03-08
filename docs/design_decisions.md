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
Compliance is non-negotiable. `SP_GDPR_DELETE` hard-deletes PII from silver tables and anonymizes gold dimension records (replacing PII with `REDACTED` while preserving surrogate keys so aggregates remain valid). Every action is logged to `GDPR_AUDIT_LOG` for regulatory proof.

## 10. Clickstream Sessionization
Raw events are aggregated into `FACT_SESSIONS` with duration, bounce detection, and conversion flags. `VW_CONVERSION_FUNNEL` provides step-by-step funnel analysis (page_view → product_view → add_to_cart → checkout → purchase) with drop-off rates by device type — the exact view a product team would query.
