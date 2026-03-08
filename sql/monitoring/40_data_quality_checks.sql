-- 40_data_quality_checks.sql
-- Data quality checks with results stored in UTIL.DQ_RESULTS.
-- Covers: row counts, freshness, referential integrity, uniqueness, range validation,
--         volume anomaly detection, cross-layer reconciliation, SCD2 integrity,
--         stream staleness monitoring, and schema drift detection.

USE ROLE DE_TRANSFORM_ROLE;
USE WAREHOUSE TRANSFORM_WH;
USE DATABASE ECOMMERCE_DB;
USE SCHEMA UTIL;

CREATE TABLE IF NOT EXISTS DQ_RESULTS (
  check_name STRING,
  status STRING,
  observed_value STRING,
  expected_value STRING,
  details STRING,
  check_ts TIMESTAMP_NTZ
);

CREATE OR REPLACE VIEW DQ_FAILURES AS
SELECT * FROM DQ_RESULTS WHERE status = 'FAIL';

CREATE OR REPLACE PROCEDURE UTIL.SP_RUN_DQ_CHECKS()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
  -- ============================================================
  -- SILVER LAYER CHECKS (existing, refined)
  -- ============================================================

  -- 1. Row count check (silver orders)
  INSERT INTO DQ_RESULTS
  SELECT
    'orders_row_count' AS check_name,
    CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END AS status,
    COUNT(*)::STRING AS observed_value,
    '> 0' AS expected_value,
    'SILVER.ORDERS should not be empty' AS details,
    CURRENT_TIMESTAMP() AS check_ts
  FROM SILVER.ORDERS;

  -- 2. Freshness check (orders within 24 hours)
  INSERT INTO DQ_RESULTS
  SELECT
    'orders_freshness_24h' AS check_name,
    CASE WHEN DATEDIFF('hour', MAX(order_ts), CURRENT_TIMESTAMP()) <= 24 THEN 'PASS' ELSE 'FAIL' END AS status,
    COALESCE(MAX(order_ts)::STRING, 'NULL') AS observed_value,
    'max(order_ts) within 24 hours' AS expected_value,
    'Detect stale ingestion' AS details,
    CURRENT_TIMESTAMP() AS check_ts
  FROM SILVER.ORDERS;

  -- 3. Null check (orders.customer_id)
  INSERT INTO DQ_RESULTS
  SELECT
    'orders_customer_id_not_null' AS check_name,
    CASE WHEN COUNT_IF(customer_id IS NULL) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
    COUNT_IF(customer_id IS NULL)::STRING AS observed_value,
    '0' AS expected_value,
    'Customer IDs must be populated' AS details,
    CURRENT_TIMESTAMP() AS check_ts
  FROM SILVER.ORDERS;

  -- 4. Uniqueness check (orders.order_id)
  INSERT INTO DQ_RESULTS
  SELECT
    'orders_unique_order_id' AS check_name,
    CASE WHEN COUNT(*) = COUNT(DISTINCT order_id) THEN 'PASS' ELSE 'FAIL' END AS status,
    COUNT(*)::STRING AS observed_value,
    'count == distinct(order_id)' AS expected_value,
    'Detect duplicate order IDs' AS details,
    CURRENT_TIMESTAMP() AS check_ts
  FROM SILVER.ORDERS;

  -- 5. Referential integrity (orders -> customers)
  INSERT INTO DQ_RESULTS
  SELECT
    'orders_customer_fk' AS check_name,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
    COUNT(*)::STRING AS observed_value,
    '0' AS expected_value,
    'Orders must have existing customers' AS details,
    CURRENT_TIMESTAMP() AS check_ts
  FROM SILVER.ORDERS o
  LEFT JOIN SILVER.CUSTOMERS c
    ON o.customer_id = c.customer_id
  WHERE c.customer_id IS NULL;

  -- 6. Referential integrity (payments -> orders)
  INSERT INTO DQ_RESULTS
  SELECT
    'payments_order_fk' AS check_name,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
    COUNT(*)::STRING AS observed_value,
    '0' AS expected_value,
    'Payments must map to existing orders' AS details,
    CURRENT_TIMESTAMP() AS check_ts
  FROM SILVER.PAYMENTS p
  LEFT JOIN SILVER.ORDERS o
    ON p.order_id = o.order_id
  WHERE o.order_id IS NULL;

  -- 7. Referential integrity (returns -> orders)
  INSERT INTO DQ_RESULTS
  SELECT
    'returns_order_fk' AS check_name,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
    COUNT(*)::STRING AS observed_value,
    '0' AS expected_value,
    'Returns must map to existing orders' AS details,
    CURRENT_TIMESTAMP() AS check_ts
  FROM SILVER.RETURNS r
  LEFT JOIN SILVER.ORDERS o
    ON r.order_id = o.order_id
  WHERE o.order_id IS NULL;

  -- 8. Refund amount sanity (refund should not exceed order total)
  INSERT INTO DQ_RESULTS
  SELECT
    'returns_refund_not_exceeding_order' AS check_name,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
    COUNT(*)::STRING AS observed_value,
    '0' AS expected_value,
    'Refund amount should not exceed order total' AS details,
    CURRENT_TIMESTAMP() AS check_ts
  FROM SILVER.RETURNS r
  JOIN SILVER.ORDERS o ON r.order_id = o.order_id
  WHERE r.refund_amount > o.total;

  -- 9. Revenue reconciliation: orders with no payment
  INSERT INTO DQ_RESULTS
  SELECT
    'orders_with_no_payment' AS check_name,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'WARN' END AS status,
    COUNT(*)::STRING AS observed_value,
    '0' AS expected_value,
    'Orders should have at least one payment' AS details,
    CURRENT_TIMESTAMP() AS check_ts
  FROM SILVER.ORDERS o
  LEFT JOIN SILVER.PAYMENTS p ON o.order_id = p.order_id
  WHERE p.payment_id IS NULL;

  -- ============================================================
  -- GOLD LAYER CHECKS (new)
  -- ============================================================

  -- 10. Gold fact orders row count (gold should have data if silver does)
  INSERT INTO DQ_RESULTS
  SELECT
    'gold_fact_orders_row_count' AS check_name,
    CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END AS status,
    COUNT(*)::STRING AS observed_value,
    '> 0' AS expected_value,
    'GOLD.FACT_ORDERS should not be empty' AS details,
    CURRENT_TIMESTAMP() AS check_ts
  FROM GOLD.FACT_ORDERS;

  -- 11. Null customer_sk in fact tables (surrogate key coverage)
  INSERT INTO DQ_RESULTS
  SELECT
    'fact_orders_customer_sk_not_null' AS check_name,
    CASE WHEN COUNT_IF(customer_sk IS NULL) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
    COUNT_IF(customer_sk IS NULL)::STRING AS observed_value,
    '0' AS expected_value,
    'FACT_ORDERS should have no NULL customer_sk (dimension lookup failure)' AS details,
    CURRENT_TIMESTAMP() AS check_ts
  FROM GOLD.FACT_ORDERS;

  -- 12. Payment amount range validation (no negatives, no extreme outliers)
  INSERT INTO DQ_RESULTS
  SELECT
    'payment_amount_range' AS check_name,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
    COUNT(*)::STRING AS observed_value,
    '0 (all amounts in 0-100000 range)' AS expected_value,
    'Payments outside valid range (0 to 100,000) may indicate data corruption' AS details,
    CURRENT_TIMESTAMP() AS check_ts
  FROM GOLD.FACT_PAYMENTS
  WHERE amount < 0 OR amount > 100000;

  -- 13. Event volume anomaly detection (today vs 7-day rolling average)
  INSERT INTO DQ_RESULTS
  SELECT
    'event_volume_anomaly' AS check_name,
    CASE
      WHEN avg_7d = 0 THEN 'WARN'
      WHEN ABS(today_count - avg_7d) / NULLIF(avg_7d, 0) <= 0.5 THEN 'PASS'
      ELSE 'FAIL'
    END AS status,
    today_count::STRING AS observed_value,
    ROUND(avg_7d, 0)::STRING || ' (7d avg ± 50%)' AS expected_value,
    'Event volume deviation > 50% from 7-day rolling average' AS details,
    CURRENT_TIMESTAMP() AS check_ts
  FROM (
    SELECT
      (SELECT COUNT(*) FROM GOLD.FACT_EVENTS WHERE event_date = CURRENT_DATE()) AS today_count,
      (SELECT AVG(daily_count) FROM (
        SELECT event_date, COUNT(*) AS daily_count
        FROM GOLD.FACT_EVENTS
        WHERE event_date BETWEEN DATEADD('day', -7, CURRENT_DATE()) AND DATEADD('day', -1, CURRENT_DATE())
        GROUP BY event_date
      )) AS avg_7d
  );

  -- 14. Cross-layer reconciliation: silver vs gold order counts
  INSERT INTO DQ_RESULTS
  SELECT
    'gold_silver_order_count_recon' AS check_name,
    CASE
      WHEN silver_count = 0 AND gold_count = 0 THEN 'PASS'
      WHEN silver_count = 0 THEN 'WARN'
      WHEN ABS(silver_count - gold_count) / NULLIF(silver_count, 0) <= 0.01 THEN 'PASS'
      ELSE 'FAIL'
    END AS status,
    'silver=' || silver_count::STRING || ', gold=' || gold_count::STRING AS observed_value,
    'Counts within 1% tolerance' AS expected_value,
    'Silver vs Gold order count reconciliation' AS details,
    CURRENT_TIMESTAMP() AS check_ts
  FROM (
    SELECT
      (SELECT COUNT(*) FROM SILVER.ORDERS) AS silver_count,
      (SELECT COUNT(*) FROM GOLD.FACT_ORDERS) AS gold_count
  );

  -- 15. SCD2 no overlapping valid_from/valid_to for same customer_id
  INSERT INTO DQ_RESULTS
  SELECT
    'dim_customer_scd2_no_overlap' AS check_name,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
    COUNT(*)::STRING AS observed_value,
    '0' AS expected_value,
    'DIM_CUSTOMER SCD2 records should have non-overlapping valid_from/valid_to per customer_id' AS details,
    CURRENT_TIMESTAMP() AS check_ts
  FROM GOLD.DIM_CUSTOMER a
  JOIN GOLD.DIM_CUSTOMER b
    ON a.customer_id = b.customer_id
    AND a.customer_sk < b.customer_sk
    AND a.valid_from < b.valid_to
    AND a.valid_to > b.valid_from;

  -- 16. Event uniqueness in gold (detect dedup failures)
  INSERT INTO DQ_RESULTS
  SELECT
    'gold_events_unique_event_id' AS check_name,
    CASE WHEN COUNT(*) = COUNT(DISTINCT event_id) THEN 'PASS' ELSE 'FAIL' END AS status,
    'total=' || COUNT(*)::STRING || ', distinct=' || COUNT(DISTINCT event_id)::STRING AS observed_value,
    'count == distinct(event_id)' AS expected_value,
    'GOLD.FACT_EVENTS should have no duplicate event_ids' AS details,
    CURRENT_TIMESTAMP() AS check_ts
  FROM GOLD.FACT_EVENTS;

  -- ============================================================
  -- STREAM STALENESS CHECKS (Issue 6)
  -- ============================================================
  -- Snowflake streams can become stale if not consumed within the
  -- data retention period. Stale streams lose their offset and
  -- require a full re-process. We check both Bronze and Silver streams.

  -- 17. Bronze stream staleness
  SHOW STREAMS IN SCHEMA BRONZE;
  INSERT INTO DQ_RESULTS
  SELECT
    'stream_staleness_bronze_' || LOWER("name") AS check_name,
    CASE WHEN "stale" = 'true' THEN 'FAIL' ELSE 'PASS' END AS status,
    COALESCE("stale_after"::STRING, 'N/A') AS observed_value,
    'Not stale' AS expected_value,
    'Bronze stream: ' || "name" || ' (stale_after: ' || COALESCE("stale_after"::STRING, 'N/A') || ')' AS details,
    CURRENT_TIMESTAMP() AS check_ts
  FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

  -- 18. Silver stream staleness
  SHOW STREAMS IN SCHEMA SILVER;
  INSERT INTO DQ_RESULTS
  SELECT
    'stream_staleness_silver_' || LOWER("name") AS check_name,
    CASE WHEN "stale" = 'true' THEN 'FAIL' ELSE 'PASS' END AS status,
    COALESCE("stale_after"::STRING, 'N/A') AS observed_value,
    'Not stale' AS expected_value,
    'Silver stream: ' || "name" || ' (stale_after: ' || COALESCE("stale_after"::STRING, 'N/A') || ')' AS details,
    CURRENT_TIMESTAMP() AS check_ts
  FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

  -- ============================================================
  -- SCHEMA DRIFT DETECTION (Issue 10)
  -- ============================================================
  -- Detects if source table column counts deviate from expected.
  -- This catches added/dropped columns that could break COPY INTO or MERGEs.
  -- Expected column counts include metadata columns (ingestion_ts, source_file, load_id).

  -- 19. Schema drift: Bronze raw tables
  INSERT INTO DQ_RESULTS
  SELECT
    'schema_drift_' || LOWER(table_name) AS check_name,
    CASE
      WHEN table_name = 'CUSTOMERS_RAW' AND column_count <> 11 THEN 'FAIL'
      WHEN table_name = 'PRODUCTS_RAW' AND column_count <> 11 THEN 'FAIL'
      WHEN table_name = 'ORDERS_RAW' AND column_count <> 13 THEN 'FAIL'
      WHEN table_name = 'PAYMENTS_RAW' AND column_count <> 10 THEN 'FAIL'
      WHEN table_name = 'RETURNS_RAW' AND column_count <> 11 THEN 'FAIL'
      WHEN table_name = 'EVENTS_RAW' AND column_count <> 10 THEN 'FAIL'
      ELSE 'PASS'
    END AS status,
    column_count::STRING AS observed_value,
    CASE
      WHEN table_name = 'CUSTOMERS_RAW' THEN '11'
      WHEN table_name = 'PRODUCTS_RAW' THEN '11'
      WHEN table_name = 'ORDERS_RAW' THEN '13'
      WHEN table_name = 'PAYMENTS_RAW' THEN '10'
      WHEN table_name = 'RETURNS_RAW' THEN '11'
      WHEN table_name = 'EVENTS_RAW' THEN '10'
      ELSE 'unknown'
    END AS expected_value,
    'Schema drift detected in ' || table_name || ': expected column count mismatch' AS details,
    CURRENT_TIMESTAMP() AS check_ts
  FROM (
    SELECT table_name, COUNT(*) AS column_count
    FROM ECOMMERCE_DB.INFORMATION_SCHEMA.COLUMNS
    WHERE table_schema = 'BRONZE'
      AND table_name IN ('CUSTOMERS_RAW','PRODUCTS_RAW','ORDERS_RAW','PAYMENTS_RAW','RETURNS_RAW','EVENTS_RAW')
    GROUP BY table_name
  );

  -- 20. Schema drift: Silver tables
  INSERT INTO DQ_RESULTS
  SELECT
    'schema_drift_silver_' || LOWER(table_name) AS check_name,
    CASE
      WHEN table_name = 'CUSTOMERS' AND column_count <> 8 THEN 'FAIL'
      WHEN table_name = 'PRODUCTS' AND column_count <> 8 THEN 'FAIL'
      WHEN table_name = 'ORDERS' AND column_count <> 10 THEN 'FAIL'
      WHEN table_name = 'PAYMENTS' AND column_count <> 7 THEN 'FAIL'
      WHEN table_name = 'RETURNS' AND column_count <> 8 THEN 'FAIL'
      WHEN table_name = 'EVENTS' AND column_count <> 8 THEN 'FAIL'
      ELSE 'PASS'
    END AS status,
    column_count::STRING AS observed_value,
    CASE
      WHEN table_name = 'CUSTOMERS' THEN '8'
      WHEN table_name = 'PRODUCTS' THEN '8'
      WHEN table_name = 'ORDERS' THEN '10'
      WHEN table_name = 'PAYMENTS' THEN '7'
      WHEN table_name = 'RETURNS' THEN '8'
      WHEN table_name = 'EVENTS' THEN '8'
      ELSE 'unknown'
    END AS expected_value,
    'Schema drift detected in SILVER.' || table_name || ': expected column count mismatch' AS details,
    CURRENT_TIMESTAMP() AS check_ts
  FROM (
    SELECT table_name, COUNT(*) AS column_count
    FROM ECOMMERCE_DB.INFORMATION_SCHEMA.COLUMNS
    WHERE table_schema = 'SILVER'
      AND table_name IN ('CUSTOMERS','PRODUCTS','ORDERS','PAYMENTS','RETURNS','EVENTS')
    GROUP BY table_name
  );

  RETURN 'SP_RUN_DQ_CHECKS completed: 20 checks executed';
END;
$$;

-- Optional manual run
-- CALL UTIL.SP_RUN_DQ_CHECKS();
-- SELECT * FROM UTIL.DQ_RESULTS ORDER BY check_ts DESC LIMIT 30;
-- SELECT * FROM UTIL.DQ_FAILURES ORDER BY check_ts DESC;
