-- 40_data_quality_checks.sql
-- Data quality checks with results stored in UTIL.DQ_RESULTS.

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
  -- Row count check (orders)
  INSERT INTO DQ_RESULTS
  SELECT
    'orders_row_count' AS check_name,
    CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END AS status,
    COUNT(*)::STRING AS observed_value,
    '> 0' AS expected_value,
    'SILVER.ORDERS should not be empty' AS details,
    CURRENT_TIMESTAMP() AS check_ts
  FROM SILVER.ORDERS;

  -- Freshness check (orders within 24 hours)
  INSERT INTO DQ_RESULTS
  SELECT
    'orders_freshness_24h' AS check_name,
    CASE WHEN DATEDIFF('hour', MAX(order_ts), CURRENT_TIMESTAMP()) <= 24 THEN 'PASS' ELSE 'FAIL' END AS status,
    COALESCE(MAX(order_ts)::STRING, 'NULL') AS observed_value,
    'max(order_ts) within 24 hours' AS expected_value,
    'Detect stale ingestion' AS details,
    CURRENT_TIMESTAMP() AS check_ts
  FROM SILVER.ORDERS;

  -- Null check (orders.customer_id)
  INSERT INTO DQ_RESULTS
  SELECT
    'orders_customer_id_not_null' AS check_name,
    CASE WHEN COUNT_IF(customer_id IS NULL) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
    COUNT_IF(customer_id IS NULL)::STRING AS observed_value,
    '0' AS expected_value,
    'Customer IDs must be populated' AS details,
    CURRENT_TIMESTAMP() AS check_ts
  FROM SILVER.ORDERS;

  -- Uniqueness check (orders.order_id)
  INSERT INTO DQ_RESULTS
  SELECT
    'orders_unique_order_id' AS check_name,
    CASE WHEN COUNT(*) = COUNT(DISTINCT order_id) THEN 'PASS' ELSE 'FAIL' END AS status,
    COUNT(*)::STRING AS observed_value,
    'count == distinct(order_id)' AS expected_value,
    'Detect duplicate order IDs' AS details,
    CURRENT_TIMESTAMP() AS check_ts
  FROM SILVER.ORDERS;

  -- Referential integrity (orders -> customers)
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

  -- Referential integrity (payments -> orders)
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

  -- Referential integrity (returns -> orders)
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

  -- Refund amount sanity (refund should not exceed order total)
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

  -- Revenue reconciliation: orders with no payment
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

  RETURN 'SP_RUN_DQ_CHECKS completed';
END;
$$;

-- Optional manual run
-- CALL UTIL.SP_RUN_DQ_CHECKS();
