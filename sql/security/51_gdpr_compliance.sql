-- 51_gdpr_compliance.sql
-- GDPR right-to-delete: hard-delete from bronze + silver, anonymize in gold, audit trail.
-- Includes suppression list to prevent re-ingestion of deleted customer data.

USE ROLE DE_TRANSFORM_ROLE;
USE WAREHOUSE TRANSFORM_WH;
USE DATABASE ECOMMERCE_DB;
USE SCHEMA UTIL;

-- Audit log for GDPR deletions
CREATE TABLE IF NOT EXISTS GDPR_AUDIT_LOG (
  request_id STRING DEFAULT UUID_STRING(),
  customer_id STRING,
  action STRING,
  layer STRING,
  table_name STRING,
  rows_affected NUMBER,
  requested_by STRING,
  executed_at TIMESTAMP_NTZ
);

-- Suppression list: prevents re-ingestion of deleted customer data.
-- Checked during ingestion (post-COPY cleanup) and silver merge (WHERE filter).
CREATE TABLE IF NOT EXISTS GDPR_SUPPRESSION_LIST (
  customer_id STRING NOT NULL,
  suppressed_at TIMESTAMP_NTZ,
  requested_by STRING
);

CREATE OR REPLACE PROCEDURE SP_GDPR_DELETE(P_CUSTOMER_ID STRING)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
  v_ts TIMESTAMP_NTZ := CURRENT_TIMESTAMP();
  v_request_id STRING := UUID_STRING();
BEGIN
  -- Validate input
  IF (P_CUSTOMER_ID IS NULL OR TRIM(P_CUSTOMER_ID) = '') THEN
    RETURN 'ERROR: customer_id is required';
  END IF;

  -- Check customer exists in at least one layer
  LET v_exists_silver NUMBER := (SELECT COUNT(*) FROM SILVER.CUSTOMERS WHERE customer_id = :P_CUSTOMER_ID);
  LET v_exists_bronze NUMBER := (SELECT COUNT(*) FROM BRONZE.CUSTOMERS_RAW WHERE customer_id = :P_CUSTOMER_ID);
  LET v_exists_gold NUMBER := (SELECT COUNT(*) FROM GOLD.DIM_CUSTOMER WHERE customer_id = :P_CUSTOMER_ID);

  IF (v_exists_silver = 0 AND v_exists_bronze = 0 AND v_exists_gold = 0) THEN
    RETURN 'ERROR: customer_id ' || P_CUSTOMER_ID || ' not found in any layer';
  END IF;

  -- ============================================================
  -- 1. Hard-delete from BRONZE raw tables
  -- ============================================================
  DELETE FROM BRONZE.EVENTS_RAW WHERE user_id = :P_CUSTOMER_ID;
  LET v_bronze_events NUMBER := SQLROWCOUNT;

  DELETE FROM BRONZE.PAYMENTS_RAW WHERE order_id IN (
    SELECT order_id FROM BRONZE.ORDERS_RAW WHERE customer_id = :P_CUSTOMER_ID
  );
  LET v_bronze_payments NUMBER := SQLROWCOUNT;

  DELETE FROM BRONZE.RETURNS_RAW WHERE customer_id = :P_CUSTOMER_ID;
  LET v_bronze_returns NUMBER := SQLROWCOUNT;

  DELETE FROM BRONZE.ORDERS_RAW WHERE customer_id = :P_CUSTOMER_ID;
  LET v_bronze_orders NUMBER := SQLROWCOUNT;

  DELETE FROM BRONZE.CUSTOMERS_RAW WHERE customer_id = :P_CUSTOMER_ID;
  LET v_bronze_customers NUMBER := SQLROWCOUNT;

  -- ============================================================
  -- 2. Hard-delete from SILVER tables
  -- ============================================================
  DELETE FROM SILVER.EVENTS WHERE user_id = :P_CUSTOMER_ID;
  LET v_silver_events NUMBER := SQLROWCOUNT;

  DELETE FROM SILVER.PAYMENTS WHERE order_id IN (
    SELECT order_id FROM SILVER.ORDERS WHERE customer_id = :P_CUSTOMER_ID
  );
  LET v_silver_payments NUMBER := SQLROWCOUNT;

  DELETE FROM SILVER.RETURNS WHERE customer_id = :P_CUSTOMER_ID;
  LET v_silver_returns NUMBER := SQLROWCOUNT;

  DELETE FROM SILVER.ORDERS WHERE customer_id = :P_CUSTOMER_ID;
  LET v_silver_orders NUMBER := SQLROWCOUNT;

  DELETE FROM SILVER.CUSTOMERS WHERE customer_id = :P_CUSTOMER_ID;
  LET v_silver_customers NUMBER := SQLROWCOUNT;

  -- ============================================================
  -- 3. Anonymize in GOLD (preserve surrogate keys so aggregates remain valid)
  -- ============================================================
  UPDATE GOLD.DIM_CUSTOMER
  SET first_name = 'REDACTED',
      last_name = 'REDACTED',
      email = 'REDACTED',
      phone = 'REDACTED',
      hash_diff = SHA2('REDACTED', 256),
      load_ts = :v_ts
  WHERE customer_id = :P_CUSTOMER_ID;
  LET v_dim_rows NUMBER := SQLROWCOUNT;

  -- Fact tables keep surrogate key references intact (aggregates still work)
  -- Revenue totals remain accurate; the customer simply can't be identified.

  -- ============================================================
  -- 4. Add to suppression list (prevents re-ingestion)
  -- ============================================================
  INSERT INTO UTIL.GDPR_SUPPRESSION_LIST (customer_id, suppressed_at, requested_by)
  SELECT :P_CUSTOMER_ID, :v_ts, CURRENT_USER()
  WHERE NOT EXISTS (
    SELECT 1 FROM UTIL.GDPR_SUPPRESSION_LIST WHERE customer_id = :P_CUSTOMER_ID
  );

  -- ============================================================
  -- 5. Audit trail
  -- ============================================================
  INSERT INTO UTIL.GDPR_AUDIT_LOG (request_id, customer_id, action, layer, table_name, rows_affected, requested_by, executed_at)
  VALUES
    -- Bronze layer
    (:v_request_id, :P_CUSTOMER_ID, 'HARD_DELETE', 'BRONZE', 'CUSTOMERS_RAW', :v_bronze_customers, CURRENT_USER(), :v_ts),
    (:v_request_id, :P_CUSTOMER_ID, 'HARD_DELETE', 'BRONZE', 'ORDERS_RAW', :v_bronze_orders, CURRENT_USER(), :v_ts),
    (:v_request_id, :P_CUSTOMER_ID, 'HARD_DELETE', 'BRONZE', 'PAYMENTS_RAW', :v_bronze_payments, CURRENT_USER(), :v_ts),
    (:v_request_id, :P_CUSTOMER_ID, 'HARD_DELETE', 'BRONZE', 'RETURNS_RAW', :v_bronze_returns, CURRENT_USER(), :v_ts),
    (:v_request_id, :P_CUSTOMER_ID, 'HARD_DELETE', 'BRONZE', 'EVENTS_RAW', :v_bronze_events, CURRENT_USER(), :v_ts),
    -- Silver layer
    (:v_request_id, :P_CUSTOMER_ID, 'HARD_DELETE', 'SILVER', 'CUSTOMERS', :v_silver_customers, CURRENT_USER(), :v_ts),
    (:v_request_id, :P_CUSTOMER_ID, 'HARD_DELETE', 'SILVER', 'ORDERS', :v_silver_orders, CURRENT_USER(), :v_ts),
    (:v_request_id, :P_CUSTOMER_ID, 'HARD_DELETE', 'SILVER', 'PAYMENTS', :v_silver_payments, CURRENT_USER(), :v_ts),
    (:v_request_id, :P_CUSTOMER_ID, 'HARD_DELETE', 'SILVER', 'RETURNS', :v_silver_returns, CURRENT_USER(), :v_ts),
    (:v_request_id, :P_CUSTOMER_ID, 'HARD_DELETE', 'SILVER', 'EVENTS', :v_silver_events, CURRENT_USER(), :v_ts),
    -- Gold layer
    (:v_request_id, :P_CUSTOMER_ID, 'ANONYMIZE', 'GOLD', 'DIM_CUSTOMER', :v_dim_rows, CURRENT_USER(), :v_ts),
    -- Suppression
    (:v_request_id, :P_CUSTOMER_ID, 'SUPPRESS', 'UTIL', 'GDPR_SUPPRESSION_LIST', 1, CURRENT_USER(), :v_ts),
    -- Time Travel / Fail-Safe warning
    (:v_request_id, :P_CUSTOMER_ID, 'TIME_TRAVEL_WARNING', 'ALL', 'ALL_TABLES', 0, CURRENT_USER(), :v_ts);

  -- NOTE on Time Travel & Fail-Safe:
  -- Snowflake retains deleted data in Time Travel (DATA_RETENTION_TIME_IN_DAYS = 7 for this DB)
  -- and Fail-Safe (additional 7 days, Snowflake-controlled, not queryable).
  -- Total: up to 14 days before physical deletion.
  -- For strict GDPR compliance, document this in your Data Processing Agreement (DPA).
  -- To reduce exposure: ALTER TABLE <table> SET DATA_RETENTION_TIME_IN_DAYS = 1;
  -- on PII-containing tables after deletion.

  RETURN 'GDPR delete completed for ' || P_CUSTOMER_ID ||
         '. Request ID: ' || v_request_id ||
         '. Customer added to suppression list.' ||
         ' Note: Time Travel (7d) + Fail-Safe (7d) = data persists up to 14 days before physical removal.';
END;
$$;

-- Verification
-- CALL UTIL.SP_GDPR_DELETE('C001');
-- SELECT * FROM UTIL.GDPR_AUDIT_LOG ORDER BY executed_at DESC;
-- SELECT * FROM UTIL.GDPR_SUPPRESSION_LIST;
-- SELECT * FROM GOLD.DIM_CUSTOMER WHERE customer_id = 'C001';  -- Should show REDACTED
-- SELECT * FROM BRONZE.CUSTOMERS_RAW WHERE customer_id = 'C001';  -- Should return 0 rows
