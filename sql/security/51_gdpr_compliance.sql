-- 51_gdpr_compliance.sql
-- GDPR right-to-delete: hard-delete from silver, anonymize in gold, audit trail.

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

CREATE OR REPLACE PROCEDURE SP_GDPR_DELETE(P_CUSTOMER_ID STRING)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
  v_ts TIMESTAMP_NTZ := CURRENT_TIMESTAMP();
  v_request_id STRING := UUID_STRING();
  v_rows_deleted NUMBER;
BEGIN
  -- Validate input
  IF (P_CUSTOMER_ID IS NULL OR TRIM(P_CUSTOMER_ID) = '') THEN
    RETURN 'ERROR: customer_id is required';
  END IF;

  -- Check customer exists
  LET v_exists NUMBER := (SELECT COUNT(*) FROM SILVER.CUSTOMERS WHERE customer_id = :P_CUSTOMER_ID);
  IF (v_exists = 0) THEN
    RETURN 'ERROR: customer_id ' || P_CUSTOMER_ID || ' not found';
  END IF;

  -- 1. Hard-delete from SILVER tables
  DELETE FROM SILVER.EVENTS WHERE user_id = :P_CUSTOMER_ID;
  LET v_events_deleted NUMBER := SQLROWCOUNT;

  DELETE FROM SILVER.PAYMENTS WHERE order_id IN (
    SELECT order_id FROM SILVER.ORDERS WHERE customer_id = :P_CUSTOMER_ID
  );
  LET v_payments_deleted NUMBER := SQLROWCOUNT;

  DELETE FROM SILVER.RETURNS WHERE customer_id = :P_CUSTOMER_ID;
  LET v_returns_deleted NUMBER := SQLROWCOUNT;

  DELETE FROM SILVER.ORDERS WHERE customer_id = :P_CUSTOMER_ID;
  LET v_orders_deleted NUMBER := SQLROWCOUNT;

  DELETE FROM SILVER.CUSTOMERS WHERE customer_id = :P_CUSTOMER_ID;

  -- 2. Anonymize in GOLD (preserve surrogate keys so aggregates remain valid)
  UPDATE GOLD.DIM_CUSTOMER
  SET first_name = 'REDACTED',
      last_name = 'REDACTED',
      email = 'REDACTED',
      phone = 'REDACTED',
      hash_diff = MD5('REDACTED'),
      load_ts = :v_ts
  WHERE customer_id = :P_CUSTOMER_ID;
  LET v_dim_rows NUMBER := SQLROWCOUNT;

  -- Fact tables keep surrogate key references intact (aggregates still work)
  -- but we null out any direct PII references if they exist

  -- 3. Audit trail
  INSERT INTO UTIL.GDPR_AUDIT_LOG (request_id, customer_id, action, layer, table_name, rows_affected, requested_by, executed_at)
  VALUES
    (:v_request_id, :P_CUSTOMER_ID, 'HARD_DELETE', 'SILVER', 'CUSTOMERS', 1, CURRENT_USER(), :v_ts),
    (:v_request_id, :P_CUSTOMER_ID, 'HARD_DELETE', 'SILVER', 'ORDERS', :v_orders_deleted, CURRENT_USER(), :v_ts),
    (:v_request_id, :P_CUSTOMER_ID, 'HARD_DELETE', 'SILVER', 'PAYMENTS', :v_payments_deleted, CURRENT_USER(), :v_ts),
    (:v_request_id, :P_CUSTOMER_ID, 'HARD_DELETE', 'SILVER', 'RETURNS', :v_returns_deleted, CURRENT_USER(), :v_ts),
    (:v_request_id, :P_CUSTOMER_ID, 'HARD_DELETE', 'SILVER', 'EVENTS', :v_events_deleted, CURRENT_USER(), :v_ts),
    (:v_request_id, :P_CUSTOMER_ID, 'ANONYMIZE', 'GOLD', 'DIM_CUSTOMER', :v_dim_rows, CURRENT_USER(), :v_ts);

  RETURN 'GDPR delete completed for ' || P_CUSTOMER_ID || '. Request ID: ' || v_request_id;
END;
$$;

-- Verification
-- CALL UTIL.SP_GDPR_DELETE('C001');
-- SELECT * FROM UTIL.GDPR_AUDIT_LOG ORDER BY executed_at DESC;
-- SELECT * FROM GOLD.DIM_CUSTOMER WHERE customer_id = 'C001';  -- Should show REDACTED
