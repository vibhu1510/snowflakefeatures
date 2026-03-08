-- 21_merge_clean.sql
-- Stored procedures to incrementally merge bronze streams into silver tables.
-- Split into batch (dimensions + transactional) and events (clickstream) for
-- independent scheduling. Both wrapped in transactions with error handling.

USE ROLE DE_TRANSFORM_ROLE;
USE WAREHOUSE TRANSFORM_WH;
USE DATABASE ECOMMERCE_DB;

-- ============================================================
-- SP_MERGE_SILVER_BATCH: Customers, Products, Orders, Payments, Returns
-- Called by TASK_MERGE_SILVER_BATCH (hourly, after batch ingestion)
-- ============================================================
CREATE OR REPLACE PROCEDURE UTIL.SP_MERGE_SILVER_BATCH()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
  v_error_msg STRING;
BEGIN
  BEGIN TRANSACTION;

  -- Customers (filtered against GDPR suppression list)
  MERGE INTO SILVER.CUSTOMERS AS tgt
  USING (
    SELECT *
    FROM (
      SELECT
        customer_id,
        first_name,
        last_name,
        email,
        phone,
        region,
        created_at,
        updated_at,
        source_file AS record_source,
        ingestion_ts AS load_ts,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY updated_at DESC, ingestion_ts DESC) AS rn
      FROM BRONZE.CUSTOMERS_RAW_STREAM
      WHERE METADATA$ACTION IN ('INSERT','UPDATE')
        AND customer_id IS NOT NULL
        AND customer_id NOT IN (SELECT customer_id FROM UTIL.GDPR_SUPPRESSION_LIST)
    )
    WHERE rn = 1
  ) AS src
  ON tgt.customer_id = src.customer_id
  WHEN MATCHED AND NVL(src.updated_at, src.load_ts) >= NVL(tgt.updated_at, tgt.load_ts) THEN
    UPDATE SET
      first_name = src.first_name,
      last_name = src.last_name,
      email = src.email,
      phone = src.phone,
      region = src.region,
      created_at = src.created_at,
      updated_at = src.updated_at,
      record_source = src.record_source,
      load_ts = src.load_ts
  WHEN NOT MATCHED THEN
    INSERT (customer_id, first_name, last_name, email, phone, region, created_at, updated_at, record_source, load_ts)
    VALUES (src.customer_id, src.first_name, src.last_name, src.email, src.phone, src.region, src.created_at, src.updated_at, src.record_source, src.load_ts);

  -- Products
  MERGE INTO SILVER.PRODUCTS AS tgt
  USING (
    SELECT *
    FROM (
      SELECT
        product_id,
        sku,
        name,
        category,
        price,
        currency,
        created_at,
        updated_at,
        source_file AS record_source,
        ingestion_ts AS load_ts,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY updated_at DESC, ingestion_ts DESC) AS rn
      FROM BRONZE.PRODUCTS_RAW_STREAM
      WHERE METADATA$ACTION IN ('INSERT','UPDATE')
        AND product_id IS NOT NULL
    )
    WHERE rn = 1
  ) AS src
  ON tgt.product_id = src.product_id
  WHEN MATCHED AND NVL(src.updated_at, src.load_ts) >= NVL(tgt.updated_at, tgt.load_ts) THEN
    UPDATE SET
      sku = src.sku,
      name = src.name,
      category = src.category,
      price = src.price,
      currency = src.currency,
      created_at = src.created_at,
      updated_at = src.updated_at,
      record_source = src.record_source,
      load_ts = src.load_ts
  WHEN NOT MATCHED THEN
    INSERT (product_id, sku, name, category, price, currency, created_at, updated_at, record_source, load_ts)
    VALUES (src.product_id, src.sku, src.name, src.category, src.price, src.currency, src.created_at, src.updated_at, src.record_source, src.load_ts);

  -- Orders (filtered against GDPR suppression list)
  MERGE INTO SILVER.ORDERS AS tgt
  USING (
    SELECT *
    FROM (
      SELECT
        order_id,
        customer_id,
        order_ts,
        status,
        subtotal,
        tax,
        shipping,
        total,
        payment_method,
        updated_at,
        source_file AS record_source,
        ingestion_ts AS load_ts,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY updated_at DESC, ingestion_ts DESC) AS rn
      FROM BRONZE.ORDERS_RAW_STREAM
      WHERE METADATA$ACTION IN ('INSERT','UPDATE')
        AND order_id IS NOT NULL
        AND customer_id NOT IN (SELECT customer_id FROM UTIL.GDPR_SUPPRESSION_LIST)
    )
    WHERE rn = 1
  ) AS src
  ON tgt.order_id = src.order_id
  WHEN MATCHED AND NVL(src.updated_at, src.load_ts) >= NVL(tgt.updated_at, tgt.load_ts) THEN
    UPDATE SET
      customer_id = src.customer_id,
      order_ts = src.order_ts,
      status = src.status,
      subtotal = src.subtotal,
      tax = src.tax,
      shipping = src.shipping,
      total = src.total,
      payment_method = src.payment_method,
      updated_at = src.updated_at,
      record_source = src.record_source,
      load_ts = src.load_ts
  WHEN NOT MATCHED THEN
    INSERT (order_id, customer_id, order_ts, status, subtotal, tax, shipping, total, payment_method, updated_at, record_source, load_ts)
    VALUES (src.order_id, src.customer_id, src.order_ts, src.status, src.subtotal, src.tax, src.shipping, src.total, src.payment_method, src.updated_at, src.record_source, src.load_ts);

  -- Payments (filtered: exclude payments for suppressed customers' orders)
  MERGE INTO SILVER.PAYMENTS AS tgt
  USING (
    SELECT *
    FROM (
      SELECT
        payment_id,
        order_id,
        payment_ts,
        amount,
        status,
        provider,
        updated_at,
        source_file AS record_source,
        ingestion_ts AS load_ts,
        ROW_NUMBER() OVER (PARTITION BY payment_id ORDER BY updated_at DESC, ingestion_ts DESC) AS rn
      FROM BRONZE.PAYMENTS_RAW_STREAM
      WHERE METADATA$ACTION IN ('INSERT','UPDATE')
        AND payment_id IS NOT NULL
        AND order_id NOT IN (
          SELECT order_id FROM BRONZE.ORDERS_RAW
          WHERE customer_id IN (SELECT customer_id FROM UTIL.GDPR_SUPPRESSION_LIST)
        )
    )
    WHERE rn = 1
  ) AS src
  ON tgt.payment_id = src.payment_id
  WHEN MATCHED AND NVL(src.updated_at, src.load_ts) >= NVL(tgt.updated_at, tgt.load_ts) THEN
    UPDATE SET
      order_id = src.order_id,
      payment_ts = src.payment_ts,
      amount = src.amount,
      status = src.status,
      provider = src.provider,
      updated_at = src.updated_at,
      record_source = src.record_source,
      load_ts = src.load_ts
  WHEN NOT MATCHED THEN
    INSERT (payment_id, order_id, payment_ts, amount, status, provider, updated_at, record_source, load_ts)
    VALUES (src.payment_id, src.order_id, src.payment_ts, src.amount, src.status, src.provider, src.updated_at, src.record_source, src.load_ts);

  -- Returns (filtered against GDPR suppression list)
  MERGE INTO SILVER.RETURNS AS tgt
  USING (
    SELECT *
    FROM (
      SELECT
        return_id,
        order_id,
        customer_id,
        return_ts,
        reason,
        refund_amount,
        status,
        updated_at,
        source_file AS record_source,
        ingestion_ts AS load_ts,
        ROW_NUMBER() OVER (PARTITION BY return_id ORDER BY updated_at DESC, ingestion_ts DESC) AS rn
      FROM BRONZE.RETURNS_RAW_STREAM
      WHERE METADATA$ACTION IN ('INSERT','UPDATE')
        AND return_id IS NOT NULL
        AND customer_id NOT IN (SELECT customer_id FROM UTIL.GDPR_SUPPRESSION_LIST)
    )
    WHERE rn = 1
  ) AS src
  ON tgt.return_id = src.return_id
  WHEN MATCHED AND NVL(src.updated_at, src.load_ts) >= NVL(tgt.updated_at, tgt.load_ts) THEN
    UPDATE SET
      order_id = src.order_id,
      customer_id = src.customer_id,
      return_ts = src.return_ts,
      reason = src.reason,
      refund_amount = src.refund_amount,
      status = src.status,
      updated_at = src.updated_at,
      record_source = src.record_source,
      load_ts = src.load_ts
  WHEN NOT MATCHED THEN
    INSERT (return_id, order_id, customer_id, return_ts, reason, refund_amount, status, updated_at, record_source, load_ts)
    VALUES (src.return_id, src.order_id, src.customer_id, src.return_ts, src.reason, src.refund_amount, src.status, src.updated_at, src.record_source, src.load_ts);

  COMMIT;
  RETURN 'SP_MERGE_SILVER_BATCH completed successfully';

EXCEPTION
  WHEN OTHER THEN
    ROLLBACK;
    v_error_msg := SQLERRM;
    -- Log failure for DQ alerting visibility
    INSERT INTO UTIL.DQ_RESULTS (check_name, status, observed_value, expected_value, details, check_ts)
    VALUES ('merge_silver_batch_execution', 'FAIL', :v_error_msg, 'No errors',
            'SP_MERGE_SILVER_BATCH failed and rolled back', CURRENT_TIMESTAMP());
    RAISE;
END;
$$;

-- ============================================================
-- SP_MERGE_SILVER_EVENTS: Clickstream events (append-only)
-- Called by TASK_MERGE_SILVER_EVENTS (every 5 min, after event ingestion)
-- ============================================================
CREATE OR REPLACE PROCEDURE UTIL.SP_MERGE_SILVER_EVENTS()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
  v_error_msg STRING;
BEGIN
  BEGIN TRANSACTION;

  -- Events: INSERT with cross-batch dedup guard (NOT IN existing event_ids)
  -- and GDPR suppression filtering.
  INSERT INTO SILVER.EVENTS
    (event_id, event_ts, user_id, session_id, event_name, device_type, payload, record_source, load_ts)
  SELECT
    event_id,
    event_ts,
    user_id,
    session_id,
    event_name,
    device_type,
    payload,
    source_file AS record_source,
    ingestion_ts AS load_ts
  FROM (
    SELECT
      event_id,
      event_ts,
      user_id,
      session_id,
      event_name,
      device_type,
      payload,
      source_file,
      ingestion_ts,
      ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY event_ts DESC, ingestion_ts DESC) AS rn
    FROM BRONZE.EVENTS_RAW_STREAM
    WHERE METADATA$ACTION = 'INSERT'
      AND event_id IS NOT NULL
      AND user_id NOT IN (SELECT customer_id FROM UTIL.GDPR_SUPPRESSION_LIST)
  )
  WHERE rn = 1
    AND event_id NOT IN (SELECT event_id FROM SILVER.EVENTS);

  COMMIT;
  RETURN 'SP_MERGE_SILVER_EVENTS completed successfully';

EXCEPTION
  WHEN OTHER THEN
    ROLLBACK;
    v_error_msg := SQLERRM;
    INSERT INTO UTIL.DQ_RESULTS (check_name, status, observed_value, expected_value, details, check_ts)
    VALUES ('merge_silver_events_execution', 'FAIL', :v_error_msg, 'No errors',
            'SP_MERGE_SILVER_EVENTS failed and rolled back', CURRENT_TIMESTAMP());
    RAISE;
END;
$$;

-- ============================================================
-- SP_MERGE_SILVER: Convenience wrapper that calls both (for manual runs)
-- ============================================================
CREATE OR REPLACE PROCEDURE UTIL.SP_MERGE_SILVER()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
  CALL UTIL.SP_MERGE_SILVER_BATCH();
  CALL UTIL.SP_MERGE_SILVER_EVENTS();
  RETURN 'SP_MERGE_SILVER completed (batch + events)';
END;
$$;

-- Optional manual run
-- CALL UTIL.SP_MERGE_SILVER();
-- CALL UTIL.SP_MERGE_SILVER_BATCH();
-- CALL UTIL.SP_MERGE_SILVER_EVENTS();
