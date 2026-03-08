-- 31_build_gold.sql
-- Stored procedure to build gold dimensions, facts, and aggregates.

USE ROLE DE_TRANSFORM_ROLE;
USE WAREHOUSE TRANSFORM_WH;
USE DATABASE ECOMMERCE_DB;

CREATE OR REPLACE PROCEDURE UTIL.SP_BUILD_GOLD()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
  v_run_ts TIMESTAMP_NTZ := CURRENT_TIMESTAMP();
BEGIN
  -- Type 1 product dimension
  MERGE INTO GOLD.DIM_PRODUCT AS tgt
  USING (
    SELECT
      product_id,
      sku,
      name,
      category,
      price,
      currency,
      record_source,
      load_ts
    FROM SILVER.PRODUCTS
  ) AS src
  ON tgt.product_id = src.product_id
  WHEN MATCHED THEN
    UPDATE SET
      sku = src.sku,
      name = src.name,
      category = src.category,
      price = src.price,
      currency = src.currency,
      record_source = src.record_source,
      load_ts = v_run_ts
  WHEN NOT MATCHED THEN
    INSERT (product_id, sku, name, category, price, currency, record_source, load_ts)
    VALUES (src.product_id, src.sku, src.name, src.category, src.price, src.currency, src.record_source, v_run_ts);

  -- SCD2 customer dimension
  CREATE OR REPLACE TEMP TABLE CUSTOMER_STAGE AS
  SELECT
    customer_id,
    first_name,
    last_name,
    email,
    phone,
    region,
    COALESCE(updated_at, load_ts) AS effective_ts,
    record_source,
    MD5(CONCAT_WS('|', COALESCE(first_name, ''), COALESCE(last_name, ''), COALESCE(email, ''), COALESCE(phone, ''), COALESCE(region, ''))) AS hash_diff
  FROM SILVER.CUSTOMERS;

  -- Close out changed records
  UPDATE GOLD.DIM_CUSTOMER AS tgt
  SET valid_to = stg.effective_ts,
      is_current = FALSE,
      load_ts = v_run_ts
  FROM CUSTOMER_STAGE AS stg
  WHERE tgt.customer_id = stg.customer_id
    AND tgt.is_current = TRUE
    AND tgt.hash_diff <> stg.hash_diff;

  -- Insert new/current records
  INSERT INTO GOLD.DIM_CUSTOMER
    (customer_id, first_name, last_name, email, phone, region, valid_from, valid_to, is_current, hash_diff, record_source, load_ts)
  SELECT
    stg.customer_id,
    stg.first_name,
    stg.last_name,
    stg.email,
    stg.phone,
    stg.region,
    stg.effective_ts AS valid_from,
    TO_TIMESTAMP_NTZ('9999-12-31') AS valid_to,
    TRUE AS is_current,
    stg.hash_diff,
    stg.record_source,
    v_run_ts
  FROM CUSTOMER_STAGE AS stg
  LEFT JOIN GOLD.DIM_CUSTOMER AS tgt
    ON stg.customer_id = tgt.customer_id
   AND tgt.is_current = TRUE
  WHERE tgt.customer_id IS NULL
     OR tgt.hash_diff <> stg.hash_diff;

  -- Orders fact
  MERGE INTO GOLD.FACT_ORDERS AS tgt
  USING (
    SELECT
      o.order_id,
      o.order_ts,
      CAST(o.order_ts AS DATE) AS order_date,
      d.customer_sk,
      o.status AS order_status,
      o.subtotal,
      o.tax,
      o.shipping,
      o.total,
      o.payment_method,
      v_run_ts AS load_ts
    FROM SILVER.ORDERS AS o
    LEFT JOIN GOLD.DIM_CUSTOMER AS d
      ON o.customer_id = d.customer_id AND d.is_current = TRUE
  ) AS src
  ON tgt.order_id = src.order_id
  WHEN MATCHED THEN
    UPDATE SET
      order_ts = src.order_ts,
      order_date = src.order_date,
      customer_sk = src.customer_sk,
      order_status = src.order_status,
      subtotal = src.subtotal,
      tax = src.tax,
      shipping = src.shipping,
      total = src.total,
      payment_method = src.payment_method,
      load_ts = src.load_ts
  WHEN NOT MATCHED THEN
    INSERT (order_id, order_ts, order_date, customer_sk, order_status, subtotal, tax, shipping, total, payment_method, load_ts)
    VALUES (src.order_id, src.order_ts, src.order_date, src.customer_sk, src.order_status, src.subtotal, src.tax, src.shipping, src.total, src.payment_method, src.load_ts);

  -- Payments fact
  MERGE INTO GOLD.FACT_PAYMENTS AS tgt
  USING (
    SELECT
      p.payment_id,
      p.order_id,
      p.payment_ts,
      d.customer_sk,
      p.amount,
      p.status,
      p.provider,
      v_run_ts AS load_ts
    FROM SILVER.PAYMENTS AS p
    LEFT JOIN SILVER.ORDERS AS o
      ON p.order_id = o.order_id
    LEFT JOIN GOLD.DIM_CUSTOMER AS d
      ON o.customer_id = d.customer_id AND d.is_current = TRUE
  ) AS src
  ON tgt.payment_id = src.payment_id
  WHEN MATCHED THEN
    UPDATE SET
      order_id = src.order_id,
      payment_ts = src.payment_ts,
      customer_sk = src.customer_sk,
      amount = src.amount,
      status = src.status,
      provider = src.provider,
      load_ts = src.load_ts
  WHEN NOT MATCHED THEN
    INSERT (payment_id, order_id, payment_ts, customer_sk, amount, status, provider, load_ts)
    VALUES (src.payment_id, src.order_id, src.payment_ts, src.customer_sk, src.amount, src.status, src.provider, src.load_ts);

  -- Events fact
  MERGE INTO GOLD.FACT_EVENTS AS tgt
  USING (
    SELECT
      e.event_id,
      e.event_ts,
      CAST(e.event_ts AS DATE) AS event_date,
      d.customer_sk,
      e.session_id,
      e.event_name,
      e.device_type,
      e.payload:product_id::STRING AS product_id,
      v_run_ts AS load_ts
    FROM SILVER.EVENTS AS e
    LEFT JOIN GOLD.DIM_CUSTOMER AS d
      ON e.user_id = d.customer_id AND d.is_current = TRUE
  ) AS src
  ON tgt.event_id = src.event_id
  WHEN MATCHED THEN
    UPDATE SET
      event_ts = src.event_ts,
      event_date = src.event_date,
      customer_sk = src.customer_sk,
      session_id = src.session_id,
      event_name = src.event_name,
      device_type = src.device_type,
      product_id = src.product_id,
      load_ts = src.load_ts
  WHEN NOT MATCHED THEN
    INSERT (event_id, event_ts, event_date, customer_sk, session_id, event_name, device_type, product_id, load_ts)
    VALUES (src.event_id, src.event_ts, src.event_date, src.customer_sk, src.session_id, src.event_name, src.device_type, src.product_id, src.load_ts);

  -- Returns fact
  MERGE INTO GOLD.FACT_RETURNS AS tgt
  USING (
    SELECT
      r.return_id,
      r.order_id,
      d.customer_sk,
      r.return_ts,
      CAST(r.return_ts AS DATE) AS return_date,
      r.reason,
      r.refund_amount,
      r.status,
      v_run_ts AS load_ts
    FROM SILVER.RETURNS AS r
    LEFT JOIN GOLD.DIM_CUSTOMER AS d
      ON r.customer_id = d.customer_id AND d.is_current = TRUE
  ) AS src
  ON tgt.return_id = src.return_id
  WHEN MATCHED THEN
    UPDATE SET
      order_id = src.order_id,
      customer_sk = src.customer_sk,
      return_ts = src.return_ts,
      return_date = src.return_date,
      reason = src.reason,
      refund_amount = src.refund_amount,
      status = src.status,
      load_ts = src.load_ts
  WHEN NOT MATCHED THEN
    INSERT (return_id, order_id, customer_sk, return_ts, return_date, reason, refund_amount, status, load_ts)
    VALUES (src.return_id, src.order_id, src.customer_sk, src.return_ts, src.return_date, src.reason, src.refund_amount, src.status, src.load_ts);

  -- Sessionization: build FACT_SESSIONS from FACT_EVENTS
  MERGE INTO GOLD.FACT_SESSIONS AS tgt
  USING (
    SELECT
      session_id,
      customer_sk,
      device_type,
      MIN(event_ts) AS session_start,
      MAX(event_ts) AS session_end,
      DATEDIFF('second', MIN(event_ts), MAX(event_ts)) AS session_duration_sec,
      COUNT_IF(event_name = 'page_view') AS page_views,
      COUNT_IF(event_name = 'product_view') AS product_views,
      COUNT_IF(event_name = 'add_to_cart') AS add_to_carts,
      COUNT_IF(event_name = 'checkout_start') AS checkouts_started,
      COUNT_IF(event_name = 'purchase') AS purchases,
      COUNT(*) = 1 AS is_bounce,
      COUNT_IF(event_name = 'purchase') > 0 AS is_conversion,
      ARRAY_AGG(DISTINCT product_id) WITHIN GROUP (ORDER BY product_id) AS products_viewed,
      v_run_ts AS load_ts
    FROM GOLD.FACT_EVENTS
    WHERE session_id IS NOT NULL
    GROUP BY session_id, customer_sk, device_type
  ) AS src
  ON tgt.session_id = src.session_id
  WHEN MATCHED THEN
    UPDATE SET
      customer_sk = src.customer_sk,
      device_type = src.device_type,
      session_start = src.session_start,
      session_end = src.session_end,
      session_duration_sec = src.session_duration_sec,
      page_views = src.page_views,
      product_views = src.product_views,
      add_to_carts = src.add_to_carts,
      checkouts_started = src.checkouts_started,
      purchases = src.purchases,
      is_bounce = src.is_bounce,
      is_conversion = src.is_conversion,
      products_viewed = src.products_viewed,
      load_ts = src.load_ts
  WHEN NOT MATCHED THEN
    INSERT (session_id, customer_sk, device_type, session_start, session_end, session_duration_sec,
            page_views, product_views, add_to_carts, checkouts_started, purchases,
            is_bounce, is_conversion, products_viewed, load_ts)
    VALUES (src.session_id, src.customer_sk, src.device_type, src.session_start, src.session_end, src.session_duration_sec,
            src.page_views, src.product_views, src.add_to_carts, src.checkouts_started, src.purchases,
            src.is_bounce, src.is_conversion, src.products_viewed, src.load_ts);

  -- Rolling 30-day aggregate with gross/net revenue (incremental by date partition)
  MERGE INTO GOLD.AGG_DAILY_SALES AS tgt
  USING (
    SELECT
      o.order_date,
      COUNT(DISTINCT o.order_id) AS total_orders,
      SUM(o.total) AS gross_revenue,
      COALESCE(r.daily_refunds, 0) AS total_refunds,
      SUM(o.total) - COALESCE(r.daily_refunds, 0) AS net_revenue,
      COALESCE(r.daily_returns, 0) AS total_returns,
      AVG(o.total) AS avg_order_value,
      v_run_ts AS load_ts
    FROM GOLD.FACT_ORDERS o
    LEFT JOIN (
      SELECT
        return_date AS order_date,
        SUM(refund_amount) AS daily_refunds,
        COUNT(*) AS daily_returns
      FROM GOLD.FACT_RETURNS
      WHERE status = 'APPROVED'
      GROUP BY return_date
    ) r ON o.order_date = r.order_date
    WHERE o.order_date >= DATEADD('day', -30, CURRENT_DATE())
    GROUP BY o.order_date, r.daily_refunds, r.daily_returns
  ) AS src
  ON tgt.order_date = src.order_date
  WHEN MATCHED THEN
    UPDATE SET
      total_orders = src.total_orders,
      gross_revenue = src.gross_revenue,
      total_refunds = src.total_refunds,
      net_revenue = src.net_revenue,
      total_returns = src.total_returns,
      avg_order_value = src.avg_order_value,
      load_ts = src.load_ts
  WHEN NOT MATCHED THEN
    INSERT (order_date, total_orders, gross_revenue, total_refunds, net_revenue, total_returns, avg_order_value, load_ts)
    VALUES (src.order_date, src.total_orders, src.gross_revenue, src.total_refunds, src.net_revenue, src.total_returns, src.avg_order_value, src.load_ts);

  RETURN 'SP_BUILD_GOLD completed';
END;
$$;

-- Optional manual run
-- CALL UTIL.SP_BUILD_GOLD();
