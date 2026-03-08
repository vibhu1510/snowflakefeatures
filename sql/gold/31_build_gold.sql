-- 31_build_gold.sql
-- Stream-driven gold build procedures: incremental processing of silver changes.
-- Split into batch (dimensions + transactional facts) and events (clickstream + sessions).
-- Uses SCD2 temporal joins for point-in-time accurate dimension lookups.

USE ROLE DE_TRANSFORM_ROLE;
USE WAREHOUSE TRANSFORM_WH;
USE DATABASE ECOMMERCE_DB;

-- ============================================================
-- SP_BUILD_GOLD_BATCH: Dimensions, transactional facts, daily aggregates
-- Called by TASK_BUILD_GOLD_BATCH (hourly, after batch silver merge)
-- ============================================================
CREATE OR REPLACE PROCEDURE UTIL.SP_BUILD_GOLD_BATCH()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
  v_run_ts TIMESTAMP_NTZ := CURRENT_TIMESTAMP();
  v_status STRING := '';
  v_orders_updated BOOLEAN := FALSE;
BEGIN

  -- ==========================================
  -- DIM_PRODUCT (Type 1 — only if products changed)
  -- ==========================================
  IF (SYSTEM$STREAM_HAS_DATA('SILVER.PRODUCTS_STREAM')) THEN
    MERGE INTO GOLD.DIM_PRODUCT AS tgt
    USING (
      SELECT
        product_id, sku, name, category, price, currency, record_source, load_ts
      FROM SILVER.PRODUCTS_STREAM
      WHERE METADATA$ACTION IN ('INSERT','UPDATE')
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
        load_ts = :v_run_ts
    WHEN NOT MATCHED THEN
      INSERT (product_id, sku, name, category, price, currency, record_source, load_ts)
      VALUES (src.product_id, src.sku, src.name, src.category, src.price, src.currency, src.record_source, :v_run_ts);

    v_status := v_status || 'DIM_PRODUCT updated. ';
  END IF;

  -- ==========================================
  -- DIM_CUSTOMER (SCD Type 2 — only if customers changed)
  -- ==========================================
  IF (SYSTEM$STREAM_HAS_DATA('SILVER.CUSTOMERS_STREAM')) THEN
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
      SHA2(CONCAT_WS('|', COALESCE(first_name, ''), COALESCE(last_name, ''), COALESCE(email, ''), COALESCE(phone, ''), COALESCE(region, '')), 256) AS hash_diff
    FROM SILVER.CUSTOMERS_STREAM
    WHERE METADATA$ACTION IN ('INSERT','UPDATE');

    -- Close out changed records
    UPDATE GOLD.DIM_CUSTOMER AS tgt
    SET valid_to = stg.effective_ts,
        is_current = FALSE,
        load_ts = :v_run_ts
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
      :v_run_ts
    FROM CUSTOMER_STAGE AS stg
    LEFT JOIN GOLD.DIM_CUSTOMER AS tgt
      ON stg.customer_id = tgt.customer_id
     AND tgt.is_current = TRUE
    WHERE tgt.customer_id IS NULL
       OR tgt.hash_diff <> stg.hash_diff;

    v_status := v_status || 'DIM_CUSTOMER updated. ';
  END IF;

  -- ==========================================
  -- FACT_ORDERS (SCD2 temporal join — only if orders changed)
  -- ==========================================
  IF (SYSTEM$STREAM_HAS_DATA('SILVER.ORDERS_STREAM')) THEN
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
        :v_run_ts AS load_ts
      FROM SILVER.ORDERS_STREAM AS o
      LEFT JOIN GOLD.DIM_CUSTOMER AS d
        ON o.customer_id = d.customer_id
        AND o.order_ts >= d.valid_from
        AND o.order_ts < d.valid_to
      WHERE o.METADATA$ACTION IN ('INSERT','UPDATE')
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

    v_orders_updated := TRUE;
    v_status := v_status || 'FACT_ORDERS updated. ';
  END IF;

  -- ==========================================
  -- FACT_PAYMENTS (SCD2 temporal join — only if payments changed)
  -- ==========================================
  IF (SYSTEM$STREAM_HAS_DATA('SILVER.PAYMENTS_STREAM')) THEN
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
        :v_run_ts AS load_ts
      FROM SILVER.PAYMENTS_STREAM AS p
      LEFT JOIN SILVER.ORDERS AS o
        ON p.order_id = o.order_id
      LEFT JOIN GOLD.DIM_CUSTOMER AS d
        ON o.customer_id = d.customer_id
        AND p.payment_ts >= d.valid_from
        AND p.payment_ts < d.valid_to
      WHERE p.METADATA$ACTION IN ('INSERT','UPDATE')
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

    v_orders_updated := TRUE;
    v_status := v_status || 'FACT_PAYMENTS updated. ';
  END IF;

  -- ==========================================
  -- FACT_RETURNS (SCD2 temporal join — only if returns changed)
  -- ==========================================
  IF (SYSTEM$STREAM_HAS_DATA('SILVER.RETURNS_STREAM')) THEN
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
        :v_run_ts AS load_ts
      FROM SILVER.RETURNS_STREAM AS r
      LEFT JOIN GOLD.DIM_CUSTOMER AS d
        ON r.customer_id = d.customer_id
        AND r.return_ts >= d.valid_from
        AND r.return_ts < d.valid_to
      WHERE r.METADATA$ACTION IN ('INSERT','UPDATE')
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

    v_orders_updated := TRUE;
    v_status := v_status || 'FACT_RETURNS updated. ';
  END IF;

  -- ==========================================
  -- AGG_DAILY_SALES — cash-basis (refunds by return date)
  -- Only rebuild if orders, payments, or returns changed.
  -- ==========================================
  IF (v_orders_updated) THEN
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
        :v_run_ts AS load_ts
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

    -- ==========================================
    -- AGG_DAILY_SALES_ACCRUAL — accrual-basis (refunds by original order date)
    -- ==========================================
    MERGE INTO GOLD.AGG_DAILY_SALES_ACCRUAL AS tgt
    USING (
      SELECT
        o.order_date,
        COUNT(DISTINCT o.order_id) AS total_orders,
        SUM(o.total) AS gross_revenue,
        COALESCE(r.accrual_refunds, 0) AS total_refunds,
        SUM(o.total) - COALESCE(r.accrual_refunds, 0) AS net_revenue,
        COALESCE(r.accrual_returns, 0) AS total_returns,
        AVG(o.total) AS avg_order_value,
        :v_run_ts AS load_ts
      FROM GOLD.FACT_ORDERS o
      LEFT JOIN (
        SELECT
          fo.order_date,
          SUM(fr.refund_amount) AS accrual_refunds,
          COUNT(*) AS accrual_returns
        FROM GOLD.FACT_RETURNS fr
        JOIN GOLD.FACT_ORDERS fo ON fr.order_id = fo.order_id
        WHERE fr.status = 'APPROVED'
        GROUP BY fo.order_date
      ) r ON o.order_date = r.order_date
      WHERE o.order_date >= DATEADD('day', -30, CURRENT_DATE())
      GROUP BY o.order_date, r.accrual_refunds, r.accrual_returns
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

    v_status := v_status || 'AGG_DAILY_SALES (cash + accrual) updated. ';
  END IF;

  IF (v_status = '') THEN
    v_status := 'No streams had data. No-op.';
  END IF;

  RETURN 'SP_BUILD_GOLD_BATCH completed: ' || v_status;
END;
$$;

-- ============================================================
-- SP_BUILD_GOLD_EVENTS: Clickstream facts and sessions
-- Called by TASK_BUILD_GOLD_EVENTS (every 5 min, after event silver merge)
-- ============================================================
CREATE OR REPLACE PROCEDURE UTIL.SP_BUILD_GOLD_EVENTS()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
  v_run_ts TIMESTAMP_NTZ := CURRENT_TIMESTAMP();
  v_status STRING := '';
  v_events_updated BOOLEAN := FALSE;
BEGIN

  -- ==========================================
  -- FACT_EVENTS (SCD2 temporal join — only if events changed)
  -- ==========================================
  IF (SYSTEM$STREAM_HAS_DATA('SILVER.EVENTS_STREAM')) THEN
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
        :v_run_ts AS load_ts
      FROM SILVER.EVENTS_STREAM AS e
      LEFT JOIN GOLD.DIM_CUSTOMER AS d
        ON e.user_id = d.customer_id
        AND e.event_ts >= d.valid_from
        AND e.event_ts < d.valid_to
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

    v_events_updated := TRUE;
    v_status := v_status || 'FACT_EVENTS updated. ';
  END IF;

  -- ==========================================
  -- FACT_SESSIONS (rebuilt from FACT_EVENTS when events change)
  -- Uses computed sessionization: 30-min inactivity timeout for events
  -- without a session_id, passthrough for events with one.
  -- ==========================================
  IF (v_events_updated) THEN
    MERGE INTO GOLD.FACT_SESSIONS AS tgt
    USING (
      SELECT
        effective_session_id AS session_id,
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
        MAX(CASE WHEN effective_session_id LIKE 'COMPUTED_%' THEN TRUE ELSE FALSE END) AS is_computed_session,
        ARRAY_AGG(DISTINCT product_id) WITHIN GROUP (ORDER BY product_id) AS products_viewed,
        :v_run_ts AS load_ts
      FROM GOLD.VW_EVENTS_WITH_COMPUTED_SESSION
      WHERE effective_session_id IS NOT NULL
      GROUP BY effective_session_id, customer_sk, device_type
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
        is_computed_session = src.is_computed_session,
        products_viewed = src.products_viewed,
        load_ts = src.load_ts
    WHEN NOT MATCHED THEN
      INSERT (session_id, customer_sk, device_type, session_start, session_end, session_duration_sec,
              page_views, product_views, add_to_carts, checkouts_started, purchases,
              is_bounce, is_conversion, is_computed_session, products_viewed, load_ts)
      VALUES (src.session_id, src.customer_sk, src.device_type, src.session_start, src.session_end, src.session_duration_sec,
              src.page_views, src.product_views, src.add_to_carts, src.checkouts_started, src.purchases,
              src.is_bounce, src.is_conversion, src.is_computed_session, src.products_viewed, src.load_ts);

    v_status := v_status || 'FACT_SESSIONS updated. ';
  END IF;

  IF (v_status = '') THEN
    v_status := 'No streams had data. No-op.';
  END IF;

  RETURN 'SP_BUILD_GOLD_EVENTS completed: ' || v_status;
END;
$$;

-- ============================================================
-- SP_BUILD_GOLD: Convenience wrapper that calls both (for manual runs)
-- ============================================================
CREATE OR REPLACE PROCEDURE UTIL.SP_BUILD_GOLD()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
  CALL UTIL.SP_BUILD_GOLD_BATCH();
  CALL UTIL.SP_BUILD_GOLD_EVENTS();
  RETURN 'SP_BUILD_GOLD completed (batch + events)';
END;
$$;

-- Optional manual run
-- CALL UTIL.SP_BUILD_GOLD();
-- CALL UTIL.SP_BUILD_GOLD_BATCH();
-- CALL UTIL.SP_BUILD_GOLD_EVENTS();

-- ONE-TIME MIGRATION: Backfill existing DIM_CUSTOMER hash_diff from MD5 to SHA2.
-- Run once after deploying the SHA2 change to avoid a false SCD2 close-and-reopen of all records:
--
-- UPDATE GOLD.DIM_CUSTOMER SET hash_diff = SHA2(CONCAT_WS('|',
--   COALESCE(first_name,''), COALESCE(last_name,''), COALESCE(email,''),
--   COALESCE(phone,''), COALESCE(region,'')), 256)
-- WHERE first_name <> 'REDACTED';
