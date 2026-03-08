-- 33_sessionization.sql
-- Clickstream sessionization and conversion funnel analysis.

USE ROLE DE_TRANSFORM_ROLE;
USE WAREHOUSE TRANSFORM_WH;
USE DATABASE ECOMMERCE_DB;
USE SCHEMA GOLD;

-- Session-level fact table
CREATE OR REPLACE TABLE FACT_SESSIONS (
  session_id STRING,
  customer_sk NUMBER,
  device_type STRING,
  session_start TIMESTAMP_NTZ,
  session_end TIMESTAMP_NTZ,
  session_duration_sec NUMBER,
  page_views NUMBER,
  product_views NUMBER,
  add_to_carts NUMBER,
  checkouts_started NUMBER,
  purchases NUMBER,
  is_bounce BOOLEAN,
  is_conversion BOOLEAN,
  products_viewed ARRAY,
  load_ts TIMESTAMP_NTZ
) CLUSTER BY (session_start::DATE);

-- Conversion funnel view (step-by-step drop-off)
CREATE OR REPLACE VIEW VW_CONVERSION_FUNNEL AS
WITH session_steps AS (
  SELECT
    session_id,
    customer_sk,
    device_type,
    session_start::DATE AS session_date,
    MAX(CASE WHEN event_name = 'page_view' THEN 1 ELSE 0 END) AS hit_page_view,
    MAX(CASE WHEN event_name = 'product_view' THEN 1 ELSE 0 END) AS hit_product_view,
    MAX(CASE WHEN event_name = 'add_to_cart' THEN 1 ELSE 0 END) AS hit_add_to_cart,
    MAX(CASE WHEN event_name = 'checkout_start' THEN 1 ELSE 0 END) AS hit_checkout,
    MAX(CASE WHEN event_name = 'purchase' THEN 1 ELSE 0 END) AS hit_purchase
  FROM FACT_EVENTS
  GROUP BY session_id, customer_sk, device_type, session_start::DATE
)
SELECT
  session_date,
  device_type,
  COUNT(*) AS total_sessions,
  SUM(hit_page_view) AS step_1_page_view,
  SUM(hit_product_view) AS step_2_product_view,
  SUM(hit_add_to_cart) AS step_3_add_to_cart,
  SUM(hit_checkout) AS step_4_checkout,
  SUM(hit_purchase) AS step_5_purchase,
  -- Drop-off rates
  ROUND(SUM(hit_product_view) / NULLIF(SUM(hit_page_view), 0) * 100, 1) AS pv_to_pdp_pct,
  ROUND(SUM(hit_add_to_cart) / NULLIF(SUM(hit_product_view), 0) * 100, 1) AS pdp_to_atc_pct,
  ROUND(SUM(hit_checkout) / NULLIF(SUM(hit_add_to_cart), 0) * 100, 1) AS atc_to_checkout_pct,
  ROUND(SUM(hit_purchase) / NULLIF(SUM(hit_checkout), 0) * 100, 1) AS checkout_to_purchase_pct,
  ROUND(SUM(hit_purchase) / NULLIF(COUNT(*), 0) * 100, 1) AS overall_conversion_pct
FROM session_steps
GROUP BY session_date, device_type;

-- Daily session KPIs for product/marketing dashboards
CREATE OR REPLACE VIEW VW_SESSION_KPIS AS
SELECT
  session_start::DATE AS session_date,
  device_type,
  COUNT(*) AS total_sessions,
  SUM(CASE WHEN is_bounce THEN 1 ELSE 0 END) AS bounce_sessions,
  ROUND(SUM(CASE WHEN is_bounce THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) * 100, 1) AS bounce_rate_pct,
  SUM(CASE WHEN is_conversion THEN 1 ELSE 0 END) AS converting_sessions,
  ROUND(SUM(CASE WHEN is_conversion THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) * 100, 1) AS conversion_rate_pct,
  ROUND(AVG(session_duration_sec), 0) AS avg_session_duration_sec,
  ROUND(AVG(page_views), 1) AS avg_page_views,
  ROUND(AVG(product_views), 1) AS avg_product_views
FROM FACT_SESSIONS
GROUP BY session_start::DATE, device_type;
