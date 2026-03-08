-- 30_dim_fact.sql
-- Gold tables optimized for analytics (facts + dimensions).

USE ROLE DE_TRANSFORM_ROLE;
USE WAREHOUSE TRANSFORM_WH;
USE DATABASE ECOMMERCE_DB;
USE SCHEMA GOLD;

CREATE OR REPLACE SEQUENCE SEQ_CUSTOMER_SK START = 1 INCREMENT = 1;
CREATE OR REPLACE SEQUENCE SEQ_PRODUCT_SK START = 1 INCREMENT = 1;

-- SCD2 Customer Dimension
CREATE OR REPLACE TABLE DIM_CUSTOMER (
  customer_sk NUMBER DEFAULT SEQ_CUSTOMER_SK.NEXTVAL,
  customer_id STRING,
  first_name STRING,
  last_name STRING,
  email STRING,
  phone STRING,
  region STRING,
  valid_from TIMESTAMP_NTZ,
  valid_to TIMESTAMP_NTZ,
  is_current BOOLEAN,
  hash_diff STRING,
  record_source STRING,
  load_ts TIMESTAMP_NTZ
) CLUSTER BY (customer_id, is_current);

-- Type 1 Product Dimension
CREATE OR REPLACE TABLE DIM_PRODUCT (
  product_sk NUMBER DEFAULT SEQ_PRODUCT_SK.NEXTVAL,
  product_id STRING,
  sku STRING,
  name STRING,
  category STRING,
  price NUMBER(10,2),
  currency STRING,
  record_source STRING,
  load_ts TIMESTAMP_NTZ
) CLUSTER BY (product_id);

-- Orders fact (order-level grain)
CREATE OR REPLACE TABLE FACT_ORDERS (
  order_id STRING,
  order_ts TIMESTAMP_NTZ,
  order_date DATE,
  customer_sk NUMBER,
  order_status STRING,
  subtotal NUMBER(10,2),
  tax NUMBER(10,2),
  shipping NUMBER(10,2),
  total NUMBER(10,2),
  payment_method STRING,
  load_ts TIMESTAMP_NTZ
) CLUSTER BY (order_date);

-- Payments fact
CREATE OR REPLACE TABLE FACT_PAYMENTS (
  payment_id STRING,
  order_id STRING,
  payment_ts TIMESTAMP_NTZ,
  customer_sk NUMBER,
  amount NUMBER(10,2),
  status STRING,
  provider STRING,
  load_ts TIMESTAMP_NTZ
) CLUSTER BY (payment_ts);

-- Events fact
CREATE OR REPLACE TABLE FACT_EVENTS (
  event_id STRING,
  event_ts TIMESTAMP_NTZ,
  event_date DATE,
  customer_sk NUMBER,
  session_id STRING,
  event_name STRING,
  device_type STRING,
  product_id STRING,
  load_ts TIMESTAMP_NTZ
) CLUSTER BY (event_date);

-- Returns fact
CREATE OR REPLACE TABLE FACT_RETURNS (
  return_id STRING,
  order_id STRING,
  customer_sk NUMBER,
  return_ts TIMESTAMP_NTZ,
  return_date DATE,
  reason STRING,
  refund_amount NUMBER(10,2),
  status STRING,
  load_ts TIMESTAMP_NTZ
) CLUSTER BY (return_date);

-- Aggregations

-- Cash-basis: refunds attributed to the date the return was processed
CREATE OR REPLACE TABLE AGG_DAILY_SALES (
  order_date DATE,
  total_orders NUMBER,
  gross_revenue NUMBER(12,2),
  total_refunds NUMBER(12,2),
  net_revenue NUMBER(12,2),
  total_returns NUMBER,
  avg_order_value NUMBER(12,2),
  load_ts TIMESTAMP_NTZ
) CLUSTER BY (order_date);

-- Accrual-basis: refunds attributed to the original order date (finance/GAAP view)
CREATE OR REPLACE TABLE AGG_DAILY_SALES_ACCRUAL (
  order_date DATE,
  total_orders NUMBER,
  gross_revenue NUMBER(12,2),
  total_refunds NUMBER(12,2),
  net_revenue NUMBER(12,2),
  total_returns NUMBER,
  avg_order_value NUMBER(12,2),
  load_ts TIMESTAMP_NTZ
) CLUSTER BY (order_date);
