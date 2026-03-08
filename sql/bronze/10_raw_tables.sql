-- 10_raw_tables.sql
-- Raw landing tables (bronze) with minimal transformation and metadata columns.

USE ROLE DE_INGEST_ROLE;
USE WAREHOUSE INGEST_WH;
USE DATABASE ECOMMERCE_DB;
USE SCHEMA BRONZE;

CREATE OR REPLACE TABLE CUSTOMERS_RAW (
  customer_id STRING,
  first_name STRING,
  last_name STRING,
  email STRING,
  phone STRING,
  region STRING,
  created_at TIMESTAMP_NTZ,
  updated_at TIMESTAMP_NTZ,
  ingestion_ts TIMESTAMP_NTZ,
  source_file STRING,
  load_id STRING
);

CREATE OR REPLACE TABLE PRODUCTS_RAW (
  product_id STRING,
  sku STRING,
  name STRING,
  category STRING,
  price NUMBER(10,2),
  currency STRING,
  created_at TIMESTAMP_NTZ,
  updated_at TIMESTAMP_NTZ,
  ingestion_ts TIMESTAMP_NTZ,
  source_file STRING,
  load_id STRING
);

CREATE OR REPLACE TABLE ORDERS_RAW (
  order_id STRING,
  customer_id STRING,
  order_ts TIMESTAMP_NTZ,
  status STRING,
  subtotal NUMBER(10,2),
  tax NUMBER(10,2),
  shipping NUMBER(10,2),
  total NUMBER(10,2),
  payment_method STRING,
  updated_at TIMESTAMP_NTZ,
  ingestion_ts TIMESTAMP_NTZ,
  source_file STRING,
  load_id STRING
);

CREATE OR REPLACE TABLE PAYMENTS_RAW (
  payment_id STRING,
  order_id STRING,
  payment_ts TIMESTAMP_NTZ,
  amount NUMBER(10,2),
  status STRING,
  provider STRING,
  updated_at TIMESTAMP_NTZ,
  ingestion_ts TIMESTAMP_NTZ,
  source_file STRING,
  load_id STRING
);

CREATE OR REPLACE TABLE RETURNS_RAW (
  return_id STRING,
  order_id STRING,
  customer_id STRING,
  return_ts TIMESTAMP_NTZ,
  reason STRING,
  refund_amount NUMBER(10,2),
  status STRING,
  updated_at TIMESTAMP_NTZ,
  ingestion_ts TIMESTAMP_NTZ,
  source_file STRING,
  load_id STRING
);

-- Events are semi-structured to handle schema evolution safely
CREATE OR REPLACE TABLE EVENTS_RAW (
  event_id STRING,
  event_ts TIMESTAMP_NTZ,
  user_id STRING,
  session_id STRING,
  event_name STRING,
  device_type STRING,
  payload VARIANT,
  ingestion_ts TIMESTAMP_NTZ,
  source_file STRING,
  load_id STRING
);
