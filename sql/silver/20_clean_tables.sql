-- 20_clean_tables.sql
-- Cleaned silver tables with typed columns and constraints (not enforced).

USE ROLE DE_TRANSFORM_ROLE;
USE WAREHOUSE TRANSFORM_WH;
USE DATABASE ECOMMERCE_DB;
USE SCHEMA SILVER;

CREATE OR REPLACE TABLE CUSTOMERS (
  customer_id STRING NOT NULL,
  first_name STRING,
  last_name STRING,
  email STRING,
  phone STRING,
  region STRING,
  created_at TIMESTAMP_NTZ,
  updated_at TIMESTAMP_NTZ,
  record_source STRING,
  load_ts TIMESTAMP_NTZ,
  CONSTRAINT pk_customers PRIMARY KEY (customer_id) NOT ENFORCED
);

CREATE OR REPLACE TABLE PRODUCTS (
  product_id STRING NOT NULL,
  sku STRING,
  name STRING,
  category STRING,
  price NUMBER(10,2),
  currency STRING,
  created_at TIMESTAMP_NTZ,
  updated_at TIMESTAMP_NTZ,
  record_source STRING,
  load_ts TIMESTAMP_NTZ,
  CONSTRAINT pk_products PRIMARY KEY (product_id) NOT ENFORCED
);

CREATE OR REPLACE TABLE ORDERS (
  order_id STRING NOT NULL,
  customer_id STRING NOT NULL,
  order_ts TIMESTAMP_NTZ,
  status STRING,
  subtotal NUMBER(10,2),
  tax NUMBER(10,2),
  shipping NUMBER(10,2),
  total NUMBER(10,2),
  payment_method STRING,
  updated_at TIMESTAMP_NTZ,
  record_source STRING,
  load_ts TIMESTAMP_NTZ,
  CONSTRAINT pk_orders PRIMARY KEY (order_id) NOT ENFORCED
);

CREATE OR REPLACE TABLE PAYMENTS (
  payment_id STRING NOT NULL,
  order_id STRING NOT NULL,
  payment_ts TIMESTAMP_NTZ,
  amount NUMBER(10,2),
  status STRING,
  provider STRING,
  updated_at TIMESTAMP_NTZ,
  record_source STRING,
  load_ts TIMESTAMP_NTZ,
  CONSTRAINT pk_payments PRIMARY KEY (payment_id) NOT ENFORCED
);

CREATE OR REPLACE TABLE EVENTS (
  event_id STRING NOT NULL,
  event_ts TIMESTAMP_NTZ,
  user_id STRING,
  session_id STRING,
  event_name STRING,
  device_type STRING,
  payload VARIANT,
  record_source STRING,
  load_ts TIMESTAMP_NTZ,
  CONSTRAINT pk_events PRIMARY KEY (event_id) NOT ENFORCED
);

CREATE OR REPLACE TABLE RETURNS (
  return_id STRING NOT NULL,
  order_id STRING NOT NULL,
  customer_id STRING NOT NULL,
  return_ts TIMESTAMP_NTZ,
  reason STRING,
  refund_amount NUMBER(10,2),
  status STRING,
  updated_at TIMESTAMP_NTZ,
  record_source STRING,
  load_ts TIMESTAMP_NTZ,
  CONSTRAINT pk_returns PRIMARY KEY (return_id) NOT ENFORCED
);

-- Search optimization to accelerate equality lookups on event users/sessions
ALTER TABLE EVENTS ADD SEARCH OPTIMIZATION ON EQUALITY(user_id, session_id);
