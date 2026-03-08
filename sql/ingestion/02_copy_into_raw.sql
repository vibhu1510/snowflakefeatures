-- 02_copy_into_raw.sql
-- Stored procedures for batch and event ingestion using COPY INTO.

USE ROLE DE_INGEST_ROLE;
USE WAREHOUSE INGEST_WH;
USE DATABASE ECOMMERCE_DB;
USE SCHEMA UTIL;

CREATE OR REPLACE PROCEDURE SP_INGEST_BATCH()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
  -- Customers
  COPY INTO BRONZE.CUSTOMERS_RAW
    (customer_id, first_name, last_name, email, phone, region, created_at, updated_at, ingestion_ts, source_file, load_id)
  FROM (
    SELECT
      $1::STRING,
      $2::STRING,
      $3::STRING,
      $4::STRING,
      $5::STRING,
      $6::STRING,
      $7::TIMESTAMP_NTZ,
      $8::TIMESTAMP_NTZ,
      CURRENT_TIMESTAMP(),
      METADATA$FILENAME,
      CONCAT(METADATA$FILENAME, ':', METADATA$FILE_ROW_NUMBER)
    FROM @BRONZE.ECOM_RAW_STAGE/batch/
  )
  FILE_FORMAT = (FORMAT_NAME = 'BRONZE.FF_ECOM_CSV')
  PATTERN = '.*customers.*\\.csv.*'
  ON_ERROR = 'CONTINUE';

  -- Products
  COPY INTO BRONZE.PRODUCTS_RAW
    (product_id, sku, name, category, price, currency, created_at, updated_at, ingestion_ts, source_file, load_id)
  FROM (
    SELECT
      $1::STRING,
      $2::STRING,
      $3::STRING,
      $4::STRING,
      $5::NUMBER(10,2),
      $6::STRING,
      $7::TIMESTAMP_NTZ,
      $8::TIMESTAMP_NTZ,
      CURRENT_TIMESTAMP(),
      METADATA$FILENAME,
      CONCAT(METADATA$FILENAME, ':', METADATA$FILE_ROW_NUMBER)
    FROM @BRONZE.ECOM_RAW_STAGE/batch/
  )
  FILE_FORMAT = (FORMAT_NAME = 'BRONZE.FF_ECOM_CSV')
  PATTERN = '.*products.*\\.csv.*'
  ON_ERROR = 'CONTINUE';

  -- Orders
  COPY INTO BRONZE.ORDERS_RAW
    (order_id, customer_id, order_ts, status, subtotal, tax, shipping, total, payment_method, updated_at, ingestion_ts, source_file, load_id)
  FROM (
    SELECT
      $1::STRING,
      $2::STRING,
      $3::TIMESTAMP_NTZ,
      $4::STRING,
      $5::NUMBER(10,2),
      $6::NUMBER(10,2),
      $7::NUMBER(10,2),
      $8::NUMBER(10,2),
      $9::STRING,
      $10::TIMESTAMP_NTZ,
      CURRENT_TIMESTAMP(),
      METADATA$FILENAME,
      CONCAT(METADATA$FILENAME, ':', METADATA$FILE_ROW_NUMBER)
    FROM @BRONZE.ECOM_RAW_STAGE/batch/
  )
  FILE_FORMAT = (FORMAT_NAME = 'BRONZE.FF_ECOM_CSV')
  PATTERN = '.*orders.*\\.csv.*'
  ON_ERROR = 'CONTINUE';

  -- Payments
  COPY INTO BRONZE.PAYMENTS_RAW
    (payment_id, order_id, payment_ts, amount, status, provider, updated_at, ingestion_ts, source_file, load_id)
  FROM (
    SELECT
      $1::STRING,
      $2::STRING,
      $3::TIMESTAMP_NTZ,
      $4::NUMBER(10,2),
      $5::STRING,
      $6::STRING,
      $7::TIMESTAMP_NTZ,
      CURRENT_TIMESTAMP(),
      METADATA$FILENAME,
      CONCAT(METADATA$FILENAME, ':', METADATA$FILE_ROW_NUMBER)
    FROM @BRONZE.ECOM_RAW_STAGE/batch/
  )
  FILE_FORMAT = (FORMAT_NAME = 'BRONZE.FF_ECOM_CSV')
  PATTERN = '.*payments.*\\.csv.*'
  ON_ERROR = 'CONTINUE';

  -- Returns
  COPY INTO BRONZE.RETURNS_RAW
    (return_id, order_id, customer_id, return_ts, reason, refund_amount, status, updated_at, ingestion_ts, source_file, load_id)
  FROM (
    SELECT
      $1::STRING,
      $2::STRING,
      $3::STRING,
      $4::TIMESTAMP_NTZ,
      $5::STRING,
      $6::NUMBER(10,2),
      $7::STRING,
      $8::TIMESTAMP_NTZ,
      CURRENT_TIMESTAMP(),
      METADATA$FILENAME,
      CONCAT(METADATA$FILENAME, ':', METADATA$FILE_ROW_NUMBER)
    FROM @BRONZE.ECOM_RAW_STAGE/batch/
  )
  FILE_FORMAT = (FORMAT_NAME = 'BRONZE.FF_ECOM_CSV')
  PATTERN = '.*returns.*\\.csv.*'
  ON_ERROR = 'CONTINUE';

  -- Post-COPY GDPR suppression cleanup: remove any re-ingested suppressed customers.
  -- COPY INTO does not support subquery filters, so we purge after loading.
  DELETE FROM BRONZE.CUSTOMERS_RAW WHERE customer_id IN (SELECT customer_id FROM UTIL.GDPR_SUPPRESSION_LIST);
  DELETE FROM BRONZE.ORDERS_RAW WHERE customer_id IN (SELECT customer_id FROM UTIL.GDPR_SUPPRESSION_LIST);
  DELETE FROM BRONZE.PAYMENTS_RAW WHERE order_id IN (
    SELECT order_id FROM BRONZE.ORDERS_RAW WHERE customer_id IN (SELECT customer_id FROM UTIL.GDPR_SUPPRESSION_LIST)
  );
  DELETE FROM BRONZE.RETURNS_RAW WHERE customer_id IN (SELECT customer_id FROM UTIL.GDPR_SUPPRESSION_LIST);

  RETURN 'SP_INGEST_BATCH completed';
END;
$$;

CREATE OR REPLACE PROCEDURE SP_INGEST_EVENTS()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
  COPY INTO BRONZE.EVENTS_RAW
    (event_id, event_ts, user_id, session_id, event_name, device_type, payload, ingestion_ts, source_file, load_id)
  FROM (
    SELECT
      $1:event_id::STRING,
      $1:event_ts::TIMESTAMP_NTZ,
      $1:user_id::STRING,
      $1:session_id::STRING,
      $1:event_name::STRING,
      $1:device_type::STRING,
      $1:payload,
      CURRENT_TIMESTAMP(),
      METADATA$FILENAME,
      CONCAT(METADATA$FILENAME, ':', METADATA$FILE_ROW_NUMBER)
    FROM @BRONZE.ECOM_RAW_STAGE/events/
  )
  FILE_FORMAT = (FORMAT_NAME = 'BRONZE.FF_ECOM_JSON')
  PATTERN = '.*events.*\\.json.*'
  ON_ERROR = 'CONTINUE';

  -- Post-COPY GDPR suppression cleanup: remove any re-ingested suppressed customer events.
  DELETE FROM BRONZE.EVENTS_RAW WHERE user_id IN (SELECT customer_id FROM UTIL.GDPR_SUPPRESSION_LIST);

  RETURN 'SP_INGEST_EVENTS completed';
END;
$$;
