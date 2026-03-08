-- 01_create_stages.sql
-- File formats and stages for batch and event ingestion.

USE ROLE DE_INGEST_ROLE;
USE WAREHOUSE INGEST_WH;
USE DATABASE ECOMMERCE_DB;
USE SCHEMA BRONZE;

-- CSV file format (schema evolution friendly)
CREATE OR REPLACE FILE FORMAT FF_ECOM_CSV
  TYPE = CSV
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  NULL_IF = ('', 'NULL', 'null')
  TIMESTAMP_FORMAT = 'YYYY-MM-DD"T"HH24:MI:SS'
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;

-- JSON file format for event payloads
CREATE OR REPLACE FILE FORMAT FF_ECOM_JSON
  TYPE = JSON
  STRIP_OUTER_ARRAY = FALSE;

-- Internal stage for local or app-based PUT
CREATE OR REPLACE STAGE ECOM_RAW_STAGE
  COMMENT = 'Internal stage for sample files';

-- Example external stage (requires storage integration)
-- CREATE OR REPLACE STAGE ECOM_S3_STAGE
--   URL = 's3://<your-bucket>/ecommerce/'
--   STORAGE_INTEGRATION = <your_integration>
--   FILE_FORMAT = FF_ECOM_CSV;

-- Sample PUT commands (run from SnowSQL or Snowsight worksheets)
-- PUT file://path/to/customers.csv @ECOM_RAW_STAGE/batch/ AUTO_COMPRESS=TRUE;
-- PUT file://path/to/products.csv  @ECOM_RAW_STAGE/batch/ AUTO_COMPRESS=TRUE;
-- PUT file://path/to/orders.csv    @ECOM_RAW_STAGE/batch/ AUTO_COMPRESS=TRUE;
-- PUT file://path/to/payments.csv  @ECOM_RAW_STAGE/batch/ AUTO_COMPRESS=TRUE;
-- PUT file://path/to/events.json   @ECOM_RAW_STAGE/events/ AUTO_COMPRESS=TRUE;
