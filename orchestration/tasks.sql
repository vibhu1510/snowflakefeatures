-- tasks.sql
-- Orchestration with Snowflake Tasks: two parallel DAG paths for batch and events.
-- Batch data (customers, products, orders, payments, returns) runs hourly.
-- Event data (clickstream) runs every 5 minutes for near-real-time analytics.

USE ROLE DE_ADMIN_ROLE;
USE DATABASE ECOMMERCE_DB;
USE SCHEMA UTIL;

-- ============================================================
-- BATCH PATH (hourly): ingest → silver merge → gold build → DQ
-- ============================================================

-- Root task: batch ingestion (hourly)
CREATE OR REPLACE TASK TASK_INGEST_BATCH
  WAREHOUSE = INGEST_WH
  SCHEDULE = 'USING CRON 0 * * * * America/Los_Angeles'
  USER_TASK_TIMEOUT_MS = 900000
  COMMENT = 'Hourly batch ingestion: customers, products, orders, payments, returns'
AS
  CALL UTIL.SP_INGEST_BATCH();

-- Merge batch data into silver after ingestion
CREATE OR REPLACE TASK TASK_MERGE_SILVER_BATCH
  WAREHOUSE = TRANSFORM_WH
  AFTER TASK_INGEST_BATCH
  USER_TASK_TIMEOUT_MS = 900000
  COMMENT = 'Merge batch streams (customers, products, orders, payments, returns) into silver'
AS
  CALL UTIL.SP_MERGE_SILVER_BATCH();

-- Build gold batch tables (dimensions + order/payment/return facts + aggregates)
CREATE OR REPLACE TASK TASK_BUILD_GOLD_BATCH
  WAREHOUSE = TRANSFORM_WH
  AFTER TASK_MERGE_SILVER_BATCH
  USER_TASK_TIMEOUT_MS = 900000
  COMMENT = 'Stream-driven gold build: DIM_PRODUCT, DIM_CUSTOMER SCD2, FACT_ORDERS/PAYMENTS/RETURNS, aggregates'
AS
  CALL UTIL.SP_BUILD_GOLD_BATCH();

-- Data quality checks after batch gold build
CREATE OR REPLACE TASK TASK_DQ_CHECKS
  WAREHOUSE = TRANSFORM_WH
  AFTER TASK_BUILD_GOLD_BATCH
  USER_TASK_TIMEOUT_MS = 300000
  COMMENT = 'Run all DQ checks: row counts, freshness, referential integrity, stream staleness, schema drift'
AS
  CALL UTIL.SP_RUN_DQ_CHECKS();

-- ============================================================
-- EVENTS PATH (every 5 minutes): ingest → silver merge → gold build
-- ============================================================

-- Root task: event ingestion (every 5 minutes)
CREATE OR REPLACE TASK TASK_INGEST_EVENTS
  WAREHOUSE = INGEST_WH
  SCHEDULE = 'USING CRON */5 * * * * America/Los_Angeles'
  USER_TASK_TIMEOUT_MS = 300000
  COMMENT = 'High-frequency clickstream event ingestion'
AS
  CALL UTIL.SP_INGEST_EVENTS();

-- Merge events into silver
CREATE OR REPLACE TASK TASK_MERGE_SILVER_EVENTS
  WAREHOUSE = TRANSFORM_WH
  AFTER TASK_INGEST_EVENTS
  USER_TASK_TIMEOUT_MS = 300000
  COMMENT = 'Merge event stream into silver (with cross-batch dedup guard)'
AS
  CALL UTIL.SP_MERGE_SILVER_EVENTS();

-- Build gold event tables (FACT_EVENTS + FACT_SESSIONS with computed sessionization)
CREATE OR REPLACE TASK TASK_BUILD_GOLD_EVENTS
  WAREHOUSE = TRANSFORM_WH
  AFTER TASK_MERGE_SILVER_EVENTS
  USER_TASK_TIMEOUT_MS = 300000
  COMMENT = 'Stream-driven gold build: FACT_EVENTS, FACT_SESSIONS (with computed sessionization)'
AS
  CALL UTIL.SP_BUILD_GOLD_EVENTS();

-- ============================================================
-- RETRY BEHAVIOR (suspend after repeated failures)
-- ============================================================

-- Batch path: suspend after 3 consecutive failures
ALTER TASK TASK_INGEST_BATCH SET SUSPEND_TASK_AFTER_NUM_FAILURES = 3;
ALTER TASK TASK_MERGE_SILVER_BATCH SET SUSPEND_TASK_AFTER_NUM_FAILURES = 3;
ALTER TASK TASK_BUILD_GOLD_BATCH SET SUSPEND_TASK_AFTER_NUM_FAILURES = 3;
ALTER TASK TASK_DQ_CHECKS SET SUSPEND_TASK_AFTER_NUM_FAILURES = 3;

-- Events path: higher tolerance (5) since events are high-frequency
ALTER TASK TASK_INGEST_EVENTS SET SUSPEND_TASK_AFTER_NUM_FAILURES = 5;
ALTER TASK TASK_MERGE_SILVER_EVENTS SET SUSPEND_TASK_AFTER_NUM_FAILURES = 5;
ALTER TASK TASK_BUILD_GOLD_EVENTS SET SUSPEND_TASK_AFTER_NUM_FAILURES = 5;

-- ============================================================
-- RESUME TASKS (children first, then root tasks)
-- ============================================================

-- Batch path (leaf → root)
ALTER TASK TASK_DQ_CHECKS RESUME;
ALTER TASK TASK_BUILD_GOLD_BATCH RESUME;
ALTER TASK TASK_MERGE_SILVER_BATCH RESUME;
ALTER TASK TASK_INGEST_BATCH RESUME;

-- Events path (leaf → root)
ALTER TASK TASK_BUILD_GOLD_EVENTS RESUME;
ALTER TASK TASK_MERGE_SILVER_EVENTS RESUME;
ALTER TASK TASK_INGEST_EVENTS RESUME;

-- ============================================================
-- DAG TOPOLOGY
-- ============================================================
-- Batch (hourly):
--   TASK_INGEST_BATCH (CRON 0 * * * *)
--     → TASK_MERGE_SILVER_BATCH
--       → TASK_BUILD_GOLD_BATCH
--         → TASK_DQ_CHECKS
--
-- Events (every 5 minutes):
--   TASK_INGEST_EVENTS (CRON */5 * * * *)
--     → TASK_MERGE_SILVER_EVENTS
--       → TASK_BUILD_GOLD_EVENTS
--
-- The two paths run independently. Events reach GOLD within ~5-7 minutes
-- of file landing, instead of waiting up to 60 minutes for the batch cycle.
-- DQ checks run on the batch path since most checks target batch tables;
-- event-specific DQ can be added to SP_BUILD_GOLD_EVENTS if needed.
