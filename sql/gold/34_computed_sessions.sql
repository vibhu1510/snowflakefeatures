-- 34_computed_sessions.sql
-- Computed sessionization: assigns session boundaries using 30-minute inactivity
-- timeout for events that lack a pre-supplied session_id from the source.
-- Events with a source-provided session_id pass through unchanged.

USE ROLE DE_TRANSFORM_ROLE;
USE WAREHOUSE TRANSFORM_WH;
USE DATABASE ECOMMERCE_DB;
USE SCHEMA GOLD;

CREATE OR REPLACE VIEW VW_EVENTS_WITH_COMPUTED_SESSION AS
WITH event_gaps AS (
  SELECT
    event_id,
    event_ts,
    event_date,
    customer_sk,
    session_id AS original_session_id,
    event_name,
    device_type,
    product_id,
    load_ts,
    -- Detect new session boundaries using 30-minute inactivity timeout
    CASE
      -- Events with a pre-supplied session_id: no computation needed
      WHEN session_id IS NOT NULL THEN 0
      -- First event for this customer: always starts a new session
      WHEN LAG(event_ts) OVER (PARTITION BY customer_sk ORDER BY event_ts) IS NULL THEN 1
      -- Gap of > 30 minutes since last event: new session
      WHEN DATEDIFF('minute',
        LAG(event_ts) OVER (PARTITION BY customer_sk ORDER BY event_ts),
        event_ts
      ) > 30 THEN 1
      -- Same session
      ELSE 0
    END AS is_new_session
  FROM FACT_EVENTS
),
session_groups AS (
  SELECT
    eg.*,
    -- Running sum of new-session flags gives a unique group number per session
    -- Only computed for events without a source session_id
    SUM(CASE WHEN original_session_id IS NULL THEN is_new_session ELSE 0 END)
      OVER (PARTITION BY customer_sk ORDER BY event_ts ROWS UNBOUNDED PRECEDING) AS session_group
  FROM event_gaps eg
)
SELECT
  event_id,
  event_ts,
  event_date,
  customer_sk,
  original_session_id,
  event_name,
  device_type,
  product_id,
  load_ts,
  is_new_session,
  -- Use source session_id if available, otherwise generate a computed one
  COALESCE(
    original_session_id,
    'COMPUTED_' || customer_sk || '_' || session_group
  ) AS effective_session_id
FROM session_groups;

-- Usage:
-- SELECT * FROM VW_EVENTS_WITH_COMPUTED_SESSION WHERE effective_session_id LIKE 'COMPUTED_%';
-- This view is consumed by SP_BUILD_GOLD_EVENTS for FACT_SESSIONS aggregation.
