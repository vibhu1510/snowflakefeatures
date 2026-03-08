-- 45_dq_alerting.sql
-- DQ alerting: Snowflake ALERT on failures + trend analysis.

USE ROLE DE_TRANSFORM_ROLE;
USE WAREHOUSE TRANSFORM_WH;
USE DATABASE ECOMMERCE_DB;
USE SCHEMA UTIL;

-- DQ failure trend (rolling 7-day view for dashboards)
CREATE OR REPLACE VIEW VW_DQ_FAILURE_TREND AS
SELECT
  check_ts::DATE AS check_date,
  check_name,
  COUNT(*) AS total_runs,
  SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END) AS fail_count,
  SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END) AS pass_count,
  ROUND(SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END)
    / NULLIF(COUNT(*), 0) * 100, 1) AS failure_rate_pct
FROM DQ_RESULTS
WHERE check_ts >= DATEADD('day', -7, CURRENT_DATE())
GROUP BY check_ts::DATE, check_name
ORDER BY check_date DESC, failure_rate_pct DESC;

-- Latest status per check
CREATE OR REPLACE VIEW VW_DQ_LATEST_STATUS AS
SELECT *
FROM (
  SELECT
    check_name,
    status,
    observed_value,
    expected_value,
    details,
    check_ts,
    ROW_NUMBER() OVER (PARTITION BY check_name ORDER BY check_ts DESC) AS rn
  FROM DQ_RESULTS
)
WHERE rn = 1
ORDER BY status DESC, check_name;

-- Snowflake ALERT: fires when new DQ failures are detected since last check
-- The alert checks every 15 minutes; if there are recent failures it triggers.
CREATE OR REPLACE ALERT ALERT_DQ_FAILURES
  WAREHOUSE = TRANSFORM_WH
  SCHEDULE = 'USING CRON */15 * * * * America/Los_Angeles'
  IF (EXISTS (
    SELECT 1
    FROM UTIL.DQ_RESULTS
    WHERE status = 'FAIL'
      AND check_ts >= DATEADD('minute', -15, CURRENT_TIMESTAMP())
  ))
  THEN
    CALL SYSTEM$SEND_EMAIL(
      'DQ_EMAIL_INTEGRATION',
      'data-team@example.com',
      'DATA QUALITY ALERT: Failures detected in ECOMMERCE_DB',
      (SELECT LISTAGG(
        CONCAT(check_name, ': ', details, ' (observed=', observed_value, ')'),
        '\n'
       )
       FROM UTIL.DQ_RESULTS
       WHERE status = 'FAIL'
         AND check_ts >= DATEADD('minute', -15, CURRENT_TIMESTAMP()))
    );

-- NOTE: To use email alerting, you must first create an email integration:
-- CREATE NOTIFICATION INTEGRATION DQ_EMAIL_INTEGRATION
--   TYPE = EMAIL
--   ENABLED = TRUE
--   ALLOWED_RECIPIENTS = ('data-team@example.com');

-- Alternative: use a webhook for Slack/PagerDuty (requires external function)
-- CALL SYSTEM$SEND_NOTIFICATION('DQ_WEBHOOK_INTEGRATION', ...);

-- Resume alert
ALTER ALERT ALERT_DQ_FAILURES RESUME;
