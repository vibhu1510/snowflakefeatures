-- 41_task_history.sql
-- Task history for orchestration observability.

-- Requires ACCOUNTADMIN or a role with access to SNOWFLAKE.ACCOUNT_USAGE
USE ROLE ACCOUNTADMIN;

SELECT
  name,
  state,
  completed_time,
  scheduled_time,
  query_id,
  error_code,
  error_message
FROM SNOWFLAKE.ACCOUNT_USAGE.TASK_HISTORY
WHERE name ILIKE 'TASK_%'
  AND scheduled_time >= DATEADD('day', -7, CURRENT_TIMESTAMP())
ORDER BY scheduled_time DESC;
