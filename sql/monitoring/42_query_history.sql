-- 42_query_history.sql
-- Query history for performance troubleshooting and cost attribution.

USE ROLE ACCOUNTADMIN;

SELECT
  query_id,
  user_name,
  warehouse_name,
  database_name,
  schema_name,
  start_time,
  end_time,
  total_elapsed_time / 1000 AS total_elapsed_seconds,
  credits_used_cloud_services,
  credits_used_compute
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE start_time >= DATEADD('day', -7, CURRENT_TIMESTAMP())
  AND database_name = 'ECOMMERCE_DB'
ORDER BY start_time DESC;
