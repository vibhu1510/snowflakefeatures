-- 43_cost_usage.sql
-- Warehouse cost visibility.

USE ROLE ACCOUNTADMIN;

SELECT
  start_time,
  end_time,
  warehouse_name,
  credits_used_compute,
  credits_used_cloud_services
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE start_time >= DATEADD('day', -7, CURRENT_TIMESTAMP())
  AND warehouse_name IN ('INGEST_WH', 'TRANSFORM_WH', 'ANALYTICS_WH')
ORDER BY start_time DESC;
