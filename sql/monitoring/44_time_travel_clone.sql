-- 44_time_travel_clone.sql
-- Time travel and zero-copy cloning examples for recovery and dev/test.

USE ROLE SYSADMIN;
USE DATABASE ECOMMERCE_DB;

-- Time travel query (query as-of 5 minutes ago)
SELECT *
FROM GOLD.FACT_ORDERS
AT (OFFSET => -60*5);

-- Undrop in case of accidental delete
-- DROP TABLE GOLD.FACT_ORDERS;
-- UNDROP TABLE GOLD.FACT_ORDERS;

-- Zero-copy clone for dev/test
CREATE OR REPLACE DATABASE ECOMMERCE_DB_DEV CLONE ECOMMERCE_DB;

-- Optional: drop clone when finished
-- DROP DATABASE ECOMMERCE_DB_DEV;
