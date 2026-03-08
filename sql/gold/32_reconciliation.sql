-- 32_reconciliation.sql
-- Revenue reconciliation: orders vs payments with mismatch flagging.
-- This is the view that finance and data teams use to resolve discrepancies.

USE ROLE DE_TRANSFORM_ROLE;
USE WAREHOUSE TRANSFORM_WH;
USE DATABASE ECOMMERCE_DB;
USE SCHEMA GOLD;

-- Order-level reconciliation
CREATE OR REPLACE VIEW VW_REVENUE_RECONCILIATION AS
WITH order_totals AS (
  SELECT
    order_id,
    order_date,
    customer_sk,
    order_status,
    total AS order_total
  FROM FACT_ORDERS
),
payment_totals AS (
  SELECT
    order_id,
    SUM(CASE WHEN status = 'SETTLED' THEN amount ELSE 0 END) AS settled_amount,
    SUM(CASE WHEN status = 'PENDING' THEN amount ELSE 0 END) AS pending_amount,
    SUM(CASE WHEN status = 'REFUNDED' THEN amount ELSE 0 END) AS refunded_amount,
    SUM(amount) AS total_payment_amount,
    COUNT(*) AS payment_count,
    MAX(payment_ts) AS last_payment_ts
  FROM FACT_PAYMENTS
  GROUP BY order_id
),
return_totals AS (
  SELECT
    order_id,
    SUM(refund_amount) AS total_refund_amount,
    COUNT(*) AS return_count
  FROM FACT_RETURNS
  GROUP BY order_id
)
SELECT
  o.order_id,
  o.order_date,
  o.customer_sk,
  o.order_status,
  o.order_total,
  COALESCE(p.settled_amount, 0) AS settled_amount,
  COALESCE(p.pending_amount, 0) AS pending_amount,
  COALESCE(p.refunded_amount, 0) AS refunded_amount,
  COALESCE(p.total_payment_amount, 0) AS total_payment_amount,
  COALESCE(p.payment_count, 0) AS payment_count,
  p.last_payment_ts,
  COALESCE(r.total_refund_amount, 0) AS total_refund_amount,
  COALESCE(r.return_count, 0) AS return_count,
  -- Net position
  o.order_total - COALESCE(p.settled_amount, 0) AS outstanding_balance,
  -- Mismatch classification
  CASE
    WHEN p.order_id IS NULL
      THEN 'NO_PAYMENT'
    WHEN p.settled_amount = o.order_total AND COALESCE(r.total_refund_amount, 0) = 0
      THEN 'FULLY_PAID'
    WHEN p.settled_amount = o.order_total AND COALESCE(r.total_refund_amount, 0) > 0
      THEN 'PAID_WITH_RETURN'
    WHEN p.settled_amount < o.order_total AND p.pending_amount > 0
      THEN 'PARTIAL_PENDING'
    WHEN p.settled_amount < o.order_total
      THEN 'UNDERPAID'
    WHEN p.settled_amount > o.order_total
      THEN 'OVERPAID'
    ELSE 'UNKNOWN'
  END AS recon_status,
  -- Days since order with no settlement
  CASE
    WHEN COALESCE(p.settled_amount, 0) < o.order_total
      THEN DATEDIFF('day', o.order_date, CURRENT_DATE())
    ELSE NULL
  END AS days_outstanding
FROM order_totals o
LEFT JOIN payment_totals p ON o.order_id = p.order_id
LEFT JOIN return_totals r ON o.order_id = r.order_id;

-- Summary for finance dashboards
CREATE OR REPLACE VIEW VW_RECON_SUMMARY AS
SELECT
  recon_status,
  COUNT(*) AS order_count,
  SUM(order_total) AS total_order_value,
  SUM(settled_amount) AS total_settled,
  SUM(outstanding_balance) AS total_outstanding,
  SUM(total_refund_amount) AS total_refunds,
  AVG(days_outstanding) AS avg_days_outstanding
FROM VW_REVENUE_RECONCILIATION
GROUP BY recon_status
ORDER BY total_order_value DESC;
