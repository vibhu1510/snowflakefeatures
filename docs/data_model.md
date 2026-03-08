# Data Model

## Bronze (Raw)
- `BRONZE.CUSTOMERS_RAW`: Raw customer records with metadata columns.
- `BRONZE.PRODUCTS_RAW`: Raw product catalog records.
- `BRONZE.ORDERS_RAW`: Raw orders with monetary fields.
- `BRONZE.PAYMENTS_RAW`: Raw payment transactions.
- `BRONZE.RETURNS_RAW`: Raw return/refund requests.
- `BRONZE.EVENTS_RAW`: Semi-structured clickstream events in VARIANT payload.

## Silver (Clean)
- `SILVER.CUSTOMERS`: Deduplicated customers with `region` (latest `updated_at`).
- `SILVER.PRODUCTS`: Clean product catalog.
- `SILVER.ORDERS`: Clean orders with consistent types.
- `SILVER.PAYMENTS`: Clean payment facts.
- `SILVER.RETURNS`: Clean return/refund records.
- `SILVER.EVENTS`: Parsed events with searchable fields.

## Gold (Analytics)
- `GOLD.DIM_CUSTOMER` (SCD2, PII masked for analysts)
  - Grain: one row per customer version
  - Keys: `customer_sk` (surrogate), `customer_id` (business)
  - Includes `region` for row-level security filtering
- `GOLD.DIM_PRODUCT` (Type 1)
  - Grain: one row per product
  - Keys: `product_sk`, `product_id`
- `GOLD.FACT_ORDERS`
  - Grain: one row per order
  - Keys: `order_id` with `customer_sk`
- `GOLD.FACT_PAYMENTS`
  - Grain: one row per payment
  - Keys: `payment_id`, `order_id`, `customer_sk`
- `GOLD.FACT_EVENTS`
  - Grain: one row per event
  - Keys: `event_id`, `customer_sk`
- `GOLD.FACT_RETURNS`
  - Grain: one row per return
  - Keys: `return_id`, `order_id`, `customer_sk`
- `GOLD.FACT_SESSIONS`
  - Grain: one row per session
  - Metrics: duration, page views, bounce/conversion flags
- `GOLD.AGG_DAILY_SALES`
  - Grain: one row per day
  - Metrics: total orders, **gross revenue, refunds, net revenue**, avg order value
- `GOLD.VW_REVENUE_RECONCILIATION`
  - Order-level reconciliation: orders vs payments vs refunds
  - Flags: FULLY_PAID, UNDERPAID, OVERPAID, PARTIAL_PENDING, NO_PAYMENT
- `GOLD.VW_CONVERSION_FUNNEL`
  - Session-level funnel: page_view -> product_view -> add_to_cart -> checkout -> purchase
  - Drop-off percentages per step, by device type
