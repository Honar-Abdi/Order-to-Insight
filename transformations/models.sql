-- Load raw CSVs as views
CREATE OR REPLACE VIEW raw_orders AS
SELECT *
FROM read_csv_auto('data/raw/orders.csv');

CREATE OR REPLACE VIEW raw_order_events AS
SELECT *
FROM read_csv_auto('data/raw/order_events.csv');

-- Normalize timestamps
CREATE OR REPLACE VIEW orders AS
SELECT
  order_id,
  customer_id,
  CAST(order_created_at AS TIMESTAMP) AS order_created_at,
  CAST(order_amount AS DOUBLE) AS order_amount,
  currency,
  order_status
FROM raw_orders;

CREATE OR REPLACE VIEW order_events AS
SELECT
  event_id,
  order_id,
  event_type,
  CAST(event_timestamp AS TIMESTAMP) AS event_timestamp,
  source_system
FROM raw_order_events;

-- Event summary per order
CREATE OR REPLACE TABLE order_event_summary AS
SELECT
  order_id,
  MIN(CASE WHEN event_type = 'order_created' THEN event_timestamp END) AS created_event_ts,
  MIN(CASE WHEN event_type = 'payment_confirmed' THEN event_timestamp END) AS payment_event_ts,
  MIN(CASE WHEN event_type = 'order_shipped' THEN event_timestamp END) AS shipped_event_ts,
  MIN(CASE WHEN event_type = 'order_cancelled' THEN event_timestamp END) AS cancelled_event_ts,
  COUNT(*) AS event_count,
  SUM(CASE WHEN event_type = 'payment_confirmed' THEN 1 ELSE 0 END) AS payment_event_count,
  SUM(CASE WHEN event_type = 'order_shipped' THEN 1 ELSE 0 END) AS shipped_event_count
FROM order_events
GROUP BY order_id;

-- Fact table: one row per order
CREATE OR REPLACE TABLE fact_orders AS
SELECT
  o.order_id,
  o.customer_id,
  o.order_created_at,
  o.order_amount,
  o.currency,
  o.order_status,
  s.created_event_ts,
  s.payment_event_ts,
  s.shipped_event_ts,
  s.cancelled_event_ts,
  s.event_count,
  CASE WHEN s.payment_event_ts IS NOT NULL THEN 1 ELSE 0 END AS has_payment_event,
  CASE WHEN s.shipped_event_ts IS NOT NULL THEN 1 ELSE 0 END AS has_shipped_event,
  CASE
    WHEN o.order_status = 'completed' AND s.payment_event_ts IS NULL THEN 1
    ELSE 0
  END AS dq_completed_missing_payment_flag,
  CASE
    WHEN o.order_status = 'cancelled' AND s.shipped_event_ts IS NOT NULL THEN 1
    ELSE 0
  END AS dq_cancelled_has_shipment_flag
FROM orders o
LEFT JOIN order_event_summary s
  ON o.order_id = s.order_id;

-- Daily revenue fact
CREATE OR REPLACE TABLE fact_daily_revenue AS
SELECT
  DATE(order_created_at) AS order_date,
  SUM(CASE WHEN order_status = 'completed' THEN order_amount ELSE 0 END) AS revenue_completed,
  COUNT(*) AS orders_total,
  SUM(CASE WHEN order_status = 'completed' THEN 1 ELSE 0 END) AS orders_completed
FROM fact_orders
GROUP BY DATE(order_created_at)
ORDER BY order_date;
