-- These queries are designed to be run against data/processed/warehouse.duckdb
-- Tables used: fact_orders, order_event_summary, fact_daily_revenue

-- 1) Overall KPI summary
SELECT
  COUNT(*) AS orders_total,
  SUM(CASE WHEN order_status = 'completed' THEN 1 ELSE 0 END) AS orders_completed,
  SUM(CASE WHEN order_status = 'cancelled' THEN 1 ELSE 0 END) AS orders_cancelled,
  SUM(CASE WHEN order_status = 'refunded' THEN 1 ELSE 0 END) AS orders_refunded,
  ROUND(SUM(CASE WHEN order_status = 'completed' THEN order_amount ELSE 0 END), 2) AS revenue_completed
FROM fact_orders;

-- 2) Revenue trend over time (daily)
SELECT
  order_date,
  ROUND(revenue_completed, 2) AS revenue_completed,
  orders_total,
  orders_completed
FROM fact_daily_revenue
ORDER BY order_date;

-- 3) Average order value (completed)
SELECT
  ROUND(AVG(order_amount), 2) AS avg_order_value_completed
FROM fact_orders
WHERE order_status = 'completed';

-- 4) Data quality flags inside the modeled layer
SELECT
  SUM(dq_completed_missing_payment_flag) AS completed_missing_payment,
  SUM(dq_cancelled_has_shipment_flag) AS cancelled_has_shipment
FROM fact_orders;

-- 5) Top customers by completed revenue
SELECT
  customer_id,
  ROUND(SUM(order_amount), 2) AS revenue_completed
FROM fact_orders
WHERE order_status = 'completed'
GROUP BY customer_id
ORDER BY revenue_completed DESC
LIMIT 10;

-- 6) Lead time from payment to shipment (minutes), completed orders only
SELECT
  ROUND(AVG(DATEDIFF('minute', payment_event_ts, shipped_event_ts)), 2) AS avg_minutes_payment_to_ship,
  MIN(DATEDIFF('minute', payment_event_ts, shipped_event_ts)) AS min_minutes_payment_to_ship,
  MAX(DATEDIFF('minute', payment_event_ts, shipped_event_ts)) AS max_minutes_payment_to_ship
FROM fact_orders
WHERE order_status = 'completed'
  AND payment_event_ts IS NOT NULL
  AND shipped_event_ts IS NOT NULL;

-- 7) Orders without any events (cross-source consistency check)
SELECT
  COUNT(*) AS orders_without_events
FROM fact_orders
WHERE created_event_ts IS NULL
  AND payment_event_ts IS NULL
  AND shipped_event_ts IS NULL
  AND cancelled_event_ts IS NULL;

-- 8) Event coverage by type (how often each event exists per order)
SELECT
  ROUND(100.0 * SUM(CASE WHEN created_event_ts IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_with_created_event,
  ROUND(100.0 * SUM(CASE WHEN payment_event_ts IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_with_payment_event,
  ROUND(100.0 * SUM(CASE WHEN shipped_event_ts IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_with_shipped_event,
  ROUND(100.0 * SUM(CASE WHEN cancelled_event_ts IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_with_cancelled_event
FROM fact_orders;
