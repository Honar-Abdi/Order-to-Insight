-- Fact daily revenue
-- Rakeisuus yksi rivi per order_date.
-- Laskee completed-tilausten liikevaihdon ja tilausmäärät päivittäin.

CREATE OR REPLACE TABLE fct_daily_revenue AS
SELECT
  DATE(order_created_at) AS order_date,
  SUM(CASE WHEN order_status = 'completed' THEN order_amount ELSE 0 END) AS revenue_completed,
  COUNT(*) AS orders_total,
  SUM(CASE WHEN order_status = 'completed' THEN 1 ELSE 0 END) AS orders_completed
FROM fct_orders
GROUP BY DATE(order_created_at)
ORDER BY order_date;