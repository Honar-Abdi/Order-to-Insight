-- Kyselyt on tarkoitettu ajettavaksi DuckDB-tietokantaa vasten: data/processed/warehouse.duckdb
-- Käytettävät taulut: fct_orders, fct_daily_revenue


-- 1) Keskeinen insight: completed vs paid -liikevaihto ja ero
SELECT
  COUNT(*) AS orders_total,
  SUM(CASE WHEN order_status = 'completed' THEN 1 ELSE 0 END) AS orders_completed,
  SUM(CASE WHEN order_status = 'completed' AND has_payment_event = 1 THEN 1 ELSE 0 END) AS orders_paid,
  ROUND(SUM(CASE WHEN order_status = 'completed' THEN order_amount ELSE 0 END), 2) AS completed_revenue,
  ROUND(SUM(CASE WHEN order_status = 'completed' AND has_payment_event = 1 THEN order_amount ELSE 0 END), 2) AS paid_revenue,
  ROUND(
    SUM(CASE WHEN order_status = 'completed' THEN order_amount ELSE 0 END)
    - SUM(CASE WHEN order_status = 'completed' AND has_payment_event = 1 THEN order_amount ELSE 0 END),
    2
  ) AS revenue_gap,
  ROUND(
    100.0
    * (
      SUM(CASE WHEN order_status = 'completed' THEN order_amount ELSE 0 END)
      - SUM(CASE WHEN order_status = 'completed' AND has_payment_event = 1 THEN order_amount ELSE 0 END)
    )
    / NULLIF(SUM(CASE WHEN order_status = 'completed' THEN order_amount ELSE 0 END), 0),
    2
  ) AS revenue_gap_pct
FROM fct_orders;


-- 2) DQ-liput fact-taulussa (ristiriidat näkyviin analyysiin)
SELECT
  SUM(dq_completed_missing_payment_flag) AS completed_missing_payment_orders,
  SUM(dq_cancelled_has_shipment_flag) AS cancelled_has_shipment_orders
FROM fct_orders;


-- 3) KPI-yhteenveto: statusjakauma ja completed-liikevaihto
SELECT
  COUNT(*) AS orders_total,
  SUM(CASE WHEN order_status = 'completed' THEN 1 ELSE 0 END) AS orders_completed,
  SUM(CASE WHEN order_status = 'cancelled' THEN 1 ELSE 0 END) AS orders_cancelled,
  SUM(CASE WHEN order_status = 'refunded' THEN 1 ELSE 0 END) AS orders_refunded,
  ROUND(SUM(CASE WHEN order_status = 'completed' THEN order_amount ELSE 0 END), 2) AS revenue_completed
FROM fct_orders;


-- 4) Liikevaihdon kehitys ajan yli (päivittäin)
SELECT
  order_date,
  ROUND(revenue_completed, 2) AS revenue_completed,
  orders_total,
  orders_completed
FROM fct_daily_revenue
ORDER BY order_date;


-- 5) Keskimääräinen tilausarvo (vain completed)
SELECT
  ROUND(AVG(order_amount), 2) AS avg_order_value_completed
FROM fct_orders
WHERE order_status = 'completed';


-- 6) Top asiakkaat completed-liikevaihdon perusteella
-- NULL customer_id esitetään arvolla 'UNKNOWN'
SELECT
  COALESCE(customer_id, 'UNKNOWN') AS customer_id,
  ROUND(SUM(order_amount), 2) AS revenue_completed
FROM fct_orders
WHERE order_status = 'completed'
GROUP BY COALESCE(customer_id, 'UNKNOWN')
ORDER BY revenue_completed DESC
LIMIT 10;


-- 7) Läpimenoaika maksusta toimitukseen (minuuteissa), vain validit rivit
SELECT
  ROUND(AVG(DATEDIFF('minute', payment_event_ts, shipped_event_ts)), 2) AS avg_minutes_payment_to_ship,
  MIN(DATEDIFF('minute', payment_event_ts, shipped_event_ts)) AS min_minutes_payment_to_ship,
  MAX(DATEDIFF('minute', payment_event_ts, shipped_event_ts)) AS max_minutes_payment_to_ship
FROM fct_orders
WHERE order_status = 'completed'
  AND has_payment_event = 1
  AND has_shipped_event = 1
  AND DATEDIFF('minute', payment_event_ts, shipped_event_ts) >= 0;


-- 8) Tapahtumien kattavuus (% tilauksista, joilla tapahtuma löytyy)
SELECT
  ROUND(100.0 * SUM(CASE WHEN created_event_ts IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_with_created_event,
  ROUND(100.0 * SUM(CASE WHEN payment_event_ts IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_with_payment_event,
  ROUND(100.0 * SUM(CASE WHEN shipped_event_ts IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_with_shipped_event,
  ROUND(100.0 * SUM(CASE WHEN cancelled_event_ts IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_with_cancelled_event
FROM fct_orders;


-- 9) Tilaukset ilman tapahtumia (lähteiden välinen eheystarkistus)
SELECT
  COUNT(*) AS orders_without_events
FROM fct_orders
WHERE created_event_ts IS NULL
  AND payment_event_ts IS NULL
  AND shipped_event_ts IS NULL
  AND cancelled_event_ts IS NULL;
