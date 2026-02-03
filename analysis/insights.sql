-- Nämä kyselyt on tarkoitettu ajettavaksi tietokantaa vasten: data/processed/warehouse.duckdb
-- Käytettävät taulut: fact_orders, order_event_summary, fact_daily_revenue

-- 1) Kokonais-KPI-yhteenveto
SELECT
  COUNT(*) AS orders_total,                                   -- tilausten kokonaismäärä
  SUM(CASE WHEN order_status = 'completed' THEN 1 ELSE 0 END) AS orders_completed,
  SUM(CASE WHEN order_status = 'cancelled' THEN 1 ELSE 0 END) AS orders_cancelled,
  SUM(CASE WHEN order_status = 'refunded' THEN 1 ELSE 0 END) AS orders_refunded,
  ROUND(
    SUM(CASE WHEN order_status = 'completed' THEN order_amount ELSE 0 END),
    2
  ) AS revenue_completed                                      -- toteutunut liikevaihto
FROM fact_orders;

-- 2) Liikevaihdon kehitys ajan yli (päivittäin)
SELECT
  order_date,                                                 -- päivämäärä
  ROUND(revenue_completed, 2) AS revenue_completed,           -- toteutunut liikevaihto
  orders_total,                                               -- tilausten määrä
  orders_completed                                            -- toteutuneet tilaukset
FROM fact_daily_revenue
ORDER BY order_date;

-- 3) Keskimääräinen tilausarvo (vain toteutuneet tilaukset)
SELECT
  ROUND(AVG(order_amount), 2) AS avg_order_value_completed
FROM fact_orders
WHERE order_status = 'completed';

-- 4) Mallinnetun kerroksen datalaatu-liput
SELECT
  SUM(dq_completed_missing_payment_flag) AS completed_missing_payment,   -- toteutunut tilaus ilman maksutapahtumaa
  SUM(dq_cancelled_has_shipment_flag) AS cancelled_has_shipment           -- peruttu tilaus, jolla on toimitustapahtuma
FROM fact_orders;

-- 5) Parhaat asiakkaat toteutuneen liikevaihdon perusteella
SELECT
  customer_id,
  ROUND(SUM(order_amount), 2) AS revenue_completed
FROM fact_orders
WHERE order_status = 'completed'
GROUP BY customer_id
ORDER BY revenue_completed DESC
LIMIT 10;

-- 6) Läpimenoaika maksusta toimitukseen (minuuteissa), vain toteutuneet tilaukset
SELECT
  ROUND(
    AVG(DATEDIFF('minute', payment_event_ts, shipped_event_ts)),
    2
  ) AS avg_minutes_payment_to_ship,                            -- keskimääräinen aika
  MIN(DATEDIFF('minute', payment_event_ts, shipped_event_ts)) AS min_minutes_payment_to_ship,
  MAX(DATEDIFF('minute', payment_event_ts, shipped_event_ts)) AS max_minutes_payment_to_ship
FROM fact_orders
WHERE order_status = 'completed'
  AND payment_event_ts IS NOT NULL
  AND shipped_event_ts IS NOT NULL;

-- 7) Tilaukset, joilla ei ole yhtään tapahtumaa (lähteiden välinen eheystarkistus)
SELECT
  COUNT(*) AS orders_without_events
FROM fact_orders
WHERE created_event_ts IS NULL
  AND payment_event_ts IS NULL
  AND shipped_event_ts IS NULL
  AND cancelled_event_ts IS NULL;

-- 8) Tapahtumien kattavuus (% tilauksista, joilla tapahtuma löytyy)
SELECT
  ROUND(
    100.0 * SUM(CASE WHEN created_event_ts IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*),
    2
  ) AS pct_with_created_event,
  ROUND(
    100.0 * SUM(CASE WHEN payment_event_ts IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*),
    2
  ) AS pct_with_payment_event,
  ROUND(
    100.0 * SUM(CASE WHEN shipped_event_ts IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*),
    2
  ) AS pct_with_shipped_event,
  ROUND(
    100.0 * SUM(CASE WHEN cancelled_event_ts IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*),
    2
  ) AS pct_with_cancelled_event
FROM fact_orders;
