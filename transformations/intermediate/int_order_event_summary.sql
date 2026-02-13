-- Intermediate order_event_summary
-- Rakeisuus yksi rivi per order_id.
-- Koostaa event-tason datan tekniseksi yhteenvedoksi.

CREATE OR REPLACE TABLE int_order_event_summary AS
SELECT
  order_id,
  MIN(CASE WHEN event_type = 'order_created' THEN event_timestamp END) AS created_event_ts,
  MIN(CASE WHEN event_type = 'payment_confirmed' THEN event_timestamp END) AS payment_event_ts,
  MIN(CASE WHEN event_type = 'order_shipped' THEN event_timestamp END) AS shipped_event_ts,
  MIN(CASE WHEN event_type = 'order_cancelled' THEN event_timestamp END) AS cancelled_event_ts,
  COUNT(*) AS event_count,
  SUM(CASE WHEN event_type = 'payment_confirmed' THEN 1 ELSE 0 END) AS payment_event_count,
  SUM(CASE WHEN event_type = 'order_shipped' THEN 1 ELSE 0 END) AS shipped_event_count
FROM stg_order_events
GROUP BY order_id;
