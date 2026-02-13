-- Fact orders
-- Rakeisuus yksi rivi per order_id.
-- Sisältää bisneslogiikkaa ja dq-flagit analytiikkaa varten.

CREATE OR REPLACE TABLE fct_orders AS
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
FROM stg_orders o
LEFT JOIN int_order_event_summary s
  ON o.order_id = s.order_id;
