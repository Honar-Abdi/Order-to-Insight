-- Staging orders
-- Valitaan tarvittavat kentät ja muunnetaan tietotyypit analytiikkaa varten.
-- Ei sisällä bisneslogiikkaa.

CREATE OR REPLACE VIEW stg_orders AS
SELECT
  order_id,
  customer_id,
  CAST(order_created_at AS TIMESTAMP) AS order_created_at,
  CAST(order_amount AS DOUBLE) AS order_amount,
  currency,
  order_status
FROM raw_orders;
