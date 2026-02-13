-- Staging order_events
-- Valitaan tarvittavat kentät ja muunnetaan tietotyypit analytiikkaa varten.
-- Ei sisällä bisneslogiikkaa.

CREATE OR REPLACE VIEW stg_order_events AS
SELECT
  event_id,
  order_id,
  event_type,
  CAST(event_timestamp AS TIMESTAMP) AS event_timestamp,
  source_system
FROM raw_order_events;
