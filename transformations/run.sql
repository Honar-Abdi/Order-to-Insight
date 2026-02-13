-- Ajaa transformations-kerroksen objektit järjestyksessä.

.read transformations/staging/stg_orders.sql
.read transformations/staging/stg_order_events.sql
.read transformations/intermediate/int_order_event_summary.sql
.read transformations/marts/fct_orders.sql
.read transformations/marts/fct_daily_revenue.sql
