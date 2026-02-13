from __future__ import annotations

from typing import List, Tuple

import pandas as pd

from ingestion.dq_rules import (
    RuleResult,
    rule_duplicate_pk,
    rule_not_null,
    rule_allowed_values,
    rule_amount_non_negative,
    rule_orders_without_events,
    rule_events_without_orders,
    rule_completed_without_payment,
    rule_timestamp_parseable,
    rule_event_not_before_order_created,
)

ALLOWED_EVENT_TYPES = {"order_created", "payment_confirmed", "order_shipped", "order_cancelled"}
ALLOWED_ORDER_STATUS = {"completed", "cancelled", "refunded"}


def run_quality_checks(orders_raw: pd.DataFrame, events_raw: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Ajaa DQ-säännöt raw-datan päälle.

    Dataa ei muokata pysyvästi:
    - timestamp-tyyppitarkistus tehdään parse-testinä
    - relaatiosäännöissä käytetään kopioita
    """
    results: List[RuleResult] = []
    failed_samples: List[pd.DataFrame] = []

    def add(res: RuleResult, bad_df: pd.DataFrame, sample_cols: List[str], rule_id: str) -> None:
        results.append(res)
        if not bad_df.empty:
            sample = bad_df[sample_cols].head(50).copy()
            sample.insert(0, "rule_id", rule_id)
            failed_samples.append(sample)

    r, bad = rule_duplicate_pk(events_raw, "order_events", "event_id", "R001", "critical")
    add(r, bad, ["event_id", "order_id", "event_type", "event_timestamp"], "R001")

    r, bad = rule_duplicate_pk(orders_raw, "orders", "order_id", "R002", "critical")
    add(r, bad, ["order_id", "customer_id", "order_status", "order_amount"], "R002")

    r, bad = rule_not_null(events_raw, "order_events", ["event_id", "order_id", "event_type", "event_timestamp"], "R003", "critical")
    add(r, bad, ["event_id", "order_id", "event_type", "event_timestamp"], "R003")

    r, bad = rule_not_null(orders_raw, "orders", ["order_id", "customer_id", "order_created_at", "order_amount", "order_status"], "R004", "critical")
    add(r, bad, ["order_id", "customer_id", "order_created_at", "order_amount", "order_status"], "R004")

    r, bad = rule_allowed_values(events_raw, "order_events", "event_type", ALLOWED_EVENT_TYPES, "R005", "warning")
    add(r, bad, ["event_id", "order_id", "event_type"], "R005")

    r, bad = rule_allowed_values(orders_raw, "orders", "order_status", ALLOWED_ORDER_STATUS, "R006", "warning")
    add(r, bad, ["order_id", "order_status"], "R006")

    r, bad = rule_amount_non_negative(orders_raw, "R007", "warning")
    add(r, bad, ["order_id", "order_amount", "order_status"], "R007")

    r, bad = rule_orders_without_events(orders_raw, events_raw, "R008", "warning")
    add(r, bad, ["order_id", "order_status", "order_created_at"], "R008")

    r, bad = rule_events_without_orders(orders_raw, events_raw, "R009", "warning")
    add(r, bad, ["event_id", "order_id", "event_type", "event_timestamp"], "R009")

    r, bad = rule_completed_without_payment(orders_raw, events_raw, "R010", "warning")
    add(r, bad, ["order_id"], "R010")

    r, bad = rule_timestamp_parseable(orders_raw, "orders", "order_created_at", ["order_id"], "R011", "critical")
    add(r, bad, ["order_id", "order_created_at"], "R011")

    r, bad = rule_timestamp_parseable(events_raw, "order_events", "event_timestamp", ["order_id", "event_id"], "R012", "critical")
    add(r, bad, ["event_id", "order_id", "event_timestamp"], "R012")

    r, bad = rule_event_not_before_order_created(orders_raw, events_raw, "R013", "warning")
    add(r, bad, ["event_id", "order_id", "event_type", "event_timestamp"], "R013")

    report_df = pd.DataFrame([r.__dict__ for r in results])
    samples_df = pd.concat(failed_samples, ignore_index=True) if failed_samples else pd.DataFrame()

    return report_df, samples_df
