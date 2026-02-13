from __future__ import annotations

from dataclasses import dataclass
from typing import List, Tuple, Set

import pandas as pd


@dataclass
class RuleResult:
    """Yhden data quality -säännön ajon tulos (yhteenveto)."""
    rule_id: str
    rule_name: str
    table_name: str
    severity: str
    failed_rows: int
    total_rows: int
    failure_rate: float
    sample_keys: str


def sample_keys_from_df(df: pd.DataFrame, key_cols: List[str], max_items: int = 5) -> str:
    """Lyhyt näyte avaimista raporttia varten."""
    if df.empty:
        return ""
    samples = df[key_cols].drop_duplicates().head(max_items)
    return "; ".join([",".join(map(str, row)) for row in samples.to_numpy()])


def rule_duplicate_pk(
    df: pd.DataFrame,
    table: str,
    pk: str,
    rule_id: str,
    severity: str,
) -> Tuple[RuleResult, pd.DataFrame]:
    total = len(df)
    dup = df[df.duplicated(subset=[pk], keep=False)].copy()
    failed = len(dup)

    res = RuleResult(
        rule_id=rule_id,
        rule_name=f"Duplicate primary key: {pk}",
        table_name=table,
        severity=severity,
        failed_rows=failed,
        total_rows=total,
        failure_rate=(failed / total) if total else 0.0,
        sample_keys=sample_keys_from_df(dup, [pk]),
    )
    return res, dup


def rule_not_null(
    df: pd.DataFrame,
    table: str,
    cols: List[str],
    rule_id: str,
    severity: str,
) -> Tuple[RuleResult, pd.DataFrame]:
    total = len(df)
    mask = df[cols].isna().any(axis=1)
    bad = df[mask].copy()
    failed = len(bad)

    sample_key_col = "order_id" if "order_id" in bad.columns else cols[0]

    res = RuleResult(
        rule_id=rule_id,
        rule_name=f"Missing required fields: {', '.join(cols)}",
        table_name=table,
        severity=severity,
        failed_rows=failed,
        total_rows=total,
        failure_rate=(failed / total) if total else 0.0,
        sample_keys=sample_keys_from_df(bad, [sample_key_col]),
    )
    return res, bad


def rule_allowed_values(
    df: pd.DataFrame,
    table: str,
    col: str,
    allowed: Set[str],
    rule_id: str,
    severity: str,
) -> Tuple[RuleResult, pd.DataFrame]:
    total = len(df)
    bad = df[~df[col].isin(allowed) | df[col].isna()].copy()
    failed = len(bad)

    sample_key_col = "order_id" if "order_id" in bad.columns else col

    res = RuleResult(
        rule_id=rule_id,
        rule_name=f"Invalid values in {col}",
        table_name=table,
        severity=severity,
        failed_rows=failed,
        total_rows=total,
        failure_rate=(failed / total) if total else 0.0,
        sample_keys=sample_keys_from_df(bad, [sample_key_col]),
    )
    return res, bad


def rule_amount_non_negative(
    orders: pd.DataFrame,
    rule_id: str,
    severity: str,
) -> Tuple[RuleResult, pd.DataFrame]:
    total = len(orders)
    bad = orders[(orders["order_amount"].isna()) | (orders["order_amount"] < 0)].copy()
    failed = len(bad)

    res = RuleResult(
        rule_id=rule_id,
        rule_name="Order amount must be >= 0",
        table_name="orders",
        severity=severity,
        failed_rows=failed,
        total_rows=total,
        failure_rate=(failed / total) if total else 0.0,
        sample_keys=sample_keys_from_df(bad, ["order_id"]),
    )
    return res, bad


def rule_orders_without_events(
    orders: pd.DataFrame,
    events: pd.DataFrame,
    rule_id: str,
    severity: str,
) -> Tuple[RuleResult, pd.DataFrame]:
    total = len(orders)
    event_order_ids = set(events["order_id"].dropna().astype(str).unique())
    bad = orders[~orders["order_id"].astype(str).isin(event_order_ids)].copy()
    failed = len(bad)

    res = RuleResult(
        rule_id=rule_id,
        rule_name="Orders without any events",
        table_name="orders",
        severity=severity,
        failed_rows=failed,
        total_rows=total,
        failure_rate=(failed / total) if total else 0.0,
        sample_keys=sample_keys_from_df(bad, ["order_id"]),
    )
    return res, bad


def rule_events_without_orders(
    orders: pd.DataFrame,
    events: pd.DataFrame,
    rule_id: str,
    severity: str,
) -> Tuple[RuleResult, pd.DataFrame]:
    total = len(events)
    order_ids = set(orders["order_id"].dropna().astype(str).unique())
    bad = events[~events["order_id"].astype(str).isin(order_ids)].copy()
    failed = len(bad)

    res = RuleResult(
        rule_id=rule_id,
        rule_name="Events without matching order",
        table_name="order_events",
        severity=severity,
        failed_rows=failed,
        total_rows=total,
        failure_rate=(failed / total) if total else 0.0,
        sample_keys=sample_keys_from_df(bad, ["order_id", "event_id"]),
    )
    return res, bad


def rule_completed_without_payment(
    orders: pd.DataFrame,
    events: pd.DataFrame,
    rule_id: str,
    severity: str,
) -> Tuple[RuleResult, pd.DataFrame]:
    completed = orders[orders["order_status"] == "completed"][["order_id"]].copy()
    total = len(completed)

    paid_orders = set(
        events.loc[events["event_type"] == "payment_confirmed", "order_id"]
        .astype(str)
        .unique()
    )
    bad = completed[~completed["order_id"].astype(str).isin(paid_orders)].copy()
    failed = len(bad)

    res = RuleResult(
        rule_id=rule_id,
        rule_name="Completed orders missing payment_confirmed event",
        table_name="orders",
        severity=severity,
        failed_rows=failed,
        total_rows=total,
        failure_rate=(failed / total) if total else 0.0,
        sample_keys=sample_keys_from_df(bad, ["order_id"]),
    )
    return res, bad


def rule_timestamp_parseable(
    df_raw: pd.DataFrame,
    table: str,
    col: str,
    key_cols: List[str],
    rule_id: str,
    severity: str,
) -> Tuple[RuleResult, pd.DataFrame]:
    """
    Tarkistaa, että timestamp-sarake on parsittavissa datetimeksi.

    CSV:ssä arvot tulevat yleensä merkkijonoina, joten tyyppitarkistus tarkoittaa käytännössä parse-testiä.
    """
    total = len(df_raw)

    s = df_raw[col]
    parsed = pd.to_datetime(s, utc=True, errors="coerce")
    bad_mask = s.isna() | (parsed.isna())

    bad = df_raw[bad_mask].copy()
    failed = len(bad)

    res = RuleResult(
        rule_id=rule_id,
        rule_name=f"Unparseable timestamp in {col}",
        table_name=table,
        severity=severity,
        failed_rows=failed,
        total_rows=total,
        failure_rate=(failed / total) if total else 0.0,
        sample_keys=sample_keys_from_df(bad, key_cols),
    )
    return res, bad


def rule_event_not_before_order_created(
    orders_raw: pd.DataFrame,
    events_raw: pd.DataFrame,
    rule_id: str,
    severity: str,
) -> Tuple[RuleResult, pd.DataFrame]:
    """
    Tarkistaa, ettei event_timestamp ole ennen order_created_at.

    Tässä ei korjata dataa, vaan parsitaan aikaleimat kopioihin ja raportoidaan poikkeamat.
    """
    total = len(events_raw)

    orders = orders_raw[["order_id", "order_created_at"]].copy()
    events = events_raw[["event_id", "order_id", "event_type", "event_timestamp"]].copy()

    orders["order_created_at_parsed"] = pd.to_datetime(orders["order_created_at"], utc=True, errors="coerce")
    events["event_timestamp_parsed"] = pd.to_datetime(events["event_timestamp"], utc=True, errors="coerce")

    joined = events.merge(orders, on="order_id", how="left")

    bad = joined[
        joined["order_created_at_parsed"].notna()
        & joined["event_timestamp_parsed"].notna()
        & (joined["event_timestamp_parsed"] < joined["order_created_at_parsed"])
    ].copy()

    failed = len(bad)

    res = RuleResult(
        rule_id=rule_id,
        rule_name="Event timestamp earlier than order_created_at",
        table_name="order_events",
        severity=severity,
        failed_rows=failed,
        total_rows=total,
        failure_rate=(failed / total) if total else 0.0,
        sample_keys=sample_keys_from_df(bad, ["order_id", "event_id"]),
    )
    return res, bad
