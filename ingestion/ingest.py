from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple

import pandas as pd


BASE_DIR = Path(__file__).resolve().parents[1]
DATA_RAW = BASE_DIR / "data" / "raw"
DATA_PROCESSED = BASE_DIR / "data" / "processed"

ALLOWED_EVENT_TYPES = {"order_created", "payment_confirmed", "order_shipped", "order_cancelled"}
ALLOWED_ORDER_STATUS = {"completed", "cancelled", "refunded"}


@dataclass
class RuleResult:
    rule_id: str
    rule_name: str
    table_name: str
    severity: str
    failed_rows: int
    total_rows: int
    failure_rate: float
    sample_keys: str


def ensure_dirs() -> None:
    DATA_RAW.mkdir(parents=True, exist_ok=True)
    DATA_PROCESSED.mkdir(parents=True, exist_ok=True)


def generate_synthetic_orders(n: int = 5000, seed: int = 42) -> pd.DataFrame:
    rng = pd.Series(range(n))
    base_time = datetime.now(timezone.utc) - timedelta(days=60)

    orders = pd.DataFrame(
        {
            "order_id": [f"O{100000+i}" for i in range(n)],
            "customer_id": [f"C{10000+(i % 800)}" for i in range(n)],
            "order_created_at": [base_time + timedelta(minutes=int(i * 7)) for i in rng],
            "order_amount": (rng % 200) * 1.5,
            "currency": ["EUR"] * n,
            "order_status": ["completed"] * n,
        }
    )

    # Lisää realistisia poikkeuksia
    # 1) osa perutaan
    cancelled_idx = orders.sample(frac=0.08, random_state=seed).index
    orders.loc[cancelled_idx, "order_status"] = "cancelled"

    # 2) osa palautetaan
    refunded_idx = orders.drop(cancelled_idx).sample(frac=0.03, random_state=seed + 1).index
    orders.loc[refunded_idx, "order_status"] = "refunded"

    # 3) muutamalle negatiivinen summa (data quality -case)
    bad_amount_idx = orders.sample(frac=0.01, random_state=seed + 2).index
    orders.loc[bad_amount_idx, "order_amount"] = -10.0

    # 4) muutama puuttuva customer_id (data quality -case)
    bad_customer_idx = orders.sample(frac=0.005, random_state=seed + 3).index
    orders.loc[bad_customer_idx, "customer_id"] = None

    return orders


def generate_synthetic_events(orders: pd.DataFrame, seed: int = 42) -> pd.DataFrame:
    rnd = orders.sample(frac=1.0, random_state=seed).reset_index(drop=True)
    events: List[Dict[str, object]] = []
    event_counter = 1

    for _, row in rnd.iterrows():
        order_id = row["order_id"]
        created_at: datetime = row["order_created_at"]
        status = row["order_status"]

        source_system = "web" if int(order_id[1:]) % 2 == 0 else "mobile"

        # order_created
        events.append(
            {
                "event_id": f"E{event_counter:08d}",
                "order_id": order_id,
                "event_type": "order_created",
                "event_timestamp": created_at + timedelta(seconds=5),
                "source_system": source_system,
            }
        )
        event_counter += 1

        # completed/refunded -> payment_confirmed yleensä
        if status in {"completed", "refunded"}:
            # joskus puuttuu payment_confirmed (data quality -case)
            if int(order_id[1:]) % 37 != 0:
                events.append(
                    {
                        "event_id": f"E{event_counter:08d}",
                        "order_id": order_id,
                        "event_type": "payment_confirmed",
                        "event_timestamp": created_at + timedelta(minutes=3),
                        "source_system": "backend",
                    }
                )
                event_counter += 1

        # shipment completed tilauksille, mutta joskus myös väärin cancelled-tilauksille
        if status == "completed":
            events.append(
                {
                    "event_id": f"E{event_counter:08d}",
                    "order_id": order_id,
                    "event_type": "order_shipped",
                    "event_timestamp": created_at + timedelta(hours=4),
                    "source_system": "backend",
                }
            )
            event_counter += 1

        if status == "cancelled":
            events.append(
                {
                    "event_id": f"E{event_counter:08d}",
                    "order_id": order_id,
                    "event_type": "order_cancelled",
                    "event_timestamp": created_at + timedelta(minutes=10),
                    "source_system": "backend",
                }
            )
            event_counter += 1

            # joskus virheellisesti shipment peruutuksen jälkeen (data quality -case)
            if int(order_id[1:]) % 53 == 0:
                events.append(
                    {
                        "event_id": f"E{event_counter:08d}",
                        "order_id": order_id,
                        "event_type": "order_shipped",
                        "event_timestamp": created_at + timedelta(hours=6),
                        "source_system": "backend",
                    }
                )
                event_counter += 1

    events_df = pd.DataFrame(events)

    # Lisää muutama event ilman vastaavaa order-riviä (data quality -case)
    extra = pd.DataFrame(
        [
            {
                "event_id": f"E{event_counter:08d}",
                "order_id": "O999999",
                "event_type": "order_created",
                "event_timestamp": datetime.now(timezone.utc) - timedelta(days=1),
                "source_system": "web",
            }
        ]
    )
    events_df = pd.concat([events_df, extra], ignore_index=True)

    # Lisää duplikaatti event_id (data quality -case)
    if len(events_df) > 10:
        events_df.loc[5, "event_id"] = events_df.loc[4, "event_id"]

    return events_df


def write_raw_data(orders: pd.DataFrame, events: pd.DataFrame) -> None:
    orders.to_csv(DATA_RAW / "orders.csv", index=False)
    events.to_csv(DATA_RAW / "order_events.csv", index=False)


def parse_timestamps(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    for c in cols:
        df[c] = pd.to_datetime(df[c], utc=True, errors="coerce")
    return df


def sample_keys_from_df(df: pd.DataFrame, key_cols: List[str], max_items: int = 5) -> str:
    if df.empty:
        return ""
    samples = df[key_cols].drop_duplicates().head(max_items)
    return "; ".join([",".join(map(str, row)) for row in samples.to_numpy()])


def rule_duplicate_pk(df: pd.DataFrame, table: str, pk: str, rule_id: str, severity: str) -> Tuple[RuleResult, pd.DataFrame]:
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


def rule_not_null(df: pd.DataFrame, table: str, cols: List[str], rule_id: str, severity: str) -> Tuple[RuleResult, pd.DataFrame]:
    total = len(df)
    mask = df[cols].isna().any(axis=1)
    bad = df[mask].copy()
    failed = len(bad)
    res = RuleResult(
        rule_id=rule_id,
        rule_name=f"Missing required fields: {', '.join(cols)}",
        table_name=table,
        severity=severity,
        failed_rows=failed,
        total_rows=total,
        failure_rate=(failed / total) if total else 0.0,
        sample_keys=sample_keys_from_df(bad, ["order_id"] if "order_id" in bad.columns else [cols[0]]),
    )
    return res, bad


def rule_allowed_values(df: pd.DataFrame, table: str, col: str, allowed: set, rule_id: str, severity: str) -> Tuple[RuleResult, pd.DataFrame]:
    total = len(df)
    bad = df[~df[col].isin(allowed) | df[col].isna()].copy()
    failed = len(bad)
    res = RuleResult(
        rule_id=rule_id,
        rule_name=f"Invalid values in {col}",
        table_name=table,
        severity=severity,
        failed_rows=failed,
        total_rows=total,
        failure_rate=(failed / total) if total else 0.0,
        sample_keys=sample_keys_from_df(bad, ["order_id"] if "order_id" in bad.columns else [col]),
    )
    return res, bad


def rule_amount_non_negative(df: pd.DataFrame, rule_id: str, severity: str) -> Tuple[RuleResult, pd.DataFrame]:
    total = len(df)
    bad = df[(df["order_amount"].isna()) | (df["order_amount"] < 0)].copy()
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


def rule_orders_without_events(orders: pd.DataFrame, events: pd.DataFrame, rule_id: str, severity: str) -> Tuple[RuleResult, pd.DataFrame]:
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


def rule_events_without_orders(orders: pd.DataFrame, events: pd.DataFrame, rule_id: str, severity: str) -> Tuple[RuleResult, pd.DataFrame]:
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


def rule_completed_without_payment(orders: pd.DataFrame, events: pd.DataFrame, rule_id: str, severity: str) -> Tuple[RuleResult, pd.DataFrame]:
    completed = orders[orders["order_status"] == "completed"][["order_id"]].copy()
    total = len(completed)

    paid_orders = set(events.loc[events["event_type"] == "payment_confirmed", "order_id"].astype(str).unique())
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


def build_quality_report(orders: pd.DataFrame, events: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    results: List[RuleResult] = []
    failed_samples: List[pd.DataFrame] = []

    def add(res: RuleResult, bad_df: pd.DataFrame, sample_cols: List[str], rule_id: str) -> None:
        results.append(res)
        if not bad_df.empty:
            sample = bad_df[sample_cols].head(50).copy()
            sample.insert(0, "rule_id", rule_id)
            failed_samples.append(sample)

    # Parse timestamps early
    orders = orders.copy()
    events = events.copy()
    orders = parse_timestamps(orders, ["order_created_at"])
    events = parse_timestamps(events, ["event_timestamp"])

    r, bad = rule_duplicate_pk(events, "order_events", "event_id", "R001", "critical")
    add(r, bad, ["event_id", "order_id", "event_type", "event_timestamp"], "R001")

    r, bad = rule_duplicate_pk(orders, "orders", "order_id", "R002", "critical")
    add(r, bad, ["order_id", "customer_id", "order_status", "order_amount"], "R002")

    r, bad = rule_not_null(events, "order_events", ["event_id", "order_id", "event_type", "event_timestamp"], "R003", "critical")
    add(r, bad, ["event_id", "order_id", "event_type", "event_timestamp"], "R003")

    r, bad = rule_not_null(orders, "orders", ["order_id", "customer_id", "order_created_at", "order_amount", "order_status"], "R004", "critical")
    add(r, bad, ["order_id", "customer_id", "order_created_at", "order_amount", "order_status"], "R004")

    r, bad = rule_allowed_values(events, "order_events", "event_type", ALLOWED_EVENT_TYPES, "R005", "warning")
    add(r, bad, ["event_id", "order_id", "event_type"], "R005")

    r, bad = rule_allowed_values(orders, "orders", "order_status", ALLOWED_ORDER_STATUS, "R006", "warning")
    add(r, bad, ["order_id", "order_status"], "R006")

    r, bad = rule_amount_non_negative(orders, "R007", "warning")
    add(r, bad, ["order_id", "order_amount", "order_status"], "R007")

    r, bad = rule_orders_without_events(orders, events, "R008", "warning")
    add(r, bad, ["order_id", "order_status", "order_created_at"], "R008")

    r, bad = rule_events_without_orders(orders, events, "R009", "warning")
    add(r, bad, ["event_id", "order_id", "event_type", "event_timestamp"], "R009")

    r, bad = rule_completed_without_payment(orders, events, "R010", "warning")
    add(r, bad, ["order_id"], "R010")

    report_df = pd.DataFrame([r.__dict__ for r in results])
    samples_df = pd.concat(failed_samples, ignore_index=True) if failed_samples else pd.DataFrame()

    return report_df, samples_df


def main() -> None:
    ensure_dirs()

    orders = generate_synthetic_orders(n=5000, seed=42)
    events = generate_synthetic_events(orders, seed=42)

    write_raw_data(orders, events)

    report_df, samples_df = build_quality_report(orders, events)

    report_path = DATA_PROCESSED / "data_quality_report.csv"
    report_df.to_csv(report_path, index=False)

    samples_path = DATA_PROCESSED / "failed_samples.csv"
    samples_df.to_csv(samples_path, index=False)

    print("Generated raw data:")
    print(f"- {DATA_RAW / 'orders.csv'}")
    print(f"- {DATA_RAW / 'order_events.csv'}")

    print("Generated quality outputs:")
    print(f"- {report_path}")
    print(f"- {samples_path}")


if __name__ == "__main__":
    main()
