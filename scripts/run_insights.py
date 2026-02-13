"""
Ajaa perusanalyysit marts-tauluista ja kirjoittaa tulokset tiedostoon.

Edellyttää, että pipeline on ajettu ja fct_orders löytyy warehouse.duckdb:stä.
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import duckdb


BASE_DIR = Path(__file__).resolve().parents[1]
WAREHOUSE_DB = BASE_DIR / "data" / "processed" / "warehouse.duckdb"
OUT_PATH = BASE_DIR / "data" / "processed" / "analysis_results.txt"


@dataclass(frozen=True)
class RunContext:
    dq_profile: str
    n: int
    seed: int
    mode: str


@dataclass(frozen=True)
class RevenueMetrics:
    completed_orders: int
    completed_revenue: float
    paid_orders: int
    paid_revenue: float
    revenue_gap: float
    revenue_gap_pct: float
    missing_payment_orders: int
    cancelled_with_shipment_orders: int


def parse_args(argv: list[str]) -> RunContext:
    p = argparse.ArgumentParser(
        description="Generate analysis results from DuckDB marts tables."
    )

    p.add_argument("--dq-profile", choices=["clean", "messy"], required=True)
    p.add_argument("--n", type=int, required=True)
    p.add_argument("--seed", type=int, required=True)
    p.add_argument("--mode", choices=["dev", "prod"], required=True)

    a = p.parse_args(argv)

    return RunContext(
        dq_profile=a.dq_profile,
        n=a.n,
        seed=a.seed,
        mode=a.mode,
    )


def fetch_revenue_metrics(con: duckdb.DuckDBPyConnection) -> RevenueMetrics:
    """
    Laskee erot completed vs paid -määritelmissä.

    paid tarkoittaa completed + has_payment_event = 1.
    """

    completed_orders, completed_revenue = con.execute(
        """
        SELECT
          COUNT(*)::BIGINT,
          COALESCE(SUM(order_amount), 0)::DOUBLE
        FROM fct_orders
        WHERE order_status = 'completed'
        """
    ).fetchone()

    paid_orders, paid_revenue = con.execute(
        """
        SELECT
          COUNT(*)::BIGINT,
          COALESCE(SUM(order_amount), 0)::DOUBLE
        FROM fct_orders
        WHERE order_status = 'completed'
          AND has_payment_event = 1
        """
    ).fetchone()

    missing_payment_orders, cancelled_with_shipment_orders = con.execute(
        """
        SELECT
          COALESCE(SUM(dq_completed_missing_payment_flag), 0)::BIGINT,
          COALESCE(SUM(dq_cancelled_has_shipment_flag), 0)::BIGINT
        FROM fct_orders
        """
    ).fetchone()

    revenue_gap = float(completed_revenue) - float(paid_revenue)
    revenue_gap_pct = (
        (revenue_gap / float(completed_revenue) * 100.0)
        if completed_revenue
        else 0.0
    )

    return RevenueMetrics(
        completed_orders=int(completed_orders),
        completed_revenue=float(completed_revenue),
        paid_orders=int(paid_orders),
        paid_revenue=float(paid_revenue),
        revenue_gap=revenue_gap,
        revenue_gap_pct=revenue_gap_pct,
        missing_payment_orders=int(missing_payment_orders),
        cancelled_with_shipment_orders=int(cancelled_with_shipment_orders),
    )


def build_interpretation(metrics: RevenueMetrics) -> list[str]:
    """
    Muodostaa lyhyen liiketoiminnallisen tulkinnan.
    """

    lines: list[str] = []

    if metrics.completed_revenue > 0:
        lines.append(
            f"- Completed revenue overstates paid revenue by "
            f"{metrics.revenue_gap_pct:.2f}% ({metrics.revenue_gap:.2f})."
        )
    else:
        lines.append("- Completed revenue is zero. No comparison available.")

    if metrics.missing_payment_orders > 0:
        lines.append(
            f"- {metrics.missing_payment_orders} completed orders are missing payment events."
        )
    else:
        lines.append("- No completed orders are missing payment events.")

    if metrics.cancelled_with_shipment_orders > 0:
        lines.append(
            f"- {metrics.cancelled_with_shipment_orders} cancelled orders have shipment events."
        )
    else:
        lines.append("- No cancelled orders have shipment events.")

    return lines


def write_report(ctx: RunContext, metrics: RevenueMetrics) -> None:
    """Kirjoittaa analyysiraportin tekstitiedostoon."""

    generated_at = datetime.now(timezone.utc).isoformat()
    interpretation_lines = build_interpretation(metrics)

    lines = [
        "ORDER TO INSIGHT RESULTS",
        f"Generated at (UTC): {generated_at}",
        "",
        "Run context",
        f"- dq_profile: {ctx.dq_profile}",
        f"- n: {ctx.n}",
        f"- seed: {ctx.seed}",
        f"- mode: {ctx.mode}",
        "",
        "Revenue definitions",
        "- completed_revenue: SUM(order_amount) where order_status = 'completed'",
        "- paid_revenue:      SUM(order_amount) where order_status = 'completed' AND has_payment_event = 1",
        "- revenue_gap:       completed_revenue - paid_revenue",
        "- revenue_gap_pct:   revenue_gap / completed_revenue",
        "",
        "Key metrics",
        f"- completed_orders: {metrics.completed_orders}",
        f"- completed_revenue: {metrics.completed_revenue:.2f}",
        f"- paid_orders: {metrics.paid_orders}",
        f"- paid_revenue: {metrics.paid_revenue:.2f}",
        "",
        "Data quality impact",
        f"- revenue_gap: {metrics.revenue_gap:.2f}",
        f"- revenue_gap_pct: {metrics.revenue_gap_pct:.2f}%",
        f"- dq_completed_missing_payment_flag (orders): {metrics.missing_payment_orders}",
        f"- dq_cancelled_has_shipment_flag (orders): {metrics.cancelled_with_shipment_orders}",
        "",
        "Business interpretation",
        *interpretation_lines,
        "",
    ]

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text("\n".join(lines), encoding="utf-8")


def main(argv: list[str] | None = None) -> None:
    ctx = parse_args(argv or sys.argv[1:])

    if not WAREHOUSE_DB.exists():
        raise FileNotFoundError(
            "warehouse.duckdb not found. Run the pipeline first."
        )

    con = duckdb.connect(str(WAREHOUSE_DB))
    try:
        con.execute("SELECT 1 FROM fct_orders LIMIT 1")
        metrics = fetch_revenue_metrics(con)
    finally:
        con.close()

    write_report(ctx, metrics)
    print(f"Wrote analysis results to: {OUT_PATH}")


if __name__ == "__main__":
    main()
