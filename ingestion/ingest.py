"""
Tämä skripti hoitaa ingestion-vaiheen.

Mitä tässä tehdään:
1) Luetaan raw-kerroksen CSV:t (orders ja order_events) sellaisenaan.
2) Ajetaan data quality -tarkistukset ilman korjaavia muunnoksia.
3) Kirjoitetaan DQ-raportit processed-kerrokseen.
4) Halutessa ladataan raw + DQ-outputit DuckDB:hen mallinnusta varten.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import List, Tuple

import pandas as pd

from ingestion.dq_runner import run_quality_checks


BASE_DIR = Path(__file__).resolve().parents[1]
DATA_RAW = BASE_DIR / "data" / "raw"
DATA_PROCESSED = BASE_DIR / "data" / "processed"
WAREHOUSE_DB = DATA_PROCESSED / "warehouse.duckdb"


def ensure_dirs() -> None:
    DATA_RAW.mkdir(parents=True, exist_ok=True)
    DATA_PROCESSED.mkdir(parents=True, exist_ok=True)


def read_raw_data() -> Tuple[pd.DataFrame, pd.DataFrame]:
    orders_path = DATA_RAW / "orders.csv"
    events_path = DATA_RAW / "order_events.csv"

    if not orders_path.exists():
        raise FileNotFoundError(f"Puuttuu: {orders_path}")
    if not events_path.exists():
        raise FileNotFoundError(f"Puuttuu: {events_path}")

    orders = pd.read_csv(orders_path)
    events = pd.read_csv(events_path)
    return orders, events


def parse_timestamps(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    """DuckDB-latausta varten parsitaan aikaleimat tyypitetyiksi kopioon."""
    out = df.copy()
    for c in cols:
        out[c] = pd.to_datetime(out[c], utc=True, errors="coerce")
    return out


def load_to_duckdb(
    db_path: Path,
    orders_typed: pd.DataFrame,
    events_typed: pd.DataFrame,
    report_df: pd.DataFrame,
    samples_df: pd.DataFrame,
    overwrite_tables: bool = True,
) -> None:
    try:
        import duckdb  # type: ignore
    except ImportError as e:
        raise RuntimeError("duckdb-kirjasto puuttuu. Asenna se: pip install duckdb") from e

    db_path.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(db_path))
    try:
        con.register("orders_df", orders_typed)
        con.register("events_df", events_typed)
        con.register("report_df", report_df)
        con.register("samples_df", samples_df)

        if overwrite_tables:
            con.execute("DROP TABLE IF EXISTS raw_orders;")
            con.execute("DROP TABLE IF EXISTS raw_order_events;")
            con.execute("DROP TABLE IF EXISTS dq_report;")
            con.execute("DROP TABLE IF EXISTS dq_failed_samples;")

        con.execute("CREATE TABLE IF NOT EXISTS raw_orders AS SELECT * FROM orders_df;")
        con.execute("CREATE TABLE IF NOT EXISTS raw_order_events AS SELECT * FROM events_df;")
        con.execute("CREATE TABLE IF NOT EXISTS dq_report AS SELECT * FROM report_df;")
        con.execute("CREATE TABLE IF NOT EXISTS dq_failed_samples AS SELECT * FROM samples_df;")
    finally:
        con.close()


def parse_args(argv: List[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Read raw CSVs, run DQ checks, and optionally load to DuckDB.")

    p.add_argument(
        "--mode",
        choices=["dev", "prod"],
        default=os.environ.get("MODE", "dev"),
        help="prod-tilassa critical-säännöt kaatavat ajon.",
    )

    p.add_argument("--load-duckdb", action="store_true")
    p.add_argument("--duckdb-path", type=str, default=str(WAREHOUSE_DB))
    p.add_argument("--no-overwrite-tables", action="store_true")

    return p.parse_args(argv)


def should_fail_run_in_prod(report_df: pd.DataFrame) -> bool:
    if report_df.empty:
        return False
    crit = report_df[(report_df["severity"] == "critical") & (report_df["failed_rows"] > 0)]
    return len(crit) > 0


def main(argv: List[str] | None = None) -> None:
    args = parse_args(argv or sys.argv[1:])

    ensure_dirs()

    orders_raw, events_raw = read_raw_data()

    report_df, samples_df = run_quality_checks(orders_raw, events_raw)

    report_path = DATA_PROCESSED / "data_quality_report.csv"
    samples_path = DATA_PROCESSED / "failed_samples.csv"
    report_df.to_csv(report_path, index=False)
    samples_df.to_csv(samples_path, index=False)

    if args.load_duckdb:
        orders_typed = parse_timestamps(orders_raw, ["order_created_at"])
        events_typed = parse_timestamps(events_raw, ["event_timestamp"])

        # Clean-ajossa samples_df voi olla täysin tyhjä (ei sarakkeita), jolloin DuckDB register kaatuu.
        if samples_df.shape[1] == 0:
            samples_df = pd.DataFrame({"rule_id": pd.Series(dtype="string")})

        load_to_duckdb(
            db_path=Path(args.duckdb_path),
            orders_typed=orders_typed,
            events_typed=events_typed,
            report_df=report_df,
            samples_df=samples_df,
            overwrite_tables=not args.no_overwrite_tables,
        )

    print("Read raw data:")
    print(f"- {DATA_RAW / 'orders.csv'}")
    print(f"- {DATA_RAW / 'order_events.csv'}")

    print("Generated quality outputs:")
    print(f"- {report_path}")
    print(f"- {samples_path}")

    if args.load_duckdb:
        print("Loaded to DuckDB:")
        print(f"- {Path(args.duckdb_path)}")

    if args.mode == "prod" and should_fail_run_in_prod(report_df):
        crit = report_df[(report_df["severity"] == "critical") & (report_df["failed_rows"] > 0)]
        print("\nCRITICAL data quality failures detected (mode=prod). Failing the run.")
        print(crit[["rule_id", "rule_name", "table_name", "failed_rows", "failure_rate"]].to_string(index=False))
        sys.exit(1)


if __name__ == "__main__":
    main()
