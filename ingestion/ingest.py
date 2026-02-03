from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd


# =============================================================================
# Polut ja perusasetukset
# =============================================================================

# Projektin juurihakemisto (olettaen että tämä tiedosto on esim. ingestion/ingest.py)
BASE_DIR = Path(__file__).resolve().parents[1]

# Raw-layer: tänne kirjoitetaan lähdedata (CSV)
DATA_RAW = BASE_DIR / "data" / "raw"

# Processed-layer: tänne kirjoitetaan datalaaturaportit sekä mahdollinen DuckDB
DATA_PROCESSED = BASE_DIR / "data" / "processed"

# DuckDB warehouse -tietokannan oletuspolku
WAREHOUSE_DB = DATA_PROCESSED / "warehouse.duckdb"

# Sallitut tapahtumatyypit ja tilausstatukset (käytetään allowed-values -säännöissä)
ALLOWED_EVENT_TYPES = {"order_created", "payment_confirmed", "order_shipped", "order_cancelled"}
ALLOWED_ORDER_STATUS = {"completed", "cancelled", "refunded"}


# =============================================================================
# Datalaaturaportin tietomalli
# =============================================================================

@dataclass
class RuleResult:
    """Yhden data quality -säännön ajon tulos (yhteenveto)."""
    rule_id: str
    rule_name: str
    table_name: str
    severity: str              # esim. "critical" tai "warning"
    failed_rows: int
    total_rows: int
    failure_rate: float
    sample_keys: str           # pieni otos avaimista nopeaan debuggaamiseen


# =============================================================================
# I/O- ja apufunktiot
# =============================================================================

def ensure_dirs() -> None:
    """Varmistaa, että tarvittavat hakemistot ovat olemassa."""
    DATA_RAW.mkdir(parents=True, exist_ok=True)
    DATA_PROCESSED.mkdir(parents=True, exist_ok=True)


def write_raw_data(orders: pd.DataFrame, events: pd.DataFrame) -> None:
    """
    Kirjoittaa raakadatan CSV-muodossa raw-kansioon.

    Huom:
    - Tämä vaihe toimii "ingestion" / "landing zone" -tyyppisenä askeleena.
    - Prosessoitu/raportoitu data tallennetaan erikseen processed-kansioon.
    """
    orders.to_csv(DATA_RAW / "orders.csv", index=False)
    events.to_csv(DATA_RAW / "order_events.csv", index=False)


def parse_timestamps(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    """
    Parsii annetut sarakkeet datetime-muotoon UTC-aikavyöhykkeellä.

    errors="coerce":
    - Jos arvoa ei pystytä parsimaan, siitä tulee NaT (puuttuva datetime).
    - Tämä on hyödyllinen, koska not-null -säännöt voivat napata nämä rivit.
    """
    for c in cols:
        df[c] = pd.to_datetime(df[c], utc=True, errors="coerce")
    return df


def sample_keys_from_df(df: pd.DataFrame, key_cols: List[str], max_items: int = 5) -> str:
    """
    Palauttaa lyhyen näytteen avainarvoista raporttia varten.
    Muoto: "key1; key2; key3" tai monisarakkeisena "k1,k2; k1,k2; ...".
    """
    if df.empty:
        return ""
    samples = df[key_cols].drop_duplicates().head(max_items)
    return "; ".join([",".join(map(str, row)) for row in samples.to_numpy()])


# =============================================================================
# Synteettisen datan generointi (demo- / testidataa varten)
# =============================================================================

def generate_synthetic_orders(n: int = 5000, seed: int = 42) -> pd.DataFrame:
    """
    Generoi synteettisen orders-datan.

    Rakenteellinen idea:
    - Tilaukset syntyvät tasaisesti 60 päivän ajalta
    - Status on oletuksena "completed", ja sen jälkeen lisätään poikkeuksia:
      * osa perutaan
      * osa palautetaan
      * osa saa negatiivisen summan (DQ-case)
      * osalta puuttuu customer_id (DQ-case)

    seed:
    - Määrittää satunnaisuuden, jotta ajot ovat toistettavia.
    """
    rng = pd.Series(range(n))
    base_time = datetime.now(timezone.utc) - timedelta(days=60)

    orders = pd.DataFrame(
        {
            "order_id": [f"O{100000 + i}" for i in range(n)],
            "customer_id": [f"C{10000 + (i % 800)}" for i in range(n)],
            "order_created_at": [base_time + timedelta(minutes=int(i * 7)) for i in rng],
            "order_amount": (rng % 200) * 1.5,
            "currency": ["EUR"] * n,
            "order_status": ["completed"] * n,
        }
    )

    # 1) Osa perutaan
    cancelled_idx = orders.sample(frac=0.08, random_state=seed).index
    orders.loc[cancelled_idx, "order_status"] = "cancelled"

    # 2) Osa palautetaan (ei peruttujen joukosta)
    refunded_idx = orders.drop(cancelled_idx).sample(frac=0.03, random_state=seed + 1).index
    orders.loc[refunded_idx, "order_status"] = "refunded"

    # 3) Pieni osa saa negatiivisen summan (DQ-case)
    bad_amount_idx = orders.sample(frac=0.01, random_state=seed + 2).index
    orders.loc[bad_amount_idx, "order_amount"] = -10.0

    # 4) Pieni osa puuttuu customer_id (DQ-case)
    bad_customer_idx = orders.sample(frac=0.005, random_state=seed + 3).index
    orders.loc[bad_customer_idx, "customer_id"] = None

    return orders


def generate_synthetic_events(orders: pd.DataFrame, seed: int = 42) -> pd.DataFrame:
    """
    Generoi synteettisen order_events-datan orders-taulun pohjalta.

    Logiikka:
    - Jokaiselle tilaukselle syntyy vähintään order_created
    - completed/refunded -> yleensä payment_confirmed (joskus puuttuu DQ-casena)
    - completed -> order_shipped
    - cancelled -> order_cancelled (joskus myös virheellinen order_shipped DQ-casena)

    Lisäksi lisätään pari tarkoituksellista ongelmaa:
    - event, jolla ei ole vastaavaa order-riviä
    - duplikaatti event_id
    """
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
            # Joskus puuttuu payment_confirmed (DQ-case)
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

        # shipment completed-tilauksille
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

        # cancelled-tilauksille order_cancelled
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

            # Joskus virheellisesti shipment peruutuksen jälkeen (DQ-case)
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

    # Lisää event ilman vastaavaa order-riviä (DQ-case)
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

    # Lisää duplikaatti event_id (DQ-case)
    if len(events_df) > 10:
        events_df.loc[5, "event_id"] = events_df.loc[4, "event_id"]

    return events_df


# =============================================================================
# Data quality -säännöt (palauttaa sekä yhteenvedon että rikkovat rivit)
# =============================================================================

def rule_duplicate_pk(
    df: pd.DataFrame,
    table: str,
    pk: str,
    rule_id: str,
    severity: str
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
    severity: str
) -> Tuple[RuleResult, pd.DataFrame]:
    total = len(df)
    mask = df[cols].isna().any(axis=1)
    bad = df[mask].copy()
    failed = len(bad)

    # Sample-avaimet: suositaan order_id jos löytyy, muuten ensimmäinen sarake
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
    allowed: set,
    rule_id: str,
    severity: str
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
    df: pd.DataFrame,
    rule_id: str,
    severity: str
) -> Tuple[RuleResult, pd.DataFrame]:
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


def rule_orders_without_events(
    orders: pd.DataFrame,
    events: pd.DataFrame,
    rule_id: str,
    severity: str
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
    severity: str
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
    severity: str
) -> Tuple[RuleResult, pd.DataFrame]:
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


# =============================================================================
# Raportin muodostus (orchestrator)
# =============================================================================

def build_quality_report(orders: pd.DataFrame, events: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Ajaa DQ-säännöt ja palauttaa:
    - report_df: yhteenvedot säännöittäin
    - samples_df: esimerkkirivit rikkovista havainnoista (max 50 per sääntö)
    """
    results: List[RuleResult] = []
    failed_samples: List[pd.DataFrame] = []

    def add(res: RuleResult, bad_df: pd.DataFrame, sample_cols: List[str], rule_id: str) -> None:
        results.append(res)
        if not bad_df.empty:
            sample = bad_df[sample_cols].head(50).copy()
            sample.insert(0, "rule_id", rule_id)
            failed_samples.append(sample)

    # Tehdään kopiot, jotta alkuperäisiä DataFrameja ei muokata kutsujan näkökulmasta
    orders = orders.copy()
    events = events.copy()

    # Parsitaan timestampit aikaisin, jotta downstream-säännöt toimivat tyyppiturvallisesti
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


# =============================================================================
# DuckDB-lataus (raw + DQ outputs -> warehouse.duckdb)
# =============================================================================

def load_to_duckdb(
    db_path: Path,
    orders: pd.DataFrame,
    events: pd.DataFrame,
    report_df: pd.DataFrame,
    samples_df: pd.DataFrame,
    overwrite_tables: bool = True
) -> None:
    """
    Lataa DataFramet DuckDB:hen.

    Taulut:
    - raw_orders
    - raw_order_events
    - dq_report
    - dq_failed_samples

    overwrite_tables:
    - True: pudottaa ja luo taulut uudelleen (hyvä demo- ja kehityskäyttöön)
    - False: yrittää luoda vain jos puuttuu (ei käsittele päivityksiä)
    """
    try:
        import duckdb  # type: ignore
    except ImportError as e:
        raise RuntimeError(
            "duckdb-kirjasto puuttuu. Asenna se: pip install duckdb"
        ) from e

    db_path.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(db_path))
    try:
        # Rekisteröidään DataFramet väliaikaisiksi vieweiksi DuckDB:ssä
        con.register("orders_df", orders)
        con.register("events_df", events)
        con.register("report_df", report_df)
        con.register("samples_df", samples_df)

        if overwrite_tables:
            con.execute("DROP TABLE IF EXISTS raw_orders;")
            con.execute("DROP TABLE IF EXISTS raw_order_events;")
            con.execute("DROP TABLE IF EXISTS dq_report;")
            con.execute("DROP TABLE IF EXISTS dq_failed_samples;")

        # Luodaan taulut DataFrame-vieweistä
        con.execute("CREATE TABLE IF NOT EXISTS raw_orders AS SELECT * FROM orders_df;")
        con.execute("CREATE TABLE IF NOT EXISTS raw_order_events AS SELECT * FROM events_df;")
        con.execute("CREATE TABLE IF NOT EXISTS dq_report AS SELECT * FROM report_df;")
        con.execute("CREATE TABLE IF NOT EXISTS dq_failed_samples AS SELECT * FROM samples_df;")
    finally:
        con.close()


# =============================================================================
# CLI + ajologiikka
# =============================================================================

def parse_args(argv: List[str]) -> argparse.Namespace:
    """
    CLI-argumentit tekevät skriptistä joustavan:
    - datasetin koko
    - seed (toistettavuus)
    - mode (dev/prod)
    - halutaanko ladata DuckDB:hen
    """
    parser = argparse.ArgumentParser(
        description="Generate synthetic orders + events, run data quality checks, and optionally load outputs to DuckDB."
    )

    parser.add_argument("--n", type=int, default=5000, help="Tilauksien määrä (oletus: 5000).")
    parser.add_argument("--seed", type=int, default=42, help="Satunnaissiementä toistettavuuteen (oletus: 42).")

    parser.add_argument(
        "--mode",
        choices=["dev", "prod"],
        default=os.environ.get("MODE", "dev"),
        help="Ajotila. prod-tilassa kriittiset DQ-virheet kaatavat ajon (exit 1). (oletus: dev)"
    )

    parser.add_argument(
        "--load-duckdb",
        action="store_true",
        help="Lataa raakadatan ja DQ-raportit DuckDB:hen (warehouse.duckdb)."
    )

    parser.add_argument(
        "--duckdb-path",
        type=str,
        default=str(WAREHOUSE_DB),
        help=f"DuckDB-tiedoston polku (oletus: {WAREHOUSE_DB})."
    )

    parser.add_argument(
        "--no-overwrite-tables",
        action="store_true",
        help="Älä pudota ja luo DuckDB-tauluja uudelleen (oletus: taulut ylikirjoitetaan)."
    )

    return parser.parse_args(argv)


def should_fail_run_in_prod(report_df: pd.DataFrame) -> bool:
    """
    Palauttaa True, jos raportissa on prod-tilassa ajon kaatavia virheitä.

    Tässä toteutuksessa:
    - severity == "critical" ja failed_rows > 0 => kaada ajo
    """
    if report_df.empty:
        return False

    crit = report_df[(report_df["severity"] == "critical") & (report_df["failed_rows"] > 0)]
    return len(crit) > 0


def main(argv: List[str] | None = None) -> None:
    args = parse_args(argv or sys.argv[1:])

    ensure_dirs()

    # 1) Generoidaan synteettinen data
    orders = generate_synthetic_orders(n=args.n, seed=args.seed)
    events = generate_synthetic_events(orders, seed=args.seed)

    # 2) Kirjoitetaan raw data levylle
    write_raw_data(orders, events)

    # 3) Ajetaan datalaatutarkistukset ja muodostetaan raportit
    report_df, samples_df = build_quality_report(orders, events)

    # 4) Tallennetaan raportit processed-kansioon
    report_path = DATA_PROCESSED / "data_quality_report.csv"
    samples_path = DATA_PROCESSED / "failed_samples.csv"
    report_df.to_csv(report_path, index=False)
    samples_df.to_csv(samples_path, index=False)

    # 5) Halutessa ladataan myös DuckDB:hen (warehouse)
    if args.load_duckdb:
        # Huom: build_quality_report parsii timestampit kopioihin, mutta raw-kirjoitus tehtiin
        # alkuperäisillä DF:illä. DuckDB:hen kannattaa usein viedä tyypitetty data, joten parsitaan
        # timestampit ennen latausta.
        orders_typed = parse_timestamps(orders.copy(), ["order_created_at"])
        events_typed = parse_timestamps(events.copy(), ["event_timestamp"])

        load_to_duckdb(
            db_path=Path(args.duckdb_path),
            orders=orders_typed,
            events=events_typed,
            report_df=report_df,
            samples_df=samples_df,
            overwrite_tables=not args.no_overwrite_tables,
        )

    # 6) Tulostetaan polut käyttäjälle
    print("Generated raw data:")
    print(f"- {DATA_RAW / 'orders.csv'}")
    print(f"- {DATA_RAW / 'order_events.csv'}")

    print("Generated quality outputs:")
    print(f"- {report_path}")
    print(f"- {samples_path}")

    if args.load_duckdb:
        print("Loaded to DuckDB:")
        print(f"- {Path(args.duckdb_path)}")

    # 7) Prod-tilassa voidaan kaataa ajo, jos kriittisiä virheitä löytyi
    if args.mode == "prod" and should_fail_run_in_prod(report_df):
        # Tulostetaan kriittiset rivit lyhyesti, jotta syy näkyy ajolokeissa.
        crit = report_df[(report_df["severity"] == "critical") & (report_df["failed_rows"] > 0)]
        print("\nCRITICAL data quality failures detected (mode=prod). Failing the run.")
        print(crit[["rule_id", "rule_name", "table_name", "failed_rows", "failure_rate"]].to_string(index=False))
        sys.exit(1)


if __name__ == "__main__":
    main()
