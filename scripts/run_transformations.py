"""
Ajaa transformations-kerroksen SQL-mallit DuckDB:hen.

Edellyttää, että ingestion on ladannut raw-taulut warehouse.duckdb:hen.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import duckdb


BASE_DIR = Path(__file__).resolve().parents[1]
WAREHOUSE_DB = BASE_DIR / "data" / "processed" / "warehouse.duckdb"
TRANSFORMATIONS_DIR = BASE_DIR / "transformations"


@dataclass(frozen=True)
class SqlStep:
    """Yksi ajettava SQL-tiedosto kiinteässä järjestyksessä."""
    name: str
    path: Path


STEPS: list[SqlStep] = [
    SqlStep("staging stg_orders", TRANSFORMATIONS_DIR / "staging" / "stg_orders.sql"),
    SqlStep("staging stg_order_events", TRANSFORMATIONS_DIR / "staging" / "stg_order_events.sql"),
    SqlStep("intermediate int_order_event_summary", TRANSFORMATIONS_DIR / "intermediate" / "int_order_event_summary.sql"),
    SqlStep("marts fct_orders", TRANSFORMATIONS_DIR / "marts" / "fct_orders.sql"),
    SqlStep("marts fct_daily_revenue", TRANSFORMATIONS_DIR / "marts" / "fct_daily_revenue.sql"),
]


def read_sql(path: Path) -> str:
    """Lukee SQL-tiedoston UTF-8 -koodauksella."""
    return path.read_text(encoding="utf-8")


def ensure_prerequisites() -> None:
    """Tarkistaa että warehouse ja kaikki SQL-tiedostot ovat olemassa."""
    if not WAREHOUSE_DB.exists():
        raise FileNotFoundError(
            "warehouse.duckdb not found. Run ingestion first with: python -m ingestion.ingest --load-duckdb"
        )

    missing = [s.path for s in STEPS if not s.path.exists()]
    if missing:
        joined = "\n- ".join(str(p) for p in missing)
        raise FileNotFoundError(f"Missing SQL files:\n- {joined}")


def run_steps(con: duckdb.DuckDBPyConnection) -> None:
    """
    Ajaa SQL-tiedostot määritellyssä järjestyksessä.

    Jos yksikin vaihe epäonnistuu, ajo keskeytetään ja ilmoitetaan missä tiedostossa virhe tapahtui.
    """
    for step in STEPS:
        try:
            sql = read_sql(step.path)
            con.execute(sql)
        except Exception as e:
            raise RuntimeError(
                f"Transformations failed at step '{step.name}' ({step.path})."
            ) from e


def main() -> None:
    ensure_prerequisites()

    try:
        con = duckdb.connect(str(WAREHOUSE_DB))
        try:
            run_steps(con)
        finally:
            con.close()
    except Exception as e:
        print(f"ERROR: {e}")
        raise SystemExit(1)

    print("Transformations completed successfully.")


if __name__ == "__main__":
    main()
