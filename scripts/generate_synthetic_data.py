"""
Tämä skripti generoi synteettisen orders- ja order_events -datan raw-kerrokseen.

Tarkoitus:
- Simuloida yksinkertaista tilaus- ja tapahtumamallia (order lifecycle).
- Mahdollistaa kaksi profiilia:
    * clean  -> data ilman tahallisia laatuvirheitä
    * messy  -> data, johon injektoidaan tyypillisiä data quality -ongelmia

Tämä tiedosto vastaa vain datan generoinnista.
Varsinainen ingestion (luku, validointi, DQ-raportointi, warehouse-lataus)
tehdään erillisessä ingestion/ingest.py -tiedostossa.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, List

import pandas as pd


BASE_DIR = Path(__file__).resolve().parents[1]
DATA_RAW = BASE_DIR / "data" / "raw"


@dataclass(frozen=True)
class DQProfile:
    """
    Profiili määrittää, injektoidaanko tarkoituksellisia laatuongelmia.

    clean  -> kaikki injektiot False
    messy  -> osa injektioista True
    """
    name: str
    inject_bad_amount: bool
    inject_missing_customer: bool
    inject_missing_payment: bool
    inject_orphan_event: bool
    inject_duplicate_event_id: bool
    inject_cancelled_then_shipped: bool


PROFILES: dict[str, DQProfile] = {
    "clean": DQProfile(
        name="clean",
        inject_bad_amount=False,
        inject_missing_customer=False,
        inject_missing_payment=False,
        inject_orphan_event=False,
        inject_duplicate_event_id=False,
        inject_cancelled_then_shipped=False,
    ),
    "messy": DQProfile(
        name="messy",
        inject_bad_amount=True,
        inject_missing_customer=True,
        inject_missing_payment=True,
        inject_orphan_event=True,
        inject_duplicate_event_id=True,
        inject_cancelled_then_shipped=True,
    ),
}


def ensure_dirs() -> None:
    """Varmistaa, että raw-kansio on olemassa."""
    DATA_RAW.mkdir(parents=True, exist_ok=True)


def write_raw_data(orders: pd.DataFrame, events: pd.DataFrame) -> None:
    """
    Kirjoittaa generoidun datan raw-kerrokseen.

    Raw-kerros toimii landing-zonena:
    data tallennetaan sellaisenaan ilman validointeja.
    """
    orders.to_csv(DATA_RAW / "orders.csv", index=False)
    events.to_csv(DATA_RAW / "order_events.csv", index=False)


def generate_synthetic_orders(n: int, seed: int, profile: DQProfile) -> pd.DataFrame:
    """
    Generoi orders-taulun.

    Rakenne:
    - order_id yksilöllinen
    - customer_id kiertää 800 asiakkaan joukossa
    - order_created_at jakautuu tasaisesti 60 päivän ajalle
    - order_status oletuksena completed, osa muutetaan cancelled/refunded

    profile ohjaa injektoidaanko tarkoituksellisia laatuvirheitä.
    """
    rng = pd.Series(range(n))
    base_time = datetime.now(timezone.utc) - timedelta(days=60)

    orders = pd.DataFrame(
        {
            "order_id": [f"O{100000 + i}" for i in range(n)],
            "customer_id": [f"C{10000 + (i % 800)}" for i in range(n)],
            "order_created_at": [
                base_time + timedelta(minutes=int(i * 7)) for i in rng
            ],
            "order_amount": (rng % 200) * 1.5,
            "currency": ["EUR"] * n,
            "order_status": ["completed"] * n,
        }
    )

    # 8 % tilauksista perutaan
    cancelled_idx = orders.sample(frac=0.08, random_state=seed).index
    orders.loc[cancelled_idx, "order_status"] = "cancelled"

    # 3 % (ei perutuista) palautetaan
    refunded_idx = (
        orders.drop(cancelled_idx)
        .sample(frac=0.03, random_state=seed + 1)
        .index
    )
    orders.loc[refunded_idx, "order_status"] = "refunded"

    # Negatiivinen summa simuloi virheellistä rahamäärää
    if profile.inject_bad_amount:
        bad_amount_idx = orders.sample(frac=0.01, random_state=seed + 2).index
        orders.loc[bad_amount_idx, "order_amount"] = -10.0

    # Puuttuva customer_id simuloi rikkinäistä lähdejärjestelmää
    if profile.inject_missing_customer:
        bad_customer_idx = orders.sample(frac=0.005, random_state=seed + 3).index
        orders.loc[bad_customer_idx, "customer_id"] = None

    return orders


def generate_synthetic_events(
    orders: pd.DataFrame, seed: int, profile: DQProfile
) -> pd.DataFrame:
    """
    Generoi order_events-taulun orders-taulun pohjalta.

    Event-logiikka:
    - Jokaiselle tilaukselle order_created
    - completed/refunded -> yleensä payment_confirmed
    - completed -> order_shipped
    - cancelled -> order_cancelled

    Profiili voi rikkoa logiikkaa tarkoituksella.
    """
    rnd = orders.sample(frac=1.0, random_state=seed).reset_index(drop=True)
    events: List[Dict[str, object]] = []
    event_counter = 1

    for _, row in rnd.iterrows():
        order_id = row["order_id"]
        created_at: datetime = row["order_created_at"]
        status = row["order_status"]

        source_system = "web" if int(order_id[1:]) % 2 == 0 else "mobile"

        # order_created syntyy aina
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

        # completed/refunded -> payment_confirmed, ellei profiili riko tätä
        if status in {"completed", "refunded"}:
            missing_payment = (
                profile.inject_missing_payment
                and (int(order_id[1:]) % 37 == 0)
            )

            if not missing_payment:
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

            # Simuloi virheellistä event-sekvenssiä: shipment peruutuksen jälkeen
            if (
                profile.inject_cancelled_then_shipped
                and (int(order_id[1:]) % 53 == 0)
            ):
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

    # Orpo event, jolla ei ole vastaavaa orderia
    if profile.inject_orphan_event:
        extra = pd.DataFrame(
            [
                {
                    "event_id": f"E{event_counter:08d}",
                    "order_id": "O999999",
                    "event_type": "order_created",
                    "event_timestamp": datetime.now(timezone.utc)
                    - timedelta(days=1),
                    "source_system": "web",
                }
            ]
        )
        events_df = pd.concat([events_df, extra], ignore_index=True)

    # Duplikaatti primary key simuloi vakavaa integraatiovirhettä
    if profile.inject_duplicate_event_id and len(events_df) > 10:
        events_df.loc[5, "event_id"] = events_df.loc[4, "event_id"]

    return events_df


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Generate synthetic raw orders and order_events CSV files."
    )
    p.add_argument("--n", type=int, default=5000)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--dq-profile", choices=["clean", "messy"], default="messy")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    ensure_dirs()

    profile = PROFILES[args.dq_profile]

    orders = generate_synthetic_orders(
        n=args.n,
        seed=args.seed,
        profile=profile,
    )

    events = generate_synthetic_events(
        orders=orders,
        seed=args.seed,
        profile=profile,
    )

    write_raw_data(orders, events)

    print("Generated raw data:")
    print(f"- {DATA_RAW / 'orders.csv'}")
    print(f"- {DATA_RAW / 'order_events.csv'}")
    print(f"Profile: {profile.name}")


if __name__ == "__main__":
    main()
