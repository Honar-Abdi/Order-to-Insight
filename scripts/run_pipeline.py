"""
Ajaa koko pipeline-ketjun.

Vaiheet
1) Generoi synteettinen data (raw CSV)
2) Ajaa ingestion + DQ ja lataa DuckDB:hen
3) Ajaa transformations ja rakentaa marts-taulut
4) Ajaa analyysin ja kirjoittaa tulokset tiedostoon
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[1]


def run_cmd(cmd: list[str]) -> None:
    """
    Ajaa alikomennon projektin juuresta.

    sys.executable varmistaa, että käytössä on sama Python kuin aktiivisessa .venvissä.
    """
    res = subprocess.run(cmd, cwd=str(BASE_DIR))
    if res.returncode != 0:
        raise SystemExit(res.returncode)


def parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run full Order-to-Insight pipeline.")

    p.add_argument("--dq-profile", choices=["clean", "messy"], default="clean")
    p.add_argument("--n", type=int, default=5000)
    p.add_argument("--seed", type=int, default=42)

    p.add_argument(
        "--mode",
        choices=["dev", "prod"],
        default="dev",
        help="prod-tilassa ingestion kaataa ajon jos critical DQ-virheitä löytyy.",
    )

    p.add_argument("--skip-generate", action="store_true")
    p.add_argument("--skip-ingestion", action="store_true")
    p.add_argument("--skip-transformations", action="store_true")
    p.add_argument("--skip-insights", action="store_true")

    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv or sys.argv[1:])

    if not args.skip_generate:
        run_cmd(
            [
                sys.executable,
                "scripts/generate_synthetic_data.py",
                "--dq-profile",
                args.dq_profile,
                "--n",
                str(args.n),
                "--seed",
                str(args.seed),
            ]
        )

    if not args.skip_ingestion:
        run_cmd(
            [
                sys.executable,
                "-m",
                "ingestion.ingest",
                "--mode",
                args.mode,
                "--load-duckdb",
            ]
        )

    if not args.skip_transformations:
        run_cmd([sys.executable, "scripts/run_transformations.py"])

    if not args.skip_insights:
        run_cmd(
            [
                sys.executable,
                "scripts/run_insights.py",
                "--dq-profile",
                args.dq_profile,
                "--n",
                str(args.n),
                "--seed",
                str(args.seed),
                "--mode",
                args.mode,
            ]
        )

    print("Pipeline completed successfully.")


if __name__ == "__main__":
    main()
