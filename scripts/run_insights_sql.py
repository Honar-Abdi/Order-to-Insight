"""
Ajaa analysis/insights.sql -kyselyt DuckDB:tä vasten ja lisää tulokset raporttiin.

Tämä skripti ei korvaa raporttia, vaan lisää tulokset analysis_results.txt:n loppuun.
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
INSIGHTS_SQL = BASE_DIR / "analysis" / "insights.sql"


@dataclass(frozen=True)
class RunContext:
    dq_profile: str
    n: int
    seed: int
    mode: str


@dataclass(frozen=True)
class SqlBlock:
    title: str
    sql: str


def parse_args(argv: list[str]) -> RunContext:
    p = argparse.ArgumentParser(
        description="Run analysis/insights.sql and append results to the report."
    )

    p.add_argument("--dq-profile", choices=["clean", "messy"], required=True)
    p.add_argument("--n", type=int, required=True)
    p.add_argument("--seed", type=int, required=True)
    p.add_argument("--mode", choices=["dev", "prod"], required=True)

    a = p.parse_args(argv)
    return RunContext(dq_profile=a.dq_profile, n=a.n, seed=a.seed, mode=a.mode)


def looks_like_block_title(line: str) -> bool:
    s = line.strip()
    if not s.startswith("--"):
        return False

    s = s[2:].strip()
    if ")" not in s:
        return False

    prefix = s.split(")", 1)[0].strip()
    return prefix.isdigit()


def extract_blocks(sql_text: str) -> list[SqlBlock]:
    """
    Pilkkoo insights.sql:n blokkeihin otsikkorivien perusteella.

    Oletus: jokainen otsikkoa seuraava query päättyy puolipisteeseen.
    """
    lines = sql_text.splitlines()

    blocks: list[SqlBlock] = []
    current_title: str | None = None
    current_sql_lines: list[str] = []

    def flush_if_ready() -> None:
        nonlocal current_title, current_sql_lines
        sql = "\n".join(current_sql_lines).strip()
        if current_title and sql:
            blocks.append(SqlBlock(title=current_title, sql=sql))
        current_title = None
        current_sql_lines = []

    for line in lines:
        if looks_like_block_title(line):
            flush_if_ready()
            current_title = line.strip()[2:].strip()
            continue

        if current_title is None:
            continue

        current_sql_lines.append(line)

        if ";" in line:
            flush_if_ready()

    flush_if_ready()
    return blocks


def format_df(df) -> str:
    """Muuttaa DataFrame-tuloksen tekstiksi raporttiin ja rajaa pitkät tulokset."""
    if df is None or df.shape[1] == 0:
        return "(no columns)"
    if df.shape[0] == 0:
        return "(no rows)"

    max_rows = 10
    if df.shape[0] > max_rows:
        preview = df.head(max_rows)
        return preview.to_string(index=False) + "\n... (truncated)"

    return df.to_string(index=False)


def append_report(ctx: RunContext, results: list[tuple[str, str]]) -> None:
    """Lisää SQL-tulokset olemassa olevan raportin loppuun."""
    generated_at = datetime.now(timezone.utc).isoformat()

    lines: list[str] = []
    lines.append("SQL INSIGHTS")
    lines.append(f"Generated at (UTC): {generated_at}")
    lines.append("")

    for title, payload in results:
        lines.append("=" * 60)
        lines.append(title)
        lines.append("-" * 60)
        lines.append(payload)
        lines.append("")

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    existing = OUT_PATH.read_text(encoding="utf-8") if OUT_PATH.exists() else ""
    separator = "\n" if existing and not existing.endswith("\n") else ""
    OUT_PATH.write_text(existing + separator + "\n".join(lines), encoding="utf-8")


def main(argv: list[str] | None = None) -> None:
    ctx = parse_args(argv or sys.argv[1:])

    if not WAREHOUSE_DB.exists():
        raise FileNotFoundError("warehouse.duckdb not found. Run the pipeline first.")

    if not INSIGHTS_SQL.exists():
        raise FileNotFoundError("analysis/insights.sql not found.")

    sql_text = INSIGHTS_SQL.read_text(encoding="utf-8")
    blocks = extract_blocks(sql_text)

    if not blocks:
        raise RuntimeError("No SQL blocks found in analysis/insights.sql.")

    con = duckdb.connect(str(WAREHOUSE_DB))
    try:
        con.execute("SELECT 1 FROM fct_orders LIMIT 1")

        rendered: list[tuple[str, str]] = []

        for b in blocks:
            try:
                df = con.execute(b.sql).fetchdf()
                rendered.append((b.title, format_df(df)))
            except Exception as e:
                rendered.append((b.title, f"ERROR: {e}"))
    finally:
        con.close()

    append_report(ctx, rendered)
    print(f"Appended SQL insights to: {OUT_PATH}")


if __name__ == "__main__":
    main()
