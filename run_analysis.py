from pathlib import Path
import duckdb

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "data" / "processed" / "warehouse.duckdb"
SQL_PATH = BASE_DIR / "analysis" / "insights.sql"
OUT_PATH = BASE_DIR / "data" / "processed" / "analysis_results.txt"


def strip_sql_comments(sql_text: str) -> str:
    lines = []
    for line in sql_text.splitlines():
        stripped = line.strip()
        if stripped.startswith("--"):
            continue
        lines.append(line)
    return "\n".join(lines)


def main() -> None:
    con = duckdb.connect(str(DB_PATH))

    sql_text = SQL_PATH.read_text(encoding="utf-8")
    sql_text = strip_sql_comments(sql_text)

    statements = [s.strip() for s in sql_text.split(";") if s.strip()]

    lines = []
    for i, stmt in enumerate(statements, start=1):
        lines.append(f"Query {i}")
        lines.append("")
        lines.append(stmt)
        lines.append("")

        try:
            df = con.execute(stmt).df()
            lines.append(df.to_string(index=False))
        except Exception as e:
            lines.append(f"ERROR: {e}")

        lines.append("")
        lines.append("------------------------------------------------------------")
        lines.append("")

    OUT_PATH.write_text("\n".join(lines), encoding="utf-8")
    print(f"Analysis results written to: {OUT_PATH}")

    con.close()


if __name__ == "__main__":
    main()
