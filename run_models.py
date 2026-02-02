from pathlib import Path
import duckdb

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "data" / "processed" / "warehouse.duckdb"

SQL_PATH = BASE_DIR / "transformations" / "models.sql"

def main() -> None:
    con = duckdb.connect(str(DB_PATH))

    sql = SQL_PATH.read_text(encoding="utf-8")
    con.execute(sql)

    print("Models built successfully.")
    con.close()

if __name__ == "__main__":
    main()
