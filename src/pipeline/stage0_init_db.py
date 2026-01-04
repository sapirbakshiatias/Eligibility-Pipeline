from pathlib import Path
import sqlite3

def init_db(root: Path) -> Path:
    """
    Stage 1: Create a local SQLite warehouse and initialize RAW STAGING tables.
    """

    # 1) Build project paths from the project root
    output_dir = root / "output"
    sql_dir = root / "sql"

    db_path = output_dir / "warehouse.db"

    ddl_paths = [
        sql_dir / "01_create_raw_staging.sql",
        sql_dir / "02_create_raw_staging_payload.sql",
        sql_dir / "03_create_silver_members.sql",
    ]
    # 2) Ensure output/ exists
    output_dir.mkdir(parents=True, exist_ok=True)

    # 3) Connect to SQLite (creates the DB file if it does not exist)
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # 4) Load and run the DDL scripts
    for ddl_path in ddl_paths:
        if not ddl_path.exists():
            raise FileNotFoundError(f"Missing DDL script: {ddl_path}")
        ddl_sql = ddl_path.read_text(encoding="utf-8")
        cur.executescript(ddl_sql)

    conn.commit()

    # 5) Consolidated Verification
    def _table_exists(cursor: sqlite3.Cursor, table_name: str) -> bool:
        cursor.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?;", (table_name,))
        return cursor.fetchone() is not None

    missing_tables = []
    for table in ["raw_staging", "raw_staging_payload"]:
        if not _table_exists(cur, table):
            missing_tables.append(table)

    conn.close()

    if missing_tables:
        raise RuntimeError(f"The following tables were not created: {', '.join(missing_tables)}")

    return db_path