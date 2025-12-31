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
        sql_dir / "02_create_raw_staging_payload.sql",  # NEW
    ]
    # 2) Ensure output/ exists
    output_dir.mkdir(parents=True, exist_ok=True)

    # 3) Connect to SQLite (creates the DB file if it does not exist)
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # 4) Load and run the DDL script (can contain multiple SQL statements)
    # Run all DDL scripts (in order)
    for ddl_path in ddl_paths:
        ddl_sql = ddl_path.read_text(encoding="utf-8")
        cur.executescript(ddl_sql)

    conn.commit()

    # 5) Verify the raw_staging + raw_staging_payload  table exists
    def _table_exists(cursor: sqlite3.Cursor, table_name: str) -> bool:
        cursor.execute("""
            SELECT 1
            FROM sqlite_master
            WHERE type='table' AND name=?;
        """, (table_name,))
        return cursor.fetchone() is not None

    raw_exists = _table_exists(cur, "raw_staging")
    payload_exists = _table_exists(cur, "raw_staging_payload")

    if not payload_exists:
        raise RuntimeError("raw_staging_payload was not created. Check sql/02_create_raw_staging_payload.sql")

    # 6) close the DB connection
    conn.close()

    if not raw_exists:
        raise RuntimeError("raw_staging was not created. Check sql/01_create_raw_staging.sql")

    if not payload_exists:
        raise RuntimeError("raw_staging_payload was not created. Check sql/02_create_raw_staging_payload.sql")

    return db_path