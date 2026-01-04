from pathlib import Path
import sqlite3
import pytest
from pipeline.stage0_init_db import init_db

# Define the DDL here to make the test self-contained and independent of external files
DDL_01 = """
-- 01_create_raw_staging.sql
CREATE TABLE raw_staging (
    source_vendor TEXT, 
    source_file TEXT, 
    source_row INTEGER, 
    load_run_id TEXT, 
    ingested_at TEXT, 
    record_hash_raw TEXT, 
    plan_type TEXT, 
    provider TEXT
);
"""

DDL_02 = """
-- 02_create_raw_staging_payload.sql
CREATE TABLE raw_staging_payload (
    load_run_id TEXT, 
    source_vendor TEXT, 
    source_file TEXT, 
    source_row INTEGER, 
    ingested_at TEXT, 
    raw_payload_json TEXT, 
    PRIMARY KEY (load_run_id, source_vendor, source_file, source_row)
);
"""

def _setup_sql_files(root: Path):
    """
    Creates the 'sql' directory with the two specific SQL files
    that the init_db function expects to find.
    """
    sql_dir = root / "sql"
    sql_dir.mkdir(parents=True, exist_ok=True)
    # The init_db logic specifically looks for these two file names
    (sql_dir / "01_create_raw_staging.sql").write_text(DDL_01)
    (sql_dir / "02_create_raw_staging_payload.sql").write_text(DDL_02)


def test_stage0_init_db_creates_tables(tmp_path: Path):
    """
    Tests that calling init_db successfully creates the database file
    and the required staging tables.
    """
    # Arrange
    _setup_sql_files(tmp_path)

    # Act
    db_path = init_db(tmp_path)

    # Assert
    assert db_path.exists()
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # Check that table names exist in the DB and match those defined in the DDL
    cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row[0] for row in cur.fetchall()]
    assert "raw_staging" in tables
    assert "raw_staging_payload" in tables
    conn.close()


def test_stage0_init_db_raises_error_on_missing_sql(tmp_path: Path):
    """
    Ensures that init_db raises a FileNotFoundError if the mandatory
    SQL scripts are missing from the project structure.
    """
    # Arrange - Create an empty 'sql' directory without any .sql files
    (tmp_path / "sql").mkdir()

    # Act & Assert
    with pytest.raises(FileNotFoundError):
       init_db(tmp_path)