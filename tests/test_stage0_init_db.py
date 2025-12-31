from pathlib import Path
import sqlite3
import pytest

from pipeline.main import main


def _make_temp_project(tmp_path: Path, ddl_sql: str) -> Path:
    """
    Creates a temp project structure expected by main(root):
      root/sql/01_create_raw_staging.sql
      root/output/
    """
    (tmp_path / "sql").mkdir(parents=True, exist_ok=True)
    (tmp_path / "output").mkdir(parents=True, exist_ok=True)
    (tmp_path / "sql" / "01_create_raw_staging.sql").write_text(ddl_sql, encoding="utf-8")
    return tmp_path


def test_stage0_creates_db_and_raw_staging(tmp_path: Path) -> None:
    # Arrange: minimal DDL that creates raw_staging
    ddl = """
    CREATE TABLE IF NOT EXISTS raw_staging (
        source_vendor TEXT NOT NULL,
        source_file TEXT NOT NULL,
        source_row INTEGER NOT NULL,
        load_run_id TEXT NOT NULL,
        ingested_at TEXT NOT NULL,
        plan_type TEXT NOT NULL,
        provider TEXT NOT NULL,
        record_hash_raw TEXT NOT NULL
    );
    """
    root = _make_temp_project(tmp_path, ddl)

    # Act
    main(root)

    # Assert: DB exists
    db_path = root / "output" / "warehouse.db"
    assert db_path.exists()

    # Assert: table exists
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='raw_staging';")
    assert cur.fetchone() is not None
    conn.close()


def test_stage0_is_idempotent_running_twice(tmp_path: Path) -> None:
    ddl = """
    CREATE TABLE IF NOT EXISTS raw_staging (
        source_vendor TEXT NOT NULL,
        source_file TEXT NOT NULL,
        source_row INTEGER NOT NULL,
        load_run_id TEXT NOT NULL,
        ingested_at TEXT NOT NULL,
        plan_type TEXT NOT NULL,
        provider TEXT NOT NULL,
        record_hash_raw TEXT NOT NULL
    );
    """
    root = _make_temp_project(tmp_path, ddl)

    # Act: run twice
    main(root)
    main(root)

    # Assert: still exists
    db_path = root / "output" / "warehouse.db"
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='raw_staging';")
    assert cur.fetchone() is not None
    conn.close()


def test_stage0_raises_if_ddl_missing(tmp_path: Path) -> None:
    # Arrange: project folders but NO DDL file
    (tmp_path / "sql").mkdir(parents=True, exist_ok=True)
    (tmp_path / "output").mkdir(parents=True, exist_ok=True)

    # Act + Assert
    with pytest.raises(FileNotFoundError):
        main(tmp_path)


def test_stage0_raises_if_raw_staging_not_created(tmp_path: Path) -> None:
    # Arrange: DDL that does NOT create raw_staging
    ddl = "CREATE TABLE something_else(id INTEGER);"
    root = _make_temp_project(tmp_path, ddl)

    # Act + Assert: your code raises RuntimeError if raw_staging missing
    with pytest.raises(RuntimeError):
        main(root)
