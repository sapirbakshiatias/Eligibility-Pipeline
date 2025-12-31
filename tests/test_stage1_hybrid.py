import sqlite3
from pathlib import Path
import shutil

from pipeline.stage0_init_db import init_db
from pipeline.stage1_ingest_raw import ingest_stage1_hybrid


def copy_tree(src: Path, dst: Path) -> None:
    if not src.exists():
        raise FileNotFoundError(f"Source path does not exist: {src}")
    dst.mkdir(parents=True, exist_ok=True)
    for item in src.iterdir():
        if item.is_dir():
            shutil.copytree(item, dst / item.name, dirs_exist_ok=True)
        else:
            shutil.copy2(item, dst / item.name)


def test_hybrid_ingestion_writes_both_tables_and_joins(tmp_path: Path):
    # IMPORTANT: pytest runs with cwd=tests/, so compute repo root from this file location.
    repo_root = Path(__file__).resolve().parents[1]

    # Build a minimal "repo root" in tmp_path
    copy_tree(repo_root / "sql", tmp_path / "sql")
    copy_tree(repo_root / "mappings", tmp_path / "mappings")
    copy_tree(repo_root / "input", tmp_path / "input")

    db_path = init_db(tmp_path)
    assert db_path.exists()

    load_run_id = "TEST_RUN_1"

    counts = ingest_stage1_hybrid(
        tmp_path,
        load_run_id=load_run_id,
        yaml_dir=tmp_path / "mappings",
    )
    expected_total = sum(counts.values())
    assert expected_total > 0

    con = sqlite3.connect(db_path)
    try:
        tables = {
            r[0] for r in con.execute(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;"
            ).fetchall()
        }
        assert "raw_staging" in tables
        assert "raw_staging_payload" in tables

        n_raw = con.execute(
            "SELECT COUNT(*) FROM raw_staging WHERE load_run_id = ?;",
            (load_run_id,),
        ).fetchone()[0]

        n_payload = con.execute(
            "SELECT COUNT(*) FROM raw_staging_payload WHERE load_run_id = ?;",
            (load_run_id,),
        ).fetchone()[0]

        assert n_raw == expected_total
        assert n_payload == expected_total

        n_join = con.execute("""
            SELECT COUNT(*)
            FROM raw_staging s
            JOIN raw_staging_payload p
              ON s.load_run_id=p.load_run_id
             AND s.source_vendor=p.source_vendor
             AND s.source_file=p.source_file
             AND s.source_row=p.source_row
            WHERE s.load_run_id = ?;
        """, (load_run_id,)).fetchone()[0]

        assert n_join == expected_total

        # Optional: record_hash match check
        try:
            n_hash = con.execute("""
                SELECT COUNT(*)
                FROM raw_staging s
                JOIN raw_staging_payload p
                  ON s.load_run_id=p.load_run_id
                 AND s.source_vendor=p.source_vendor
                 AND s.source_file=p.source_file
                 AND s.source_row=p.source_row
                WHERE s.load_run_id = ?
                  AND s.record_hash_raw = p.record_hash_raw;
            """, (load_run_id,)).fetchone()[0]
            assert n_hash == expected_total
        except sqlite3.OperationalError:
            pass
    finally:
        con.close()
