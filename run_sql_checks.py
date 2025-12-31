import sqlite3
from pathlib import Path

DB_PATH = Path(r"C:\Users\sapir\PycharmProjects\Eligibility-Pipeline\output\warehouse.db")
LOAD_RUN_ID = "20251231T170615Z_5947ddfa"  # update if needed

QUERIES = {
    "tables": """
        SELECT name
        FROM sqlite_master
        WHERE type='table'
        ORDER BY name;
    """,
    "counts_raw": """
        SELECT COUNT(*) AS n
        FROM raw_staging
        WHERE load_run_id = ?;
    """,
    "counts_payload": """
        SELECT COUNT(*) AS n
        FROM raw_staging_payload
        WHERE load_run_id = ?;
    """,
    "by_vendor_raw": """
        SELECT source_vendor, COUNT(*) AS n
        FROM raw_staging
        WHERE load_run_id = ?
        GROUP BY 1
        ORDER BY 1;
    """,
    "by_vendor_payload": """
        SELECT source_vendor, COUNT(*) AS n
        FROM raw_staging_payload
        WHERE load_run_id = ?
        GROUP BY 1
        ORDER BY 1;
    """,
    "join_completeness": """
        SELECT COUNT(*) AS joined
        FROM raw_staging s
        JOIN raw_staging_payload p
          ON s.load_run_id=p.load_run_id
         AND s.source_vendor=p.source_vendor
         AND s.source_file=p.source_file
         AND s.source_row=p.source_row
        WHERE s.load_run_id = ?;
    """,
    "orphans_raw_missing_payload": """
        SELECT COUNT(*) AS n
        FROM raw_staging s
        LEFT JOIN raw_staging_payload p
          ON s.load_run_id=p.load_run_id
         AND s.source_vendor=p.source_vendor
         AND s.source_file=p.source_file
         AND s.source_row=p.source_row
        WHERE s.load_run_id = ?
          AND p.load_run_id IS NULL;
    """,
    "orphans_payload_missing_raw": """
        SELECT COUNT(*) AS n
        FROM raw_staging_payload p
        LEFT JOIN raw_staging s
          ON s.load_run_id=p.load_run_id
         AND s.source_vendor=p.source_vendor
         AND s.source_file=p.source_file
         AND s.source_row=p.source_row
        WHERE p.load_run_id = ?
          AND s.load_run_id IS NULL;
    """,
    "hash_match": """
        SELECT COUNT(*) AS n
        FROM raw_staging s
        JOIN raw_staging_payload p
          ON s.load_run_id=p.load_run_id
         AND s.source_vendor=p.source_vendor
         AND s.source_file=p.source_file
         AND s.source_row=p.source_row
        WHERE s.load_run_id = ?
          AND s.record_hash_raw = p.record_hash_raw;
    """,
}

def print_rows(title, rows):
    print(f"\n=== {title} ===")
    for r in rows:
        print(r)

def main():
    con = sqlite3.connect(DB_PATH)
    try:
        cur = con.cursor()

        # 1) tables
        rows = cur.execute(QUERIES["tables"]).fetchall()
        print_rows("Tables", [r[0] for r in rows])

        # 2) counts
        n_raw = cur.execute(QUERIES["counts_raw"], (LOAD_RUN_ID,)).fetchone()[0]
        n_payload = cur.execute(QUERIES["counts_payload"], (LOAD_RUN_ID,)).fetchone()[0]
        print_rows("Counts", [f"raw_staging={n_raw}", f"raw_staging_payload={n_payload}"])

        # 3) by vendor
        by_vendor_raw = cur.execute(QUERIES["by_vendor_raw"], (LOAD_RUN_ID,)).fetchall()
        by_vendor_payload = cur.execute(QUERIES["by_vendor_payload"], (LOAD_RUN_ID,)).fetchall()
        print_rows("Counts by vendor (raw_staging)", by_vendor_raw)
        print_rows("Counts by vendor (raw_staging_payload)", by_vendor_payload)

        # 4) join + orphans
        joined = cur.execute(QUERIES["join_completeness"], (LOAD_RUN_ID,)).fetchone()[0]
        or1 = cur.execute(QUERIES["orphans_raw_missing_payload"], (LOAD_RUN_ID,)).fetchone()[0]
        or2 = cur.execute(QUERIES["orphans_payload_missing_raw"], (LOAD_RUN_ID,)).fetchone()[0]
        print_rows("Join checks", [f"joined={joined}", f"raw_without_payload={or1}", f"payload_without_raw={or2}"])

        # 5) hash match (optional)
        try:
            hm = cur.execute(QUERIES["hash_match"], (LOAD_RUN_ID,)).fetchone()[0]
            print_rows("Hash match", [f"hash_matches={hm}"])
        except sqlite3.OperationalError as e:
            print_rows("Hash match", [f"skipped ({e})"])

    finally:
        con.close()

if __name__ == "__main__":
    main()
