import sqlite3
import logging
from pathlib import Path

# Setup logging configuration
logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

# --- Centralized SQL Queries ---
QUERIES = {
	# Fetch row counts from all three layers for the specific run
	"counts_raw": "SELECT COUNT(*) FROM raw_staging WHERE load_run_id = ?;",
	"counts_payload": "SELECT COUNT(*) FROM raw_staging_payload WHERE load_run_id = ?;",
	"counts_silver": "SELECT COUNT(*) FROM silver_members WHERE load_run_id = ?;",

	# Verify that every raw record has a corresponding JSON payload
	"join_completeness": """
        SELECT COUNT(*) FROM raw_staging s
        JOIN raw_staging_payload p ON s.load_run_id = p.load_run_id
         AND s.source_vendor = p.source_vendor AND s.source_file = p.source_file
         AND s.source_row = p.source_row
        WHERE s.load_run_id = ?;
    """,

	# Sample check to verify Stage 2 normalization (Raw value vs Standardized value)
	"sample_silver": """
        SELECT source_vendor, first_name_raw, first_name_norm, relationship_raw, relationship_norm
        FROM silver_members 
        WHERE load_run_id = ? 
        AND source_vendor = 'medical_provider_a'
        LIMIT 3;
    """
}


def get_latest_run_id(cur: sqlite3.Cursor) -> str:
	"""
	Finds the most recent load_run_id in the silver_members table.
	Since IDs are timestamp-based, MAX() returns the latest one.
	"""
	res = cur.execute("SELECT MAX(load_run_id) FROM silver_members").fetchone()
	return res[0] if res else None


def run_validation(root: Path, load_run_id: str = None):
	"""
	Performs integrity checks on Bronze and Silver layers.
	If load_run_id is not provided, it automatically discovers the latest run.
	"""
	db_path = root / "output" / "warehouse.db"

	if not db_path.exists():
		logger.error(f"Database not found at {db_path}")
		return

	conn = sqlite3.connect(db_path)
	conn.row_factory = sqlite3.Row
	cur = conn.cursor()

	try:
		# Step 1: Discover the latest Run ID if none was passed
		if load_run_id is None:
			load_run_id = get_latest_run_id(cur)

		if not load_run_id:
			logger.error("No data found in silver_members table to validate.")
			return

		logger.info(f"\n--- Integrity Report for Run: {load_run_id} ---")

		# Step 2: Compare Row Counts across all layers
		n_raw = cur.execute(QUERIES["counts_raw"], (load_run_id,)).fetchone()[0]
		n_payload = cur.execute(QUERIES["counts_payload"], (load_run_id,)).fetchone()[0]
		n_silver = cur.execute(QUERIES["counts_silver"], (load_run_id,)).fetchone()[0]

		logger.info(f"Row Counts: Raw={n_raw}, Payload={n_payload}, Silver={n_silver}")

		# Step 3: Verify Lineage (Bronze Layer Consistency)
		joined = cur.execute(QUERIES["join_completeness"], (load_run_id,)).fetchone()[0]
		logger.info(f"Lineage Check (Raw to Payload): {joined}/{n_raw}")

		# Step 4: Validate Normalization Results (Silver Layer)
		logger.info("\n--- Stage 2 Normalization Sample (Raw vs. Norm) ---")
		samples = cur.execute(QUERIES["sample_silver"], (load_run_id,)).fetchall()

		for row in samples:
			logger.info(
				f" [{row['source_vendor']}] {row['first_name_raw']} -> {row['first_name_norm']} "
				f"| Rel: {row['relationship_raw']} -> {row['relationship_norm']}"
			)

	finally:
		conn.close()


if __name__ == "__main__":
	# Detect local project root based on file location
	current_root = Path(__file__).resolve().parent
	# Run validation without a specific ID to trigger 'Auto-Discovery' of the latest run
	run_validation(current_root)