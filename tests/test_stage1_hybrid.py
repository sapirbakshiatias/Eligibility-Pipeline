import sqlite3
import pandas as pd
import yaml
import pytest
from pathlib import Path
import sys

# Ensure the src directory is in the path so we can import our modules
root_path = Path(__file__).resolve().parent.parent
sys.path.append(str(root_path / "src"))

from pipeline.stage0_init_db import init_db
from pipeline.stage1_ingest_raw import ingest_stage1_hybrid


def _setup_test_environment(root: Path):
	"""
	Sets up a temporary project structure with the full SQL schema
	required for Stage 1.
	"""
	(root / "sql").mkdir(parents=True, exist_ok=True)
	(root / "input").mkdir(parents=True, exist_ok=True)
	(root / "mappings").mkdir(parents=True, exist_ok=True)
	(root / "output").mkdir(parents=True, exist_ok=True)

	# 01: Full Raw Staging Schema matching ALL expected staging_columns
	(root / "sql" / "01_create_raw_staging.sql").write_text(
		"""CREATE TABLE raw_staging (
			source_vendor TEXT, source_file TEXT, source_row INTEGER, load_run_id TEXT, ingested_at TEXT,
			source_extract_date TEXT, record_hash_raw TEXT, group_id_raw TEXT, subscriber_id_raw TEXT,
			person_id_raw TEXT, dependent_seq_raw TEXT, ssn_hash_raw TEXT, first_name_raw TEXT,
			last_name_raw TEXT, dob_raw TEXT, relationship_raw TEXT, address_line1 TEXT, city TEXT,
			state TEXT, zip TEXT, plan_type TEXT, provider TEXT, plan_id TEXT, plan_tier TEXT,
			is_active_raw TEXT, extra_payload TEXT
		);"""
	)

	# 02: Raw Staging Payload Schema
	(root / "sql" / "02_create_raw_staging_payload.sql").write_text(
		"""CREATE TABLE raw_staging_payload (
			load_run_id TEXT, source_vendor TEXT, source_file TEXT, 
			source_row INTEGER, ingested_at TEXT, raw_payload_json TEXT
		);"""
	)

	# 03: Silver Members Schema
	(root / "sql" / "03_create_silver_members.sql").write_text(
		"""CREATE TABLE silver_members (
			load_run_id TEXT, source_vendor TEXT, source_row INTEGER,
			first_name_norm TEXT, last_name_norm TEXT, dob_norm TEXT, relationship_norm TEXT
		);"""
	)

	# Mock YAML mapping for vendor medical_a
	mapping = {
		"source_vendor": "medical_a",
		"plan_type": "medical",
		"provider": "provider_a",
		"file_format": "csv",
		"column_mapping": {"first_nm": "first_name_raw"}
	}
	with open(root / "mappings" / "medical_a.yaml", "w") as f:
		yaml.dump(mapping, f)

	# Mock CSV input data
	df = pd.DataFrame({"first_nm": ["Alice", "Bob"]})
	df.to_csv(root / "input" / "medical_provider_a.csv", index=False)


# --- Top Level Test Functions (Now discoverable by Pytest) ---

def test_ingest_stage1_hybrid_success(tmp_path: Path):
	"""Verifies that Stage 1 populates staging and payload tables correctly."""
	_setup_test_environment(tmp_path)
	init_db(tmp_path)

	load_run_id = "test_run_123"
	yaml_dir = tmp_path / "mappings"

	# Act
	counts = ingest_stage1_hybrid(tmp_path, load_run_id, yaml_dir)

	# Assert
	assert "medical_a" in counts
	conn = sqlite3.connect(tmp_path / "output" / "warehouse.db")
	df_staged = pd.read_sql("SELECT * FROM raw_staging", conn)
	assert len(df_staged) == 2
	conn.close()


def test_ingest_stage1_skips_missing_files(tmp_path: Path):
	"""Ensures the pipeline skips missing input files gracefully."""
	# Setup folders but no input files
	(tmp_path / "sql").mkdir(parents=True, exist_ok=True)
	(tmp_path / "output").mkdir(parents=True, exist_ok=True)
	(tmp_path / "mappings").mkdir(parents=True, exist_ok=True)

	# Minimal DDLs to allow init_db to pass
	(tmp_path / "sql" / "01_create_raw_staging.sql").write_text("CREATE TABLE raw_staging (id TEXT);")
	(tmp_path / "sql" / "02_create_raw_staging_payload.sql").write_text("CREATE TABLE raw_staging_payload (id TEXT);")
	(tmp_path / "sql" / "03_create_silver_members.sql").write_text("CREATE TABLE silver_members (id TEXT);")

	init_db(tmp_path)

	# Act
	counts = ingest_stage1_hybrid(tmp_path, "run_1", tmp_path / "mappings")

	# Assert
	assert counts == {}