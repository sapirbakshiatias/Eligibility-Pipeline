import sqlite3
import pandas as pd
import logging
import yaml
from datetime import datetime
from pathlib import Path

# Global Configuration Constants
CONFIG_PATH = "mappings/relationship_normalization.yaml"
OUTPUT_DB = "output/warehouse.db"

# Initialize Logger
logger = logging.getLogger(__name__)


def load_normalization_config(root: Path) -> dict:
	"""
	Loads the central normalization mapping file (YAML).
	Architecture: Decouples business rules (YAML) from the logic engine (Python).
	"""
	path = root / CONFIG_PATH
	if not path.exists():
		logger.error(f"Configuration file not found at: {path}")
		return {}

	try:
		with open(path, 'r', encoding='utf-8') as f:
			config = yaml.safe_load(f)
			logger.info("Normalization mapping file loaded successfully.")
			return config or {}
	except Exception as e:
		logger.error(f"Error parsing YAML file: {e}")
		return {}


def clean_name(series: pd.Series) -> pd.Series:
	"""
	Global name cleaning logic:
	Converts to lowercase, removes non-alphanumeric characters, and strips whitespace.
	"""
	return (
		series.astype(str)
		.str.casefold()
		.str.replace(r'[^a-z0-9]', '', regex=True)
		.str.strip()
	)


def run_stage2_cleaning(root: Path, load_run_id: str) -> int:
	"""
	Stage 2: Data Cleaning and Normalization (Silver Layer).
	Performs vendor-specific date parsing and relationship mapping.
	"""
	# 1. Load normalization rules from config
	full_config = load_normalization_config(root)
	rel_maps = full_config.get('relationship_mappings', {})
	date_formats = full_config.get('date_formats', {})

	db_path = root / OUTPUT_DB
	conn = sqlite3.connect(db_path)

	try:
		# Fetch raw data for the specific load run
		query = "SELECT * FROM raw_staging WHERE load_run_id = ?"
		df = pd.read_sql(query, conn, params=(load_run_id,))

		if df.empty:
			logger.warning(f"No raw records found for RunID: {load_run_id}")
			return 0

		# 2. Global Name Normalization
		df['first_name_norm'] = clean_name(df['first_name_raw'])
		df['last_name_norm'] = clean_name(df['last_name_raw'])

		# 3. Vendor-Specific Date of Birth (DOB) Normalization
		# Logic: Uses the date_formats map to parse strings into actual dates
		df['dob_norm'] = None
		for vendor, fmt in date_formats.items():
			mask = df['source_vendor'] == vendor
			if mask.any():
				logger.info(f"Normalizing dates for {vendor} using format {fmt}")
				# Clean raw strings before parsing to prevent NaT failures
				raw_dates = df.loc[mask, 'dob_raw'].astype(str).str.strip()
				# errors='coerce' turns invalid dates (e.g., 99/99/9999) into NULL
				parsed = pd.to_datetime(raw_dates, format=fmt, errors='coerce')
				df.loc[mask, 'dob_norm'] = parsed.dt.strftime('%Y-%m-%d')

		# 4. Vendor-Specific Relationship Normalization
		# Logic: Maps source values (e.g., 'EMP') to target values (e.g., 'employee')
		df['relationship_norm'] = 'OTHER'
		for vendor, mapping in rel_maps.items():
			mask = df['source_vendor'] == vendor
			if mask.any():
				# Extract and clean raw values for lookup
				raw_rels = df.loc[mask, 'relationship_raw'].astype(str).str.lower().str.strip()
				# Apply the map and default unknown values to 'OTHER'
				df.loc[mask, 'relationship_norm'] = raw_rels.map(mapping).fillna('OTHER')

		# 5. Persist to Silver Layer
		df['cleaned_at'] = datetime.now().isoformat()

		# Define the canonical column order for the Silver table
		silver_cols = [
			'load_run_id', 'source_vendor', 'source_file', 'source_row', 'record_hash_raw',
			'first_name_norm', 'last_name_norm', 'dob_norm', 'relationship_norm',
			'plan_type', 'provider', 'first_name_raw', 'last_name_raw', 'dob_raw',
			'relationship_raw', 'ingested_at', 'cleaned_at'
		]

		# Write data to SQL
		df[silver_cols].to_sql('silver_members', conn, if_exists='append', index=False)
		logger.info(f"Normalization complete. {len(df)} records saved to Silver Layer.")

		return len(df)

	except Exception as e:
		logger.error(f"Pipeline crashed during Stage 2: {e}")
		raise
	finally:
		conn.close()