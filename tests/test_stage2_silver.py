
import sqlite3
import pandas as pd
import yaml
from datetime import datetime
from pathlib import Path


def load_normalization_maps(root: Path) -> dict:
	"""Loads the relationship normalization map from the YAML file."""
	map_path = root / "mappings" / "relationship_normalization.yaml"
	with open(map_path, 'r') as f:
		return yaml.safe_load(f)


def run_stage2_cleaning(root: Path, load_run_id: str) -> int:
	"""
	Stage 2: Standardizes data using the YAML mapping file.
	"""
	# 1. Load the REAL mapping from your YAML (Removes any old hardcoded values)
	rel_maps = load_normalization_maps(root)

	db_path = root / "output" / "warehouse.db"
	conn = sqlite3.connect(db_path)

	try:
		df = pd.read_sql("SELECT * FROM raw_staging WHERE load_run_id = ?", conn, params=(load_run_id,))
		if df.empty:
			return 0

		# Name and Date cleaning
		df['first_name_norm'] = df['first_name_raw'].astype(str).str.lower().str.strip()
		df['last_name_norm'] = df['last_name_raw'].astype(str).str.lower().str.strip()
		df['dob_norm'] = pd.to_datetime(df['dob_raw'], errors='coerce').dt.strftime('%Y-%m-%d')

		# --- The Correct Mapping Logic ---
		df['relationship_norm'] = 'OTHER'  # Default fallback

		for vendor, mapping in rel_maps.items():
			mask = df['source_vendor'] == vendor
			if mask.any():
				# Crucial: convert raw 'EMP' to 'emp' to match YAML keys
				raw_vals = df.loc[mask, 'relationship_raw'].astype(str).str.lower().str.strip()
				# Apply the specific vendor dictionary from YAML
				df.loc[mask, 'relationship_norm'] = raw_vals.map(mapping).fillna('OTHER')

		# 2. Save to Silver
		df['cleaned_at'] = datetime.now().isoformat()
		# Using to_sql with if_exists='append' but ensure we only see THIS run
		df.to_sql('silver_members', conn, if_exists='append', index=False)
		return len(df)
	finally:
		conn.close()