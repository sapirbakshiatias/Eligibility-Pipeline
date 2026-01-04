import pandas as pd
import yaml
import sqlite3
import hashlib
import json
from datetime import datetime
from pathlib import Path

def load_mapping(mapping_path: Path) -> dict:
    """Loads YAML mapping configuration for a specific vendor."""
    with open(mapping_path, 'r') as f:
        return yaml.safe_load(f)

def generate_row_hash(row_dict: dict) -> str:
    """Generates a unique SHA-256 hash for a raw data row to ensure traceability."""
    row_str = json.dumps(row_dict, sort_keys=True)
    return hashlib.sha256(row_str.encode()).hexdigest()

def transform_medical_c(df: pd.DataFrame) -> pd.DataFrame:
    """
    STRENGTHENED: Handles Medical Provider C nested JSON and split dates.
    Ensures nested objects are flattened so they can be mapped correctly.
    """
    # 1. Flatten Nested Name
    if 'name' in df.columns:
        df['name.first'] = df['name'].apply(lambda x: x.get('first') if isinstance(x, dict) else None)
        df['name.last'] = df['name'].apply(lambda x: x.get('last') if isinstance(x, dict) else None)

    # 2. Flatten Nested Address
    if 'address' in df.columns:
        df['address.street'] = df['address'].apply(lambda x: x.get('street') if isinstance(x, dict) else None)
        df['address.city'] = df['address'].apply(lambda x: x.get('city') if isinstance(x, dict) else None)
        df['address.state'] = df['address'].apply(lambda x: x.get('state') if isinstance(x, dict) else None)
        df['address.zip'] = df['address'].apply(lambda x: x.get('zip') if isinstance(x, dict) else None)

    # 3. Flatten Nested Plan details
    if 'plan' in df.columns:
        df['plan.plan_id'] = df['plan'].apply(lambda x: x.get('plan_id') if isinstance(x, dict) else None)
        df['plan.tier'] = df['plan'].apply(lambda x: x.get('tier') if isinstance(x, dict) else None)

    # 4. Assemble Split DOB with Zero-Padding
    # We use zfill(2) to ensure 2016-5-21 becomes 2016-05-21 for consistent parsing
    date_parts = ['dob_year', 'dob_month', 'dob_day']
    if all(col in df.columns for col in date_parts):
        df['dob_raw'] = (
            df['dob_year'].astype(str) + '-' +
            df['dob_month'].astype(str).str.zfill(2) + '-' +
            df['dob_day'].astype(str).str.zfill(2)
        )
    return df
def read_source_file(file_path: Path, mapping: dict) -> pd.DataFrame:
    fmt = mapping.get('file_format')
    try:
        if fmt == 'csv':
            return pd.read_csv(file_path)
        elif fmt == 'txt':
            return pd.read_csv(file_path, sep=mapping.get('delimiter', '|'))
        elif fmt == 'xlsx':
            return pd.read_excel(file_path, sheet_name=mapping.get('sheet_name'))
        elif fmt == 'jsonl':
            return pd.read_json(file_path, lines=True)
    except Exception as e:
        logger.error(f"Failed to read {file_path}: {e}")
    return None
def ingest_stage1_hybrid(root: Path, load_run_id: str, yaml_dir: Path):
    """
    Orchestrates Stage 1: Ingests raw files into Bronze layer (Staging + Payload tables).
    Includes custom transformation hooks for specific vendors.
    """
    db_path = root / "output" / "warehouse.db"
    input_dir = root / "input"
    conn = sqlite3.connect(db_path)

    # Dictionary mapping physical files to their logic-defining YAML configurations
    file_map = {
        "medical_provider_a.csv": "medical_a.yaml",
        "medical_provider_b.txt": "medical_b.yaml",
        "medical_provider_c.jsonl": "medical_c.yaml",
        "dental_provider.xlsx": "dental.yaml",
        "vision_provider.csv": "vision.yaml"
    }

    inserted_counts = {}
    for file_name, yaml_name in file_map.items():
        file_path = input_dir / file_name
        mapping_path = yaml_dir / yaml_name

        if not file_path.exists() or not mapping_path.exists():
            continue

        mapping = load_mapping(mapping_path)
        vendor = mapping['source_vendor']

        df_raw = read_source_file(file_path, mapping)
        if df_raw is None: continue

        # --- CUSTOM TRANSFORMATIONS (The Strategy Hook) ---
        if vendor == 'medical_provider_c':
            df_raw = transform_medical_c(df_raw)

        # Apply column mapping from YAML
        col_map = mapping.get('column_mapping', {})
        df_staging = df_raw.rename(columns=col_map)

        # Metadata enrichment
        df_staging['source_vendor'] = vendor
        df_staging['source_file'] = file_name
        df_staging['load_run_id'] = load_run_id
        df_staging['ingested_at'] = datetime.now().isoformat()
        df_staging['plan_type'] = mapping['plan_type']
        df_staging['provider'] = mapping['provider']
        df_staging['source_row'] = range(1, len(df_staging) + 1)
        df_staging['record_hash_raw'] = df_raw.apply(lambda x: generate_row_hash(x.to_dict()), axis=1)

        # Canonical Columns Enforcement
        staging_columns = [
            "source_vendor", "source_file", "source_row", "load_run_id", "ingested_at",
            "source_extract_date", "record_hash_raw", "group_id_raw", "subscriber_id_raw",
            "person_id_raw", "dependent_seq_raw", "ssn_hash_raw", "first_name_raw",
            "last_name_raw", "dob_raw", "relationship_raw", "address_line1", "city",
            "state", "zip", "plan_type", "provider", "plan_id", "plan_tier",
            "is_active_raw", "extra_payload"
        ]

        for col in staging_columns:
            if col not in df_staging.columns:
                df_staging[col] = None

        # Write to SQLite
        df_staging[staging_columns].to_sql('raw_staging', conn, if_exists='append', index=False)
        inserted_counts[vendor] = len(df_staging)

    conn.close()
    return inserted_counts