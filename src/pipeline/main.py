from pathlib import Path
import logging
from pipeline.stage0_init_db import init_db
from pipeline.stage0_manifest import generate_load_run_id, build_staging_manifest
from pipeline.stage1_ingest_raw import ingest_stage1_hybrid
from pipeline.stage2_clean_silver import run_stage2_cleaning

logger = logging.getLogger(__name__)

def main(root: Path) -> str:
    """
    Orchestrates the pipeline: Setup -> Manifest -> Hybrid Ingestion.
    Returns: load_run_id (str) for downstream validation/logging.
    """

    # --- Stage 0: Infrastructure Setup ---
    # Creates SQLite DB and staging tables defined in SQL scripts
    db_path = init_db(root)
    logger.info("Stage 0: DB initialized at %s", db_path)

    # --- Stage 0b: Observability & Lineage ---
    # Generates a run ID and snapshots input file metadata (hashes/counts)
    load_run_id = generate_load_run_id()
    manifest_path = build_staging_manifest(root, load_run_id=load_run_id)
    logger.info("Stage 0b: Manifest created at %s (RunID: %s)", manifest_path, load_run_id)

    # --- Stage 1: Hybrid Ingestion ---
    # Maps source columns to canonical fields + stores full JSON sidecar
    yaml_dir = root / "mappings"
    inserted_counts = ingest_stage1_hybrid(root, load_run_id, yaml_dir)

    total = sum(inserted_counts.values())
    logger.info("Stage 1: Ingested %s rows total. Breakdown: %s", total, inserted_counts)

    # --- Stage 2: Cleaning & Normalization - --
    logger.info("Stage 2: Starting data cleaning and normalization (Silver Layer)...")
    silver_count = run_stage2_cleaning(root, load_run_id)
    logger.info("Stage 2: Successfully cleaned and moved %s rows to silver_members.", silver_count)

    return load_run_id # Required for automated post-checks