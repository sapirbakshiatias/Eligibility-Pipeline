from pathlib import Path

from pipeline.stage0_init_db import init_db
from pipeline.stage0_manifest import generate_load_run_id, build_staging_manifest
from pipeline.stage1_ingest_raw import ingest_stage1_hybrid


def main(root: Path) -> None:
    """
    Orchestrate the pipeline end-to-end for a single run.

    Stages:
      - Stage 0: Initialize SQLite warehouse + required staging tables
      - Stage 0b: Build a staging manifest (lineage/observability)
      - Stage 1: Hybrid ingestion:
          * Write canonical RAW fields into raw_staging (per YAML mappings)
          * Write full original rows as JSON into raw_staging_payload (sidecar)
    """

    # --- Stage 0: DB init (creates output/warehouse.db and staging tables) ---
    db_path = init_db(root)
    print(f"✅ Stage 0 complete: DB ready at {db_path}")

    # --- Stage 0b: manifest/lineage for this run ---
    load_run_id = generate_load_run_id()
    manifest_path = build_staging_manifest(root, load_run_id=load_run_id, require_non_empty=True)
    print(f"✅ Manifest written: {manifest_path}")
    print(f"ℹ load_run_id={load_run_id}")

    # --- Stage 1: Hybrid ingestion (canonical + payload sidecar) ---
    # YAML files live under: <repo_root>/mappings/*.yaml
    yaml_dir = root / "mappings"

    inserted_counts = ingest_stage1_hybrid(
        root,
        load_run_id=load_run_id,
        yaml_dir=yaml_dir,
    )

    total_inserted = sum(inserted_counts.values())
    print(f"✅ Stage 1 complete: inserted_rows={total_inserted} per_vendor={inserted_counts}")
