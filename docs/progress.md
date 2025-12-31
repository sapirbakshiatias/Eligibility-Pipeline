### Stage 0 — Init DB (Update: Sidecar Payload Table)

**Goal:** Extend the warehouse initialization to support Hybrid RAW ingestion by adding a sidecar payload table.

**Implementation (code):**

* `src/pipeline/stage0_init_db.py` → `init_db(root)`
* DDL added: `sql/02_create_raw_staging_payload.sql`
* Orchestration: `src/pipeline/main.py` still calls `init_db(root)`

**What it does:**

* Executes both DDL scripts:

  * `sql/01_create_raw_staging.sql` (creates `raw_staging`)
  * `sql/02_create_raw_staging_payload.sql` (creates `raw_staging_payload`)
* Verifies **both** tables exist via `sqlite_master`:

  * `raw_staging`
  * `raw_staging_payload`

**Result:**

* Warehouse DB now includes the sidecar table for full-row payload preservation.
* DB initialization is still idempotent.

---

## Decision: Hybrid RAW Staging with a Sidecar Payload Table

### What we decided

We will implement **Hybrid ingestion** for Stage 1:

1. **Canonical RAW columns in `raw_staging`**

   * Stage 1 maps vendor-specific fields into canonical `*_raw` columns using YAML mappings (no business rules, no normalization).
   * This provides a stable warehouse contract for downstream SQL QA and processing.

2. **Full raw row preservation in `raw_staging_payload`**

   * In addition to canonical columns, Stage 1 stores the **entire original vendor row** as JSON in a separate sidecar table.
   * The payload row is linked to the canonical row via lineage keys:

     * `load_run_id`, `source_vendor`, `source_file`, `source_row`
     * plus optional `record_hash_raw` for easier debugging/dedup investigations.

This keeps `raw_staging` optimized for analytics/QA while ensuring we retain complete original input context.

---

## Why we keep `raw_payload_json` even if we “don’t transform”

Even without business transformations, canonical staging can still cause **information loss or unrecoverable context**, because:

* **Selection loss:** YAML mappings include only known fields; new/unmapped vendor columns would otherwise be dropped silently.
* **Reader-induced representation changes:** ingestion libraries (especially Excel readers) may subtly alter how values are represented (empty vs null, numeric formatting, leading zeros, etc.).
* **Debug/forensics:** storing the full raw row provides a durable snapshot of “what was read” at ingestion time without relying on source files.
* **Reprocessing flexibility:** mapping changes can be re-applied using warehouse-stored payloads without re-ingesting original files.

---

## Trade-offs

### Benefits

* **High traceability:** every canonical record can be traced to the original raw row content.
* **Resilience to schema drift:** vendor adds/changes columns → payload still captures them immediately.
* **Better QA:** canonical columns enable fast SQL checks (counts, null checks, distributions, etc.).
* **Reproducibility:** enables reprocessing even if upstream source files change or are no longer available.

### Costs

* **Storage overhead:** storing JSON payloads increases disk usage (acceptable for this take-home).
* **Slightly more complexity:** one additional table + one additional insert per input row.

---

### Stage 1 — Hybrid Raw Ingestion (Canonical + Sidecar Payload)

**Goal:** Ingest all vendor files into the warehouse using YAML-driven mappings, while preserving full raw rows for lineage and debugging.

**Implementation (code):**

* `src/pipeline/stage1_ingest_raw.py` → `ingest_stage1_hybrid(root, load_run_id, yaml_dir=...)`
* Orchestration: `src/pipeline/main.py` calls Stage 1 after Stage 0 + manifest
* Mappings directory: `mappings/` (e.g., `mappings/dental.yaml`, `mappings/medical_a.yaml`, etc.)

**What it does:**

* Reads all vendor inputs (CSV / pipe-delimited / XLSX / JSONL) as defined by YAML configs.
* Builds a canonical RAW row for `raw_staging` using:

  * `constants`, `mapping`, `nulls`, and minimal allowed `derivations` (where configured)
* Writes the full original input row as JSON to `raw_staging_payload.raw_payload_json`
* Uses consistent lineage keys for both tables:

  * `load_run_id`, `source_vendor`, `source_file`, `source_row`, `ingested_at`
* Computes `record_hash_raw` on canonical content fields (excluding lineage fields), per contract.

**Result:**

* Stage 1 completed successfully with **3962** ingested rows:

  * `dental`: 811
  * `vision`: 811
  * `medical_a`: 807
  * `medical_b`: 771
  * `medical_c`: 762

---

### Automated tests (pytest) — Stage 1 Hybrid

**File:** `tests/test_stage1_hybrid.py`

**Covers:**

1. Stage 0 creates both `raw_staging` and `raw_staging_payload`
2. Stage 1 writes the same number of rows to both tables for a test `load_run_id`
3. Join completeness check (no orphan rows) using:

   * `load_run_id`, `source_vendor`, `source_file`, `source_row`
4. (Optional) `record_hash_raw` matches between canonical and payload tables (if present)

**Run:**

```bash
pytest -q
```

---
