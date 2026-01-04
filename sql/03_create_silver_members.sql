-- 03_create_silver_members.sql
-- Purpose: Store standardized and cleaned member data for Stage 2 (Silver Layer)

CREATE TABLE IF NOT EXISTS silver_members (
    -- Metadata and Lineage
    load_run_id TEXT,
    source_vendor TEXT,
    source_file TEXT,
    source_row INTEGER,
    record_hash_raw TEXT,

    -- Normalized Identity Fields
    first_name_norm TEXT,
    last_name_norm TEXT,
    dob_norm TEXT, -- Format: YYYY-MM-DD

    -- Normalized Categorical Fields
    relationship_norm TEXT, -- Values: SUBSCRIBER, SPOUSE, CHILD, OTHER
    plan_type TEXT,
    provider TEXT,

    -- Original Raw Values (for debugging and reference)
    first_name_raw TEXT,
    last_name_raw TEXT,
    dob_raw TEXT,
    relationship_raw TEXT,

    -- Operational Metadata
    ingested_at TEXT,
    cleaned_at TEXT,
    PRIMARY KEY (load_run_id, source_vendor, source_row)
);