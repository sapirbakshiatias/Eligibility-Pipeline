-- RAW STAGING: Canonical schema for all vendors (Dental/Vision/Medical A/B/C)
-- Notes:
-- * RAW means: keep values as-is (no normalization), except minimal structural mapping.
-- * Most fields are TEXT for simplicity and to avoid premature parsing.

CREATE TABLE IF NOT EXISTS raw_staging (
  -- Lineage / Meta
  source_vendor        TEXT NOT NULL,   -- dental | vision | medical_a | medical_b | medical_c
  source_file          TEXT NOT NULL,
  source_row           INTEGER NOT NULL,
  load_run_id          TEXT NOT NULL,
  ingested_at          TEXT NOT NULL,   -- keep as ISO string for portability (or TIMESTAMP in your DB)
  source_extract_date  TEXT NULL,       -- optional; ISO date string (YYYY-MM-DD) if available

  -- De-dup (content hash, computed before any normalization)
  record_hash_raw      TEXT NOT NULL,

  -- IDs (raw identifiers before cross-vendor identity resolution)
  group_id_raw         TEXT NULL,
  subscriber_id_raw    TEXT NULL,
  person_id_raw        TEXT NULL,
  dependent_seq_raw    TEXT NULL,
  ssn_hash_raw         TEXT NULL,

  -- Person (RAW)
  first_name_raw       TEXT NULL,
  last_name_raw        TEXT NULL,
  dob_raw              TEXT NULL,
  relationship_raw     TEXT NULL,

  -- Address (RAW values, canonical column names)
  address_line1        TEXT NULL,
  city                 TEXT NULL,
  state                TEXT NULL,
  zip                  TEXT NULL,

  -- Plan (RAW)
  plan_type            TEXT NOT NULL,   -- medical | dental | vision
  provider             TEXT NOT NULL,   -- vendor name, e.g. medical_a
  plan_id              TEXT NULL,
  plan_tier            TEXT NULL,

  -- Status (RAW)
  is_active_raw        TEXT NULL,

  -- Optional: unmapped fields as JSON string
  extra_payload        TEXT NULL
);

-- Recommended indexes (optional, but helpful)
CREATE INDEX IF NOT EXISTS idx_raw_staging_vendor
  ON raw_staging (source_vendor);

CREATE INDEX IF NOT EXISTS idx_raw_staging_plan_type
  ON raw_staging (plan_type);

CREATE INDEX IF NOT EXISTS idx_raw_staging_record_hash
  ON raw_staging (record_hash_raw);

CREATE INDEX IF NOT EXISTS idx_raw_staging_person_id
  ON raw_staging (person_id_raw);
