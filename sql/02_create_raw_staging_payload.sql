-- Sidecar table: full raw row snapshot as JSON, keyed by lineage
CREATE TABLE IF NOT EXISTS raw_staging_payload (
  load_run_id      TEXT NOT NULL,
  source_vendor    TEXT NOT NULL,
  source_file      TEXT NOT NULL,
  source_row       INTEGER NOT NULL,
  ingested_at      TEXT NOT NULL,

  -- Optional but useful for debugging / joins / dedup investigations
  record_hash_raw  TEXT,

  -- Full original row as read from the vendor file (JSON string)
  raw_payload_json TEXT NOT NULL,

  PRIMARY KEY (load_run_id, source_vendor, source_file, source_row)
);

CREATE INDEX IF NOT EXISTS idx_raw_staging_payload_record_hash
ON raw_staging_payload(record_hash_raw);
