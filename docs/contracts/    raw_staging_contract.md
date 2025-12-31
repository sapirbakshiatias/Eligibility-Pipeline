# RAW STAGING Contract (Canonical)

## Purpose
`raw_staging` is a canonical RAW table that stores eligibility records from all vendors (Dental/Vision/Medical A/B/C)
in a unified schema, with strong lineage fields for traceability.

RAW means:
- Values are stored as received (no normalization / standardization yet).
- Only structural mapping is done (renaming columns into a canonical schema).
- Minimal derivation is allowed when the source does not provide a single field (e.g., Medical C DOB is split across year/month/day).

## Grain
One row per person per plan record from a vendor file.

## Required lineage fields (always populated by pipeline)
- `source_vendor`, `source_file`, `source_row`, `load_run_id`, `ingested_at`

## Required business fields
- `plan_type` (medical/dental/vision)
- `provider` (vendor name, e.g. medical_a)
- `record_hash_raw` (dedup hash of content fields)

## Columns (summary)
### Meta / Lineage
- `source_vendor`: vendor identifier (dental/vision/medical_a/medical_b/medical_c)
- `source_file`: source filename
- `source_row`: row index in source
- `load_run_id`: pipeline run identifier
- `ingested_at`: ingestion timestamp (string ISO recommended)
- `source_extract_date`: optional extracted date from filename/path, if available

### De-dup
- `record_hash_raw`: hash computed BEFORE normalization, based on content columns only.
  Excludes: `source_*`, `load_run_id`, `ingested_at`, `source_extract_date`, and itself.

### IDs (raw identifiers)
- `group_id_raw`
- `subscriber_id_raw`
- `person_id_raw`
- `dependent_seq_raw`
- `ssn_hash_raw`

### Person (RAW)
- `first_name_raw`
- `last_name_raw`
- `dob_raw` (string as received; Medical C is constructed as YYYY-MM-DD string from split parts)
- `relationship_raw` (as received: e.g. Employee/Spouse/Dependent, or E/S/D, or EMP/SPS/DEP)

### Address (RAW values, canonical column names)
- `address_line1`, `city`, `state`, `zip`

### Plan (RAW)
- `plan_type` (medical/dental/vision)
- `provider` (vendor name)
- `plan_id`
- `plan_tier`

### Status (RAW)
- `is_active_raw` (as received: Y/1/true/ELIG/etc.)

### Optional
- `extra_payload`: JSON string containing unmapped/internal source fields.

## Minimal allowed derivations in RAW
- Structural renaming to canonical column names
- Medical C `dob_raw` can be constructed from `dob_year`, `dob_month`, `dob_day` into a single string.
- No trimming/casing/boolean/date parsing in RAW.

## Next stage (Normalization) - out of scope for RAW
- Parse `dob_raw` into a canonical Date field
- Normalize `relationship_raw` into canonical values (employee/spouse/dependent)
- Normalize `is_active_raw` into boolean if needed
- Apply business rules (e.g., vision/dental without medical, max one plan per employee per type)
