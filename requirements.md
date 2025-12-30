# Take-Home Exercise: Eligibility Ingestion Pipeline (Junior Data Engineer)

## Inputs
- 5 vendor eligibility files in `input/` (3 medical, 1 dental, 1 vision)
- Vendor data dictionaries in `docs/`
- Target unified schema in `target_schema.sql` and `target_schema.json`

## Rules
- Filter out members under 18 as of **2025-01-01**
- Filter out members who have dental/vision but **no medical**
- Employees can have **at most one** plan per type (medical/dental/vision)

## Data quality / edge cases included
- invalid DOBs (multiple formats)
- missing required field(s) on some records
- inactive flags in one medical source
- conflicting duplicate plan rows for a small set of employees
- standalone dental-only and vision-only members that should be filtered out

## Deliverables
- Unified output dataset (CSV or Parquet)
- Report file(s) with counts + rejection reasons
- README with how to run + how you’d handle:
  - adding a new vendor file
  - schema changes
