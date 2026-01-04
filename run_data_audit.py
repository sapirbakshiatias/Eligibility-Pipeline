import sqlite3
import pandas as pd
from pathlib import Path
import sqlite3
import json



def run_full_audit():
	db_path = Path("output/warehouse.db")
	if not db_path.exists():
		print("Database not found!")
		return

	conn = sqlite3.connect(db_path)

	print("=" * 60)
	print(f"PIPELINE DATA AUDIT REPORT - {pd.Timestamp.now()}")
	print("=" * 60)

	# --- TEST 1: RAW STAGING INTEGRITY ---
	print("\n[TEST 1] RAW STAGING: Missing Fields per Vendor")
	raw_query = """
    SELECT 
        source_vendor,
        COUNT(*) AS total,
        SUM(CASE WHEN first_name_raw IS NULL OR first_name_raw = '' THEN 1 ELSE 0 END) AS missing_names,
        SUM(CASE WHEN dob_raw IS NULL OR dob_raw = '' THEN 1 ELSE 0 END) AS missing_dob,
        SUM(CASE WHEN address_line1 IS NULL OR address_line1 = '' THEN 1 ELSE 0 END) AS missing_address
    FROM raw_staging
    GROUP BY 1
    """
	print(pd.read_sql(raw_query, conn))

	# --- TEST 2: SILVER NORMALIZATION SUCCESS ---
	print("\n[TEST 2] SILVER LAYER: Normalization Success Rate")
	silver_query = """
    SELECT 
        source_vendor,
        COUNT(*) AS total,
        -- Check date normalization
        ROUND(100.0 * SUM(CASE WHEN dob_norm IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS date_success_pct,
        -- Check relationship mapping
        SUM(CASE WHEN relationship_norm = 'OTHER' THEN 1 ELSE 0 END) AS unknown_rels,
        -- Check name cleaning
        SUM(CASE WHEN first_name_norm = 'nonenone' OR first_name_norm IS NULL THEN 1 ELSE 0 END) AS failed_names
    FROM silver_members
    GROUP BY 1
    """
	print(pd.read_sql(silver_query, conn))

	# --- TEST 3: END-TO-END TRACING (SPOT CHECK) ---
	print("\n[TEST 3] SPOT CHECK: Random Trace (One per Vendor)")
	trace_query = """
    SELECT 
        source_vendor,
        first_name_raw, first_name_norm,
        dob_raw, dob_norm,
        relationship_raw, relationship_norm
    FROM silver_members
    GROUP BY source_vendor
    """
	print(pd.read_sql(trace_query, conn))
	import sqlite3
	import json

	def verify_medical_c_extraction():
		conn = sqlite3.connect("output/warehouse.db")
		cursor = conn.cursor()

		query = """
	    SELECT p.raw_payload_json, s.first_name_raw, s.address_line1, s.dob_raw
	    FROM raw_staging s
	    JOIN raw_staging_payload p ON s.record_hash_raw = p.record_hash_raw
	    WHERE s.source_vendor = 'medical_provider_c'
	    LIMIT 5
	    """

		rows = cursor.execute(query).fetchall()
		print(f"{'JSON Original Name':<25} | {'Extracted Name':<15} | {'Status'}")
		print("-" * 55)

		for payload_str, extracted_name, address, dob in rows:
			payload = json.loads(payload_str)
			original_name = payload.get('name', {}).get('first')

			status = "✅ SUCCESS" if original_name == extracted_name else "❌ FAILED"
			print(f"{str(original_name):<25} | {str(extracted_name):<15} | {status}")

		conn.close()

	verify_medical_c_extraction()

	conn.close()


if __name__ == "__main__":
	run_full_audit()
