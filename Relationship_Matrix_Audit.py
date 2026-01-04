import sqlite3
import pandas as pd
from pathlib import Path

def audit_dob_normalization():
	root = Path(__file__).resolve().parent
	db_path = root / "output" / "warehouse.db"
	conn = sqlite3.connect(db_path)

	# Query to check unique date formats and their normalized results
	query = """
    SELECT 
        source_vendor,
        dob_raw,
        dob_norm,
        COUNT(*) as record_count
    FROM silver_members
    GROUP BY source_vendor, dob_raw, dob_norm
    ORDER BY source_vendor, record_count DESC;
    """

	df = pd.read_sql(query, conn)
	conn.close()

	print("\n" + "=" * 85)
	print("DEEP AUDIT: DATE OF BIRTH (DOB) NORMALIZATION MATRIX")
	print("=" * 85)

	for vendor in df['source_vendor'].unique():
		print(f"\n[ Vendor: {vendor.upper()} ]")
		vendor_df = df[df['source_vendor'] == vendor].copy()

		# We show a sample of successes and all failures (NULLs)
		success_sample = vendor_df[vendor_df['dob_norm'].notnull()].head(3)
		failure_sample = vendor_df[vendor_df['dob_norm'].isnull()]

		combined = pd.concat([success_sample, failure_sample])
		print(combined[['dob_raw', 'dob_norm', 'record_count']].to_string(index=False))

		if not failure_sample.empty:
			print(f"!!! ALERT: Found {len(failure_sample)} unparsed date formats for this vendor.")
		print("-" * 45)


###################
def audit_relationship_mapping():
	# Setup paths
	root = Path(__file__).resolve().parent
	db_path = root / "output" / "warehouse.db"
	conn = sqlite3.connect(db_path)

	# Query to see EVERY unique mapping per vendor
	query = """
    SELECT 
        source_vendor,
        relationship_raw,
        relationship_norm,
        COUNT(*) as record_count
    FROM silver_members
    GROUP BY source_vendor, relationship_raw, relationship_norm
    ORDER BY source_vendor, record_count DESC;
    """

	df = pd.read_sql(query, conn)
	conn.close()

	print("\n" + "=" * 80)
	print("DEEP AUDIT: FULL RELATIONSHIP MAPPING MATRIX PER VENDOR")
	print("=" * 80)

	# Iterate by vendor for better readability
	for vendor in df['source_vendor'].unique():
		print(f"\n[ Vendor: {vendor.upper()} ]")
		vendor_df = df[df['source_vendor'] == vendor].copy()
		print(vendor_df[['relationship_raw', 'relationship_norm', 'record_count']].to_string(index=False))
		print("-" * 40)


if __name__ == "__main__":
	audit_relationship_mapping()
	audit_dob_normalization()
