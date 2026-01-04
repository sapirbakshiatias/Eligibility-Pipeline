import json
from pathlib import Path
import pytest
from pipeline.stage0_manifest import build_staging_manifest, generate_load_run_id


def _create_mock_files(input_dir: Path):
	"""Creates mock data files to verify row counting and manifest generation."""
	input_dir.mkdir(parents=True, exist_ok=True)

	# 1. Mock CSV (Medical A) - Header + 2 rows
	(input_dir / "medical_provider_a.csv").write_text("id,name\n1,John\n2,Jane")

	# 2. Mock JSONL (Medical C) - 1 valid row
	(input_dir / "medical_provider_c.jsonl").write_text('{"id": 3, "name": "Bob"}\n')

	# 3. Mock TXT (Medical B) - Header + 1 row (pipe delimited)
	(input_dir / "medical_provider_b.txt").write_text("id|name\n4|Alice")


def test_manifest_creation_success(tmp_path: Path):
	"""Verifies that a valid manifest is generated when input files exist."""
	# Arrange
	input_dir = tmp_path / "input"
	_create_mock_files(input_dir)
	load_run_id = generate_load_run_id()

	# Act
	manifest_path = build_staging_manifest(tmp_path, load_run_id)

	# Assert
	assert manifest_path.exists()
	latest_path = tmp_path / "output" / "staging_manifest_latest.json"
	assert latest_path.exists()

	with open(manifest_path, 'r', encoding='utf-8') as f:
		data = json.load(f)
		assert data["load_run_id"] == load_run_id

		# Check specific row count for medical_a
		med_a = next(entry for entry in data["files"] if entry["source_vendor"] == "medical_a")
		assert med_a["row_count_read"] == 2
		assert med_a["status"] == "success"


def test_manifest_marks_missing_files_as_failed(tmp_path: Path):
	"""Verifies that missing files are recorded as 'failed' in the manifest."""
	# Arrange (no files created)
	load_run_id = generate_load_run_id()

	# Act
	manifest_path = build_staging_manifest(tmp_path, load_run_id, require_non_empty=False)

	# Assert
	with open(manifest_path, 'r', encoding='utf-8') as f:
		data = json.load(f)
		# All files should have a failed status
		for entry in data["files"]:
			assert entry["status"] == "failed"
			assert "Missing expected input file" in entry["error"]