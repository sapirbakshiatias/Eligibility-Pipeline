from __future__ import annotations

import csv
import hashlib
import json
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from openpyxl import load_workbook


# -----------------------------
# Configuration (minimal, explicit)
# -----------------------------
# NOTE: This is intentionally small and stable.
# In later stages, we can derive these from YAML configs if desired.
EXPECTED_INPUTS = [
    {
        "source_vendor": "dental",
        "file_name": "dental_provider.xlsx",
        "format": "xlsx",
        "sheet_name": "eligibility",
    },
    {
        "source_vendor": "vision",
        "file_name": "vision_provider.csv",
        "format": "csv",
        "delimiter": ",",
        "has_header": True,
    },
    {
        "source_vendor": "medical_a",
        "file_name": "medical_provider_a.csv",
        "format": "csv",
        "delimiter": ",",
        "has_header": True,
    },
    {
        "source_vendor": "medical_b",
        "file_name": "medical_provider_b.txt",
        "format": "txt",
        "delimiter": "|",
        "has_header": True,
    },
    {
        "source_vendor": "medical_c",
        "file_name": "medical_provider_c.jsonl",
        "format": "jsonl",
    },
]


# -----------------------------
# Data model
# -----------------------------
@dataclass(frozen=True)
class ManifestFileEntry:
    source_vendor: str
    source_file: str
    relative_path: str
    size_bytes: int
    modified_time_utc: str
    sha256: str
    row_count_read: int
    status: str  # "success" | "failed"
    error: str | None


@dataclass(frozen=True)
class StagingManifest:
    load_run_id: str
    ingested_at_utc: str
    input_dir: str
    files: list[ManifestFileEntry]


# -----------------------------
# Helpers
# -----------------------------
def _utc_now_iso() -> str:
    """Return current UTC timestamp in ISO format (no microseconds)."""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def generate_load_run_id() -> str:
    """Generate a unique load_run_id for each pipeline run."""
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{ts}_{uuid4().hex[:8]}"


def compute_sha256(path: Path, chunk_size: int = 1024 * 1024) -> str:
    """Compute SHA256 checksum for a file in a streaming manner."""
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def file_modified_time_utc(path: Path) -> str:
    """Return file modified time as UTC ISO timestamp."""
    mtime = path.stat().st_mtime
    return datetime.fromtimestamp(mtime, tz=timezone.utc).replace(microsecond=0).isoformat()


def count_rows_csv_like(path: Path, delimiter: str, has_header: bool = True) -> int:
    """
    Count rows in CSV/TXT files. If has_header=True, header is excluded from count.
    """
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f, delimiter=delimiter)
        count = 0
        for i, _ in enumerate(reader, start=1):
            # Skip empty lines safely
            # (csv.reader may return [] for blank lines)
            if not _:
                continue
            if has_header and i == 1:
                continue
            count += 1
    return count


def count_rows_jsonl(path: Path) -> int:
    """Count non-empty JSON Lines (one JSON object per line)."""
    count = 0
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                count += 1
    return count


def count_rows_xlsx(path: Path, sheet_name: str | None = None) -> int:
    """
    Count data rows in an Excel sheet.
    We assume first row is a header and exclude it.
    We count only rows that contain at least one non-empty cell.
    """
    wb = load_workbook(filename=path, read_only=True, data_only=True)

    if sheet_name and sheet_name in wb.sheetnames:
        ws = wb[sheet_name]
    else:
        ws = wb.active

    count = 0
    # Start at row 2 to skip header row
    for row in ws.iter_rows(min_row=2, values_only=True):
        if any(v is not None and str(v).strip() != "" for v in row):
            count += 1

    wb.close()
    return count


def count_rows_by_format(path: Path, fmt: str, meta: dict[str, Any]) -> int:
    """Dispatch row counting by file format."""
    if fmt in ("csv", "txt"):
        return count_rows_csv_like(
            path=path,
            delimiter=meta.get("delimiter", ","),
            has_header=meta.get("has_header", True),
        )
    if fmt == "jsonl":
        return count_rows_jsonl(path)
    if fmt == "xlsx":
        return count_rows_xlsx(path, sheet_name=meta.get("sheet_name"))
    raise ValueError(f"Unsupported format for row counting: {fmt}")


# -----------------------------
# Stage 0: Raw Archive Manifest
# -----------------------------
def build_staging_manifest(root: Path, load_run_id: str, require_non_empty: bool = True) -> Path:
    """
    Build and write a staging manifest for all expected input files.

    Output:
      - output/manifests/manifest_<load_run_id>.json
      - output/staging_manifest_latest.json

    The manifest is meant for human debugging and reproducibility:
    it records WHICH files were read, their checksums, and row counts.
    """
    input_dir = root / "input"
    output_dir = root / "output"
    manifests_dir = output_dir / "manifests"

    output_dir.mkdir(parents=True, exist_ok=True)
    manifests_dir.mkdir(parents=True, exist_ok=True)

    ingested_at = _utc_now_iso()

    entries: list[ManifestFileEntry] = []

    for spec in EXPECTED_INPUTS:
        vendor = spec["source_vendor"]
        file_name = spec["file_name"]
        fmt = spec["format"]

        file_path = input_dir / file_name

        try:
            if not file_path.exists():
                raise FileNotFoundError(f"Missing expected input file: {file_path}")

            size_bytes = file_path.stat().st_size
            mtime_utc = file_modified_time_utc(file_path)
            checksum = compute_sha256(file_path)
            row_count = count_rows_by_format(file_path, fmt, spec)

            if require_non_empty and row_count == 0:
                raise ValueError(f"File has zero data rows: {file_path}")

            entries.append(
                ManifestFileEntry(
                    source_vendor=vendor,
                    source_file=file_name,
                    relative_path=str(file_path.relative_to(root)),
                    size_bytes=size_bytes,
                    modified_time_utc=mtime_utc,
                    sha256=checksum,
                    row_count_read=row_count,
                    status="success",
                    error=None,
                )
            )

        except Exception as e:
            entries.append(
                ManifestFileEntry(
                    source_vendor=vendor,
                    source_file=file_name,
                    relative_path=str(file_path.relative_to(root)) if file_path.exists() else str(Path("input") / file_name),
                    size_bytes=file_path.stat().st_size if file_path.exists() else 0,
                    modified_time_utc=file_modified_time_utc(file_path) if file_path.exists() else "",
                    sha256=compute_sha256(file_path) if file_path.exists() else "",
                    row_count_read=0,
                    status="failed",
                    error=str(e),
                )
            )

    manifest = StagingManifest(
        load_run_id=load_run_id,
        ingested_at_utc=ingested_at,
        input_dir=str(input_dir.relative_to(root)),
        files=entries,
    )

    manifest_path = manifests_dir / f"manifest_{load_run_id}.json"
    latest_path = output_dir / "staging_manifest_latest.json"

    manifest_path.write_text(json.dumps(asdict(manifest), ensure_ascii=False, indent=2), encoding="utf-8")
    latest_path.write_text(json.dumps(asdict(manifest), ensure_ascii=False, indent=2), encoding="utf-8")

    return manifest_path
