from __future__ import annotations

import csv
import json
import hashlib
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple

import pandas as pd
import yaml  # pip install pyyaml


# ---------- helpers: time ----------
def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


# ---------- helpers: sqlite schema ----------
def get_table_columns(conn: sqlite3.Connection, table: str) -> List[str]:
    cur = conn.execute(f"PRAGMA table_info({table});")
    cols = [r[1] for r in cur.fetchall()]
    if not cols:
        raise RuntimeError(f"Table not found or has no columns: {table}")
    return cols


# ---------- helpers: row access ----------
def get_by_path(obj: Any, path: str) -> Any:
    """
    Supports:
      - plain keys: "DOB"
      - dotted JSON paths: "name.first" (for medical_c JSONL)
    """
    if obj is None:
        return None
    if "." not in path:
        return obj.get(path) if isinstance(obj, dict) else None

    cur = obj
    for part in path.split("."):
        if not isinstance(cur, dict):
            return None
        cur = cur.get(part)
    return cur


def join_ymd_to_string(year: Any, month: Any, day: Any) -> Optional[str]:
    """
    Minimal RAW derivation allowed by contract:
    build a YYYY-MM-DD *string* (no date parsing).
    """
    if year is None or month is None or day is None:
        return None
    try:
        y = str(year).zfill(4)
        m = str(month).zfill(2)
        d = str(day).zfill(2)
        return f"{y}-{m}-{d}"
    except Exception:
        return None


# ---------- readers ----------
def read_csv_rows(path: Path, delimiter: str = ",") -> Iterator[Dict[str, Any]]:
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter=delimiter)
        for row in reader:
            # keep RAW-ish: strings in, empty->None
            yield {k: (v if v != "" else None) for k, v in row.items()}


def read_jsonl_rows(path: Path) -> Iterator[Dict[str, Any]]:
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def read_xlsx_rows(path: Path, sheet: str) -> Iterator[Dict[str, Any]]:
    df = pd.read_excel(path, sheet_name=sheet, dtype=str)
    df = df.where(df.notna(), None)
    for rec in df.to_dict(orient="records"):
        yield rec


# ---------- yaml config ----------
def load_vendor_config(path: Path) -> Dict[str, Any]:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


# ---------- canonical row build ----------
def build_canonical_row(cfg: Dict[str, Any], raw_row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Applies:
      - constants
      - mapping (supports dotted paths)
      - nulls
      - derivations (currently supports join_ymd_to_string for dob_raw)
      - extra_payload (selected fields)
    """
    out: Dict[str, Any] = {}

    # constants
    for k, v in (cfg.get("constants") or {}).items():
        out[k] = v

    # mapping
    for target_col, source_path in (cfg.get("mapping") or {}).items():
        out[target_col] = get_by_path(raw_row, source_path)

    # nulls
    for k in (cfg.get("nulls") or {}).keys():
        out[k] = None

    # derivations (e.g., medical_c dob_raw from dob_year/month/day)
    derivations = cfg.get("derivations") or {}
    for target_col, spec in derivations.items():
        if spec.get("type") == "join_ymd_to_string":
            y = get_by_path(raw_row, spec["year"])
            m = get_by_path(raw_row, spec["month"])
            d = get_by_path(raw_row, spec["day"])
            out[target_col] = join_ymd_to_string(y, m, d)

    # extra_payload: store only listed source fields as JSON
    extras = cfg.get("extra_payload") or []
    if extras:
        extra_obj = {k: get_by_path(raw_row, k) for k in extras}
        out["extra_payload"] = json.dumps(extra_obj, ensure_ascii=False)

    return out


# ---------- record_hash_raw ----------
EXCLUDE_FROM_HASH = {
    "source_vendor", "source_file", "source_row",
    "load_run_id", "ingested_at",
    "source_extract_date",
    "record_hash_raw",
}

def compute_record_hash_raw(canonical_row: Dict[str, Any], raw_staging_cols: List[str]) -> str:
    """
    Contract: hash of content fields only, excluding lineage + record_hash_raw itself.
    We use the table columns to decide the "content field universe", making it resilient to schema changes.
    """
    content_cols = [c for c in raw_staging_cols if c not in EXCLUDE_FROM_HASH]
    content_dict = {c: canonical_row.get(c) for c in content_cols}
    payload = json.dumps(content_dict, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


# ---------- main ingestion ----------
def ingest_stage1_hybrid(
    root: Path,
    load_run_id: str,
    *,
    db_path: Optional[Path] = None,
    yaml_dir: Optional[Path] = None,
) -> Dict[str, int]:
    """
    Hybrid Stage 1:
      - writes canonical RAW row into raw_staging (per contract + YAML mapping)
      - writes full original row JSON into raw_staging_payload (sidecar)
    """
    root = Path(root)
    input_dir = root / "input"
    output_dir = root / "output"
    output_dir.mkdir(parents=True, exist_ok=True)

    if db_path is None:
        db_path = output_dir / "warehouse.db"

    if yaml_dir is None:
        yaml_dir = root / "config" / "vendors"  # you can adjust to where you store YAMLs

    vendor_yaml_files = [
        yaml_dir / "dental.yaml",
        yaml_dir / "vision.yaml",
        yaml_dir / "medical_a.yaml",
        yaml_dir / "medical_b.yaml",
        yaml_dir / "medical_c.yaml",
    ]

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    try:
        raw_cols = get_table_columns(conn, "raw_staging")
        payload_cols = get_table_columns(conn, "raw_staging_payload")

        inserted: Dict[str, int] = {}

        for yml in vendor_yaml_files:
            cfg = load_vendor_config(yml)
            source_vendor = cfg["source_vendor"]
            filename = cfg["file"]
            fmt = cfg["format"]

            file_path = input_dir / filename
            if not file_path.exists():
                raise FileNotFoundError(f"Missing input file for {source_vendor}: {file_path}")

            # choose reader
            if fmt == "csv":
                rows = read_csv_rows(file_path, delimiter=",")
            elif fmt == "pipe_delimited":
                rows = read_csv_rows(file_path, delimiter=cfg.get("delimiter", "|"))
            elif fmt == "xlsx":
                rows = read_xlsx_rows(file_path, sheet=cfg["sheet"])
            elif fmt == "jsonl":
                rows = read_jsonl_rows(file_path)
            else:
                raise ValueError(f"Unsupported format '{fmt}' in {yml}")

            # prepared statements
            # raw_staging insert
            raw_insert_cols = [c for c in raw_cols if c in (set(raw_cols))]  # keep order from table
            raw_insert_sql = f"""
                INSERT INTO raw_staging ({", ".join(raw_insert_cols)})
                VALUES ({", ".join(["?"] * len(raw_insert_cols))})
            """

            # payload insert
            payload_insert_cols = [c for c in payload_cols if c in (set(payload_cols))]
            payload_insert_sql = f"""
                INSERT INTO raw_staging_payload ({", ".join(payload_insert_cols)})
                VALUES ({", ".join(["?"] * len(payload_insert_cols))})
            """

            cnt = 0
            batch_raw: List[Tuple[Any, ...]] = []
            batch_payload: List[Tuple[Any, ...]] = []

            ingested_at = utc_now_iso()

            for source_row, raw_row in enumerate(rows, start=1):
                canonical = build_canonical_row(cfg, raw_row)

                # add required lineage fields
                canonical["source_vendor"] = source_vendor
                canonical["source_file"] = filename
                canonical["source_row"] = source_row
                canonical["load_run_id"] = load_run_id
                canonical["ingested_at"] = ingested_at

                # compute record_hash_raw per contract
                canonical["record_hash_raw"] = compute_record_hash_raw(canonical, raw_cols)

                # build payload sidecar record (full original row)
                payload_rec = {
                    "load_run_id": load_run_id,
                    "source_vendor": source_vendor,
                    "source_file": filename,
                    "source_row": source_row,
                    "ingested_at": ingested_at,
                    "record_hash_raw": canonical["record_hash_raw"],
                    "raw_payload_json": json.dumps(raw_row, ensure_ascii=False),
                }

                batch_raw.append(tuple(canonical.get(c) for c in raw_insert_cols))
                batch_payload.append(tuple(payload_rec.get(c) for c in payload_insert_cols))
                cnt += 1

                if cnt % 1000 == 0:
                    conn.executemany(raw_insert_sql, batch_raw)
                    conn.executemany(payload_insert_sql, batch_payload)
                    batch_raw.clear()
                    batch_payload.clear()

            if batch_raw:
                conn.executemany(raw_insert_sql, batch_raw)
                conn.executemany(payload_insert_sql, batch_payload)

            conn.commit()
            inserted[source_vendor] = cnt

        return inserted

    finally:
        conn.close()
