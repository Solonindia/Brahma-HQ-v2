import datetime as dt
import json
import os
import sqlite3
import tempfile
from typing import Dict, List, Tuple

from .config import (
    GCS_BUCKET, MASTER_ROOT, STANDARDS_ROOT, RELEASE_ROOT, ACTIVE_OBJECT,
    SCHEMA_VERSION, PRODUCT_DB_NAME
)
from .gcs_utils import (
    list_objects, read_bytes, read_text, write_bytes, write_text, write_json, copy_object
)

REQUIRED_FIELDS = ["mfr", "model"]  # minimum keys


def _quote_ident(name: str) -> str:
    """Safe identifier quoting for SQLite."""
    safe = (name or "").replace('"', '""')
    return f'"{safe}"'


def _ensure_columns(cur: sqlite3.Cursor, table: str, cols: List[str]) -> None:
    """ALTER TABLE ADD COLUMN for any missing columns (as TEXT).

    We store dynamic columns as TEXT to preserve mixed values
    (e.g., "45±2 °C", "1500 VDC", "5400 Pa").
    """
    cur.execute(f"PRAGMA table_info({_quote_ident(table)})")
    existing = {r[1] for r in cur.fetchall()}  # (cid, name, type, ...)
    for c in cols:
        if c in existing:
            continue
        # TEXT default '' to avoid NULL in selects
        cur.execute(
            f"ALTER TABLE {_quote_ident(table)} ADD COLUMN {_quote_ident(c)} TEXT DEFAULT ''"
        )


def _to_text(v) -> str:
    """Coerce any value into TEXT (never None)."""
    if v is None:
        return ""
    if isinstance(v, (dict, list)):
        return json.dumps(v, ensure_ascii=False)
    return str(v)

def _utc_release_id() -> str:
    # db_release_YYYYMMDD_HHMMSSZ
    return dt.datetime.utcnow().strftime("db_release_%Y%m%d_%H%M%SZ")

def _safe_json_load(raw: bytes, path: str) -> dict:
    try:
        return json.loads(raw.decode("utf-8"))
    except Exception as e:
        raise RuntimeError(f"Invalid JSON in {path}: {e}")

def _collect_master_jsons() -> List[str]:
    # list all JSON under 03_MasterData/modules/...
    all_objs = list_objects(GCS_BUCKET, MASTER_ROOT + "/")
    return [o for o in all_objs if o.lower().endswith(".json")]

def _validate_rows(rows: List[dict]) -> Tuple[List[dict], int]:
    ok = []
    bad = 0
    for r in rows:
        if not isinstance(r, dict):
            bad += 1
            continue
        if any((k not in r or r.get(k) in [None, ""]) for k in REQUIRED_FIELDS):
            bad += 1
            continue
        ok.append(r)
    return ok, bad

def _build_sqlite(rows: List[dict], release_id: str) -> bytes:
    """Build SQLite with dynamic schema evolution.

    Requirements satisfied:
      1) If new datasheets introduce new columns, we auto-add them (ALTER TABLE).
      2) Mixed int/str values are preserved by storing dynamic columns as TEXT.
      3) Avoid NULLs: every stored cell is TEXT with DEFAULT '' and we coerce None -> ''.

    Tables:
      - modules (one row per module)
      - module_variants (0..N rows per module; STC/NOCT etc)
      - meta
    """
    with tempfile.TemporaryDirectory() as td:
        db_path = os.path.join(td, PRODUCT_DB_NAME)
        con = sqlite3.connect(db_path)
        cur = con.cursor()

        # Base tables (dynamic columns are added later as needed)
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS modules (
                key TEXT PRIMARY KEY,
                mfr TEXT DEFAULT '',
                model TEXT DEFAULT '',
                source_pdf TEXT DEFAULT '',
                source_metadata TEXT DEFAULT '',
                payload_json TEXT DEFAULT ''
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS module_variants (
                variant_id INTEGER PRIMARY KEY AUTOINCREMENT,
                module_key TEXT NOT NULL,
                variant_key TEXT NOT NULL,
                payload_json TEXT DEFAULT '',
                UNIQUE(module_key, variant_key)
            )
            """
        )

        cur.execute("CREATE INDEX IF NOT EXISTS idx_modules_mfr ON modules(mfr)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_modules_model ON modules(model)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_variants_module ON module_variants(module_key)")

        # Determine the union of module-level keys (excluding variants)
        module_dynamic_keys = set()
        variant_dynamic_keys = set()
        for r in rows:
            for k in r.keys():
                if k in {"variants"}:
                    continue
                if k in {"key"}:
                    continue
                # base cols are already present
                if k in {"mfr", "model", "source_pdf", "source_metadata", "payload_json"}:
                    continue
                module_dynamic_keys.add(str(k))

            for vv in (r.get("variants") or []):
                if isinstance(vv, dict):
                    for kk in vv.keys():
                        if kk in {"payload_json"}:
                            continue
                        variant_dynamic_keys.add(str(kk))

        # ensure all dynamic columns exist as TEXT
        _ensure_columns(cur, "modules", sorted(module_dynamic_keys))
        _ensure_columns(cur, "module_variants", sorted(variant_dynamic_keys))

        # Upsert modules and variants
        for r in rows:
            mfr = _to_text(r.get("mfr")).strip()
            model = _to_text(r.get("model")).strip()
            key = f"{mfr.lower()}::{model.lower()}"

            payload_json = json.dumps(r, ensure_ascii=False)

            # Build insert dict for modules
            row_data = {
                "key": key,
                "mfr": mfr,
                "model": model,
                "source_pdf": _to_text(r.get("source_pdf")),
                "source_metadata": _to_text(r.get("source_metadata")),
                "payload_json": payload_json,
            }

            # dynamic fields (TEXT) - coerce None -> ''
            for k in module_dynamic_keys:
                row_data[k] = _to_text(r.get(k, ""))

            cols = list(row_data.keys())
            placeholders = ",".join(["?"] * len(cols))
            col_sql = ",".join([_quote_ident(c) for c in cols])
            update_sql = ",".join([f"{_quote_ident(c)}=excluded.{_quote_ident(c)}" for c in cols if c != "key"])

            cur.execute(
                f"INSERT INTO modules ({col_sql}) VALUES ({placeholders}) "
                f"ON CONFLICT(key) DO UPDATE SET {update_sql}",
                [row_data[c] for c in cols],
            )

            # Variants
            variants = r.get("variants") or []
            if isinstance(variants, list):
                for vv in variants:
                    if not isinstance(vv, dict):
                        continue
                    # Prefer pmax_w as stable id; else hash of payload
                    pmax = _to_text(vv.get("pmax_w")).strip()
                    variant_key = pmax if pmax else json.dumps(vv, ensure_ascii=False)

                    v_payload = json.dumps(vv, ensure_ascii=False)
                    v_data = {
                        "module_key": key,
                        "variant_key": variant_key,
                        "payload_json": v_payload,
                    }
                    for kk in variant_dynamic_keys:
                        v_data[kk] = _to_text(vv.get(kk, ""))

                    v_cols = list(v_data.keys())
                    v_placeholders = ",".join(["?"] * len(v_cols))
                    v_col_sql = ",".join([_quote_ident(c) for c in v_cols])
                    v_update_sql = ",".join(
                        [f"{_quote_ident(c)}=excluded.{_quote_ident(c)}" for c in v_cols if c not in {"variant_id"}]
                    )

                    cur.execute(
                        f"INSERT INTO module_variants ({v_col_sql}) VALUES ({v_placeholders}) "
                        f"ON CONFLICT(module_key, variant_key) DO UPDATE SET {v_update_sql}",
                        [v_data[c] for c in v_cols],
                    )

        # meta table
        cur.execute("""
        CREATE TABLE IF NOT EXISTS meta (
            release_id TEXT,
            created_at_utc TEXT,
            schema_version TEXT,
            count_modules INTEGER
        )
        """)
        cur.execute("DELETE FROM meta")
        cur.execute(
            "INSERT INTO meta(release_id, created_at_utc, schema_version, count_modules) VALUES(?,?,?,?)",
            (release_id, dt.datetime.utcnow().isoformat() + "Z", SCHEMA_VERSION, len(rows))
        )

        con.commit()
        con.close()

        with open(db_path, "rb") as f:
            return f.read()

def publish_release(release_notes: str, dry_run: bool = False) -> Dict:
    release_id = _utc_release_id()
    release_prefix = f"{RELEASE_ROOT}/{release_id}"

    # 1) load master JSONs
    paths = _collect_master_jsons()
    raw_rows = []
    for p in paths:
        raw = read_bytes(GCS_BUCKET, p)
        raw_rows.append(_safe_json_load(raw, p))

    rows, bad_count = _validate_rows(raw_rows)

    # 2) build sqlite
    sqlite_bytes = _build_sqlite(rows, release_id)

    # 3) standards YAMLs copy list
    standards = [o for o in list_objects(GCS_BUCKET, STANDARDS_ROOT + "/")
                 if o.lower().endswith((".yaml", ".yml"))]

    # output objects
    out_sqlite = f"{release_prefix}/compiled/{PRODUCT_DB_NAME}"
    out_schema = f"{release_prefix}/schema_version.json"
    out_notes = f"{release_prefix}/release_notes.md"
    out_manifest = f"{release_prefix}/manifest.json"

    if dry_run:
        return {
            "status": "dry_run",
            "release_id": release_id,
            "release_prefix": release_prefix,
            "active_object": ACTIVE_OBJECT,
            "counts": {
                "master_json_total": len(paths),
                "master_rows_valid": len(rows),
                "master_rows_invalid": bad_count,
                "standards_files": len(standards),
            },
            "outputs": {
                "sqlite": out_sqlite,
                "schema_version": out_schema,
                "release_notes": out_notes,
                "manifest": out_manifest
            }
        }

    # 4) write sqlite + release files
    write_bytes(GCS_BUCKET, out_sqlite, sqlite_bytes, content_type="application/x-sqlite3")

    write_json(GCS_BUCKET, out_schema, {"schema_version": SCHEMA_VERSION, "release_id": release_id})
    write_text(GCS_BUCKET, out_notes, release_notes, content_type="text/markdown")

    # manifest (for UI / debugging)
    manifest = {
        "release_id": release_id,
        "created_at_utc": dt.datetime.utcnow().isoformat() + "Z",
        "schema_version": SCHEMA_VERSION,
        "counts": {
            "master_json_total": len(paths),
            "master_rows_valid": len(rows),
            "master_rows_invalid": bad_count,
            "standards_files": len(standards),
        },
        "outputs": {
            "sqlite": out_sqlite,
            "schema_version": out_schema,
            "release_notes": out_notes
        },
        "standards_files": standards,
    }
    write_json(GCS_BUCKET, out_manifest, manifest)

    # 5) copy standards yaml into release folder
    for s in standards:
        rel_name = s.replace(STANDARDS_ROOT + "/", "")
        dst = f"{release_prefix}/02_Databases/Standards/{rel_name}"
        copy_object(GCS_BUCKET, s, dst)

    # 6) update ACTIVE pointer
    write_text(GCS_BUCKET, ACTIVE_OBJECT, release_id + "\n")

    return {
        "status": "released",
        "release_id": release_id,
        "release_prefix": release_prefix,
        "active_object": ACTIVE_OBJECT,
        "counts": manifest["counts"],
        "outputs": {
            "sqlite": out_sqlite,
            "schema_version": out_schema,
            "release_notes": out_notes,
            "manifest": out_manifest,
            "active": ACTIVE_OBJECT
        }
    }
