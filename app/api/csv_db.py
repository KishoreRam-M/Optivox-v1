"""
app/api/csv_db.py
-----------------
Agentic CSV-to-Database Engine

Endpoints:
  POST   /api/csvdb/upload            — Upload CSV(s) + company name → create company DB
  GET    /api/csvdb/list              — List all company databases
  GET    /api/csvdb/{db_id}/schema    — Return full schema for a database
  POST   /api/csvdb/{db_id}/query     — Run SQL query against a company database
  GET    /api/csvdb/{db_id}/preview/{table} — Preview first 50 rows of a table
  DELETE /api/csvdb/{db_id}           — Delete a company database

Design:
  - Each company gets an isolated SQLite file at ./csv_databases/{db_id}.db
  - Type inference: INTEGER, REAL, DATE (stored as TEXT), TEXT
  - Table names are sanitized from the CSV filename
  - Multiple CSVs → multiple tables in the same database
"""

from __future__ import annotations

import csv
import io
import logging
import os
import re
import sqlite3
import time
import uuid
from pathlib import Path
from typing import Any

from fastapi import APIRouter, File, Form, HTTPException, UploadFile
from pydantic import BaseModel

logger = logging.getLogger("OptiVox.CsvDB")

router = APIRouter(prefix="/api/csvdb", tags=["CSV Database"])

# ── Storage directory ──────────────────────────────────────────────────────

CSV_DB_DIR = Path(os.environ.get("CSV_DB_DIR", "./csv_databases"))


def _ensure_db_dir() -> None:
    """Create the CSV DB storage directory if it doesn't exist. Called lazily."""
    CSV_DB_DIR.mkdir(parents=True, exist_ok=True)

# ── Metadata registry (in-memory, rebuilt on startup from disk) ─────────────

_registry: dict[str, dict[str, Any]] = {}


def _registry_path() -> Path:
    return CSV_DB_DIR / "_registry.json"


def _load_registry() -> None:
    """Load registry from JSON file on disk."""
    import json
    _ensure_db_dir()
    p = _registry_path()
    if p.exists():
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
            _registry.update(data)
            # Prune entries whose DB file no longer exists
            dead = [k for k, v in _registry.items() if not Path(v["db_path"]).exists()]
            for k in dead:
                _registry.pop(k, None)
        except Exception as exc:
            logger.warning("Could not load CSV DB registry: %s", exc)


def _save_registry() -> None:
    import json
    try:
        _registry_path().write_text(
            json.dumps(_registry, indent=2, default=str), encoding="utf-8"
        )
    except Exception as exc:
        logger.warning("Could not save CSV DB registry: %s", exc)


# Load registry on module import
_load_registry()


# ── Type inference ─────────────────────────────────────────────────────────

_DATE_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}$|"
    r"^\d{2}/\d{2}/\d{4}$|"
    r"^\d{2}-\d{2}-\d{4}$"
)


def _infer_column_type(samples: list[str]) -> str:
    """Infer SQLite column type from a sample of non-empty string values."""
    non_null = [s.strip() for s in samples if s.strip()]
    if not non_null:
        return "TEXT"

    # Try INTEGER
    try:
        for v in non_null:
            int(v.replace(",", ""))
        return "INTEGER"
    except ValueError:
        pass

    # Try REAL
    try:
        for v in non_null:
            float(v.replace(",", ""))
        return "REAL"
    except ValueError:
        pass

    # Try DATE
    if all(_DATE_RE.match(v) for v in non_null[:20]):
        return "TEXT"  # Store dates as TEXT but tag it

    return "TEXT"


def _sanitize_name(name: str) -> str:
    """Sanitize table/column names to be valid SQLite identifiers."""
    name = re.sub(r"[^\w]", "_", name.strip())
    name = re.sub(r"_+", "_", name).strip("_")
    if name and name[0].isdigit():
        name = "col_" + name
    return name.lower() or "col"


# ── CSV Parsing ────────────────────────────────────────────────────────────

def _parse_csv(content: bytes) -> tuple[list[str], list[list[str]]]:
    """Parse CSV bytes into (headers, rows)."""
    text = content.decode("utf-8-sig", errors="replace")
    # Auto-detect delimiter
    sniffer = csv.Sniffer()
    try:
        dialect = sniffer.sniff(text[:4096], delimiters=",\t;|")
    except csv.Error:
        dialect = csv.excel

    reader = csv.reader(io.StringIO(text), dialect=dialect)
    rows = list(reader)
    if not rows:
        raise ValueError("CSV file is empty.")

    headers = [_sanitize_name(h) for h in rows[0]]
    # Deduplicate headers
    seen: dict[str, int] = {}
    deduped = []
    for h in headers:
        if h in seen:
            seen[h] += 1
            deduped.append(f"{h}_{seen[h]}")
        else:
            seen[h] = 0
            deduped.append(h)

    data_rows = rows[1:]
    return deduped, data_rows


def _infer_schema(headers: list[str], rows: list[list[str]]) -> list[dict[str, str]]:
    """Return list of {name, type} dicts for each column."""
    schema = []
    for i, col in enumerate(headers):
        samples = [row[i] if i < len(row) else "" for row in rows[:500]]
        col_type = _infer_column_type(samples)
        schema.append({"name": col, "type": col_type})
    return schema


# ── Database creation ──────────────────────────────────────────────────────

def _create_company_db(
    db_id: str,
    company_name: str,
    tables: list[dict[str, Any]],
) -> str:
    """
    Create a SQLite database for a company and seed it with CSV data.

    tables: [{"table_name": str, "columns": [...], "rows": [...]}]

    Returns the path to the created DB file.
    """
    _ensure_db_dir()
    db_path = str(CSV_DB_DIR / f"{db_id}.db")
    conn = sqlite3.connect(db_path)
    try:
        for tbl in tables:
            tbl_name = tbl["table_name"]
            columns = tbl["columns"]  # [{name, type}]
            rows = tbl["rows"]

            # Build CREATE TABLE
            col_defs = [f'"{c["name"]}" {c["type"]}' for c in columns]
            create_sql = f'CREATE TABLE IF NOT EXISTS "{tbl_name}" (_row_id INTEGER PRIMARY KEY AUTOINCREMENT, {", ".join(col_defs)});'
            conn.execute(create_sql)

            # Bulk insert
            if rows:
                placeholders = ", ".join(["?"] * len(columns))
                col_names = ", ".join(f'"{c["name"]}"' for c in columns)
                insert_sql = f'INSERT INTO "{tbl_name}" ({col_names}) VALUES ({placeholders});'
                cleaned_rows = []
                for row in rows:
                    padded = (row + [""] * len(columns))[: len(columns)]
                    cleaned = []
                    for val, col in zip(padded, columns):
                        v = val.strip()
                        if not v:
                            cleaned.append(None)
                        elif col["type"] == "INTEGER":
                            try:
                                cleaned.append(int(v.replace(",", "")))
                            except ValueError:
                                cleaned.append(v)
                        elif col["type"] == "REAL":
                            try:
                                cleaned.append(float(v.replace(",", "")))
                            except ValueError:
                                cleaned.append(v)
                        else:
                            cleaned.append(v)
                    cleaned_rows.append(cleaned)

                conn.executemany(insert_sql, cleaned_rows)

        conn.commit()
    finally:
        conn.close()

    return db_path


# ── Pydantic models ────────────────────────────────────────────────────────

class QueryRequest(BaseModel):
    sql: str
    limit: int | None = 500


# ── Helpers ────────────────────────────────────────────────────────────────

def _get_db(db_id: str) -> dict[str, Any]:
    """Fetch registry entry or raise 404."""
    entry = _registry.get(db_id)
    if not entry:
        raise HTTPException(status_code=404, detail=f"Database '{db_id}' not found.")
    if not Path(entry["db_path"]).exists():
        raise HTTPException(status_code=404, detail=f"Database file for '{db_id}' is missing.")
    return entry


def _get_live_schema(db_path: str) -> dict[str, Any]:
    """Read actual table/column info from a SQLite file."""
    conn = sqlite3.connect(db_path)
    try:
        tables = {}
        cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;")
        for (tbl_name,) in cursor.fetchall():
            if tbl_name.startswith("_"):
                continue
            col_cursor = conn.execute(f'PRAGMA table_info("{tbl_name}");')
            cols = [{"name": row[1], "type": row[2]} for row in col_cursor.fetchall() if row[1] != "_row_id"]
            count = conn.execute(f'SELECT COUNT(*) FROM "{tbl_name}";').fetchone()[0]
            tables[tbl_name] = {"columns": cols, "row_count": count}
        return tables
    finally:
        conn.close()


def _run_query(db_path: str, sql: str, limit: int = 500) -> dict[str, Any]:
    """Execute a SELECT query and return columns + rows."""
    # Safety: block destructive statements
    upper = sql.strip().upper()
    forbidden = ("DROP", "DELETE", "TRUNCATE", "ALTER", "CREATE", "INSERT", "UPDATE", "PRAGMA", "ATTACH", "DETACH")
    for kw in forbidden:
        if re.match(rf"^\s*{kw}\b", upper):
            raise ValueError(
                f"Statement type '{kw}' is not allowed. "
                "This database is read-only — only SELECT queries are supported."
            )

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        # Apply LIMIT if not already present
        if "LIMIT" not in upper:
            sql = sql.rstrip("; \n") + f" LIMIT {limit}"
        cursor = conn.execute(sql)
        if cursor.description:
            columns = [d[0] for d in cursor.description]
            rows = [list(row) for row in cursor.fetchall()]
            return {"columns": columns, "rows": rows, "row_count": len(rows)}
        return {"columns": [], "rows": [], "row_count": 0}
    finally:
        conn.close()


# ── Routes ─────────────────────────────────────────────────────────────────

@router.post("/upload")
async def upload_csv(
    company_name: str = Form(...),
    description: str = Form(""),
    files: list[UploadFile] = File(...),
):
    """
    Upload one or more CSV files and create a company-scoped database.
    Each CSV file becomes a separate table named after the filename.
    """
    if not company_name.strip():
        raise HTTPException(status_code=400, detail="company_name is required.")
    if not files:
        raise HTTPException(status_code=400, detail="At least one CSV file is required.")

    db_id = str(uuid.uuid4())[:8]
    tables_meta = []
    tables_data = []

    for upload in files:
        filename = upload.filename or "data.csv"
        raw_name = Path(filename).stem
        table_name = _sanitize_name(raw_name) or "table1"

        content = await upload.read()
        if not content:
            raise HTTPException(status_code=400, detail=f"File '{filename}' is empty.")

        try:
            headers, rows = _parse_csv(content)
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"Failed to parse '{filename}': {exc}")

        if not headers:
            raise HTTPException(status_code=400, detail=f"'{filename}' has no columns.")

        columns = _infer_schema(headers, rows)
        tables_data.append({"table_name": table_name, "columns": columns, "rows": rows})
        tables_meta.append({
            "table_name": table_name,
            "columns": columns,
            "row_count": len(rows),
            "original_filename": filename,
        })

    try:
        db_path = _create_company_db(db_id, company_name, tables_data)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Database creation failed: {exc}")

    entry = {
        "db_id": db_id,
        "company_name": company_name,
        "description": description,
        "db_path": db_path,
        "tables": tables_meta,
        "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "total_rows": sum(t["row_count"] for t in tables_meta),
    }
    _registry[db_id] = entry
    _save_registry()

    logger.info("Created company DB '%s' for '%s' with %d table(s)", db_id, company_name, len(tables_meta))

    return {
        "status": "ok",
        "db_id": db_id,
        "company_name": company_name,
        "tables": tables_meta,
        "message": f"Database created with {len(tables_meta)} table(s) and {entry['total_rows']:,} total rows.",
    }


@router.get("/list")
async def list_databases():
    """List all company databases."""
    items = []
    for db_id, entry in _registry.items():
        if Path(entry["db_path"]).exists():
            items.append({
                "db_id": db_id,
                "company_name": entry["company_name"],
                "description": entry.get("description", ""),
                "table_count": len(entry.get("tables", [])),
                "total_rows": entry.get("total_rows", 0),
                "created_at": entry.get("created_at", ""),
            })
    return {"status": "ok", "databases": sorted(items, key=lambda x: x["created_at"], reverse=True)}


@router.get("/{db_id}/schema")
async def get_schema(db_id: str):
    """Return full schema for a company database."""
    entry = _get_db(db_id)
    schema = _get_live_schema(entry["db_path"])
    return {
        "status": "ok",
        "db_id": db_id,
        "company_name": entry["company_name"],
        "schema": schema,
    }


@router.post("/{db_id}/query")
async def run_query(db_id: str, req: QueryRequest):
    """Execute a SQL query against a company database."""
    entry = _get_db(db_id)
    try:
        result = _run_query(entry["db_path"], req.sql, limit=min(req.limit or 500, 2000))
        return {"status": "ok", **result}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except sqlite3.Error as exc:
        raise HTTPException(status_code=400, detail=f"SQL error: {exc}")
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Query failed: {exc}")


@router.get("/{db_id}/preview/{table_name}")
async def preview_table(db_id: str, table_name: str, limit: int = 50):
    """Preview the first N rows of a table."""
    entry = _get_db(db_id)
    capped = min(limit, 500)
    try:
        # BUG-6: LIMIT is baked into the SQL; don't also pass limit param (it would be ignored anyway)
        result = _run_query(
            entry["db_path"],
            f'SELECT * FROM "{table_name}" LIMIT {capped}',
        )
        return {"status": "ok", **result}
    except sqlite3.Error as exc:
        raise HTTPException(status_code=400, detail=f"SQL error: {exc}")


@router.delete("/{db_id}")
async def delete_database(db_id: str):
    """Delete a company database and remove it from the registry."""
    entry = _get_db(db_id)
    db_path = Path(entry["db_path"])
    try:
        if db_path.exists():
            db_path.unlink()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Could not delete file: {exc}")

    _registry.pop(db_id, None)
    _save_registry()
    return {"status": "ok", "message": f"Database '{db_id}' deleted."}
