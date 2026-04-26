"""
app/audit/audit_log.py
-----------------------
SQLite-backed audit log for all DANGER-level query executions.

Schema: audit_events
  id          INTEGER PRIMARY KEY AUTOINCREMENT
  timestamp   TEXT     ISO-8601 UTC
  session_id  TEXT
  role        TEXT
  severity    TEXT     INFO | WARNING | DANGER
  sql_text    TEXT
  dialect     TEXT
  host        TEXT     DB host (no password stored)
  database    TEXT
  approved    INTEGER  1 = approved by Reviewer, 0 = rejected
"""

from __future__ import annotations

import sqlite3
import logging
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

_AUDIT_DB_PATH = Path("./audit.db")
_lock = threading.Lock()


def _get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(_AUDIT_DB_PATH), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_audit_db() -> None:
    """Create the audit_events table if it does not exist."""
    with _lock:
        conn = _get_conn()
        try:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS audit_events (
                    id         INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp  TEXT    NOT NULL,
                    session_id TEXT    NOT NULL DEFAULT '',
                    role       TEXT    NOT NULL DEFAULT 'admin',
                    severity   TEXT    NOT NULL DEFAULT 'INFO',
                    sql_text   TEXT    NOT NULL,
                    dialect    TEXT    NOT NULL DEFAULT 'mysql',
                    host       TEXT    NOT NULL DEFAULT '',
                    database   TEXT    NOT NULL DEFAULT '',
                    approved   INTEGER NOT NULL DEFAULT 1
                )
            """)
            conn.commit()
            logger.info("Audit DB initialised at %s", _AUDIT_DB_PATH)
        finally:
            conn.close()


def log_audit_event(
    *,
    session_id: str = "",
    role: str = "admin",
    severity: str = "INFO",
    sql_text: str,
    dialect: str = "mysql",
    host: str = "",
    database: str = "",
    approved: bool = True,
) -> None:
    """Insert one audit event.  Thread-safe."""
    ts = datetime.now(timezone.utc).isoformat()
    with _lock:
        conn = _get_conn()
        try:
            conn.execute(
                """
                INSERT INTO audit_events
                  (timestamp, session_id, role, severity, sql_text, dialect, host, database, approved)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (ts, session_id, role, severity, sql_text, dialect, host, database, int(approved)),
            )
            conn.commit()
        except Exception as exc:
            logger.error("Audit log write failed: %s", exc)
        finally:
            conn.close()


def get_audit_events(limit: int = 200, severity_filter: Optional[str] = None) -> list[dict]:
    """Return recent audit events as list of dicts (newest first)."""
    with _lock:
        conn = _get_conn()
        try:
            if severity_filter:
                rows = conn.execute(
                    "SELECT * FROM audit_events WHERE severity = ? ORDER BY id DESC LIMIT ?",
                    (severity_filter.upper(), limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM audit_events ORDER BY id DESC LIMIT ?",
                    (limit,),
                ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()


def classify_severity(sql: str) -> str:
    """
    Classify a SQL statement by safety severity.

    INFO    → SELECT only
    WARNING → UPDATE or DELETE WITH a WHERE clause; INSERT
    DANGER  → DROP, TRUNCATE, DELETE without WHERE, or any DDL
    """
    upper = sql.strip().upper()

    # DANGER patterns
    danger_keywords = ["DROP ", "TRUNCATE ", "ALTER TABLE", "CREATE TABLE", "CREATE INDEX"]
    if any(upper.startswith(kw) or f" {kw}" in upper for kw in danger_keywords):
        return "DANGER"

    # DELETE without WHERE → DANGER
    if upper.lstrip().startswith("DELETE") and "WHERE" not in upper:
        return "DANGER"

    # DML with WHERE
    if any(upper.lstrip().startswith(kw) for kw in ("UPDATE ", "DELETE ", "INSERT ")):
        return "WARNING"

    return "INFO"
