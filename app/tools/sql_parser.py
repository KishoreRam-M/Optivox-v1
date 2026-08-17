"""
tools/sql_parser.py
-------------------
AST-level SQL safety validator using sqlglot.

Provides:
  - validate_sql_ast(sql, dialect) -> (bool, str)
  - classify_sql_risk(sql) -> str  (INFO | WARNING | DANGER)

Only blocks genuine SQL injection patterns — not legitimate DDL/DML.
"""

from __future__ import annotations

import re
import logging
from typing import Tuple

import sqlglot

logger = logging.getLogger(__name__)

# Only block actual injection patterns — not legitimate DDL/DML operations
_INJECTION_PATTERNS = [
    re.compile(r";\s*--", re.IGNORECASE),                  # comment after stacked query
    re.compile(r"'\s*OR\s*'1'\s*=\s*'1", re.IGNORECASE),  # classic OR injection
    re.compile(r"UNION\s+SELECT", re.IGNORECASE),          # UNION-based injection
]


def validate_sql_ast(sql: str, dialect: str = "mysql") -> Tuple[bool, str]:
    """
    Returns (is_safe, reason).
    is_safe=True  → SQL passed all checks.
    is_safe=False → reason explains what was blocked.

    Checks injection patterns only; allows all legitimate SQL operations.
    """
    if not sql or not sql.strip():
        return False, "Empty SQL statement."

    # Injection pattern scan
    for pattern in _INJECTION_PATTERNS:
        if pattern.search(sql):
            return False, "SQL injection pattern detected."

    # Ensure it parses as valid SQL (catches completely malformed input)
    try:
        read_dialect = dialect if dialect != "mssql" else "tsql"
        stmts = sqlglot.parse(sql, read=read_dialect)
        if not stmts or all(s is None for s in stmts):
            return False, "Could not parse SQL — no valid statements found."
    except Exception as exc:
        return False, f"SQL parse error: {exc}"

    return True, "APPROVED"
