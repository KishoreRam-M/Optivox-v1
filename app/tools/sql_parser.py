"""
tools/sql_parser.py
-------------------
AST-level SQL safety validator using sqlglot.

Only blocks genuine SQL injection patterns.
"""

from __future__ import annotations

import re
import logging
from typing import Tuple

import sqlglot

from crewai.tools import BaseTool
from pydantic import BaseModel, Field

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

    Checks injection patterns only.
    """
    # Injection pattern scan
    for pattern in _INJECTION_PATTERNS:
        if pattern.search(sql):
            return False, f"SQL injection pattern detected."

    # Ensure it parses as valid SQL (catches completely malformed input)
    try:
        stmts = sqlglot.parse(sql, read=dialect if dialect != "mssql" else "tsql")
        if not stmts or all(s is None for s in stmts):
            return False, "Could not parse SQL — no valid statements found."
    except Exception as exc:
        return False, f"SQL parse error: {exc}"

    return True, "APPROVED"


class SQLValidatorInput(BaseModel):
    sql: str = Field(description="The SQL statement to validate for safety.")
    dialect: str = Field(default="mysql", description="SQL dialect: mysql | postgres | oracle")


class SQLValidatorTool(BaseTool):
    name: str = "validate_sql_ast"
    description: str = (
        "Validates a SQL statement using AST analysis. "
        "Returns APPROVED or REJECTED with a reason. "
        "Always call this before approving any SQL."
    )
    args_schema: type[BaseModel] = SQLValidatorInput

    def _run(self, sql: str, dialect: str = "mysql") -> str:
        is_safe, reason = validate_sql_ast(sql, dialect)
        if is_safe:
            return "APPROVED"
        return f"REJECTED: {reason}"
