"""
tools/sql_parser.py
-------------------
AST-level SQL safety validator using sqlglot.

Blocks:
  - DROP statements
  - TRUNCATE statements
  - DELETE without a WHERE clause
  - SQL injection patterns (stacked queries, comment injection)
"""

from __future__ import annotations

import re
import logging
from typing import Tuple

import sqlglot
import sqlglot.expressions as exp

from crewai.tools import BaseTool
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

_INJECTION_PATTERNS = [
    re.compile(r";\s*--", re.IGNORECASE),           # comment after stacked query
    re.compile(r";\s*(DROP|DELETE|TRUNCATE|INSERT|UPDATE)", re.IGNORECASE),  # stacked writes
    re.compile(r"'\s*OR\s*'1'\s*=\s*'1", re.IGNORECASE),  # classic injection
    re.compile(r"UNION\s+SELECT", re.IGNORECASE),   # UNION injection
]


def validate_sql_ast(sql: str, dialect: str = "mysql") -> Tuple[bool, str]:
    """
    Returns (is_safe, reason).
    is_safe=True  → SQL passed all checks.
    is_safe=False → reason explains what was blocked.
    """
    # Injection pattern scan
    for pattern in _INJECTION_PATTERNS:
        if pattern.search(sql):
            return False, f"SQL injection pattern detected: {pattern.pattern}"

    # AST analysis
    try:
        statements = sqlglot.parse(sql, read=dialect)
    except Exception as exc:
        return False, f"SQL parse error: {exc}"

    for stmt in statements:
        if stmt is None:
            continue

        # Block DROP
        if isinstance(stmt, exp.Drop):
            return False, "DROP statements are not permitted."

        # Block TRUNCATE
        if stmt.__class__.__name__ == "Command" and "TRUNCATE" in str(stmt).upper():
            return False, "TRUNCATE statements are not permitted."
        if isinstance(stmt, exp.Command):
            if stmt.name and "TRUNCATE" in stmt.name.upper():
                return False, "TRUNCATE statements are not permitted."

        # Block DELETE without WHERE
        if isinstance(stmt, exp.Delete):
            if stmt.find(exp.Where) is None:
                return False, "DELETE without a WHERE clause is not permitted."

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
