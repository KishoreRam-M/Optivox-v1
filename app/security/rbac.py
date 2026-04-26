"""
security/rbac.py
----------------
Role-Based Access Control definitions.

Roles
-----
ADMIN    — full access: any database, any operation
ANALYST  — SELECT only
DEVELOPER — SELECT + DML (INSERT, UPDATE, DELETE) — no DDL
"""

from __future__ import annotations

from enum import Enum
from typing import Set

import sqlglot
import sqlglot.expressions as exp


class Role(str, Enum):
    ADMIN = "admin"
    ANALYST = "analyst"
    DEVELOPER = "developer"


# Operations each role may perform
_ROLE_PERMISSIONS: dict[Role, Set[str]] = {
    Role.ADMIN:     {"SELECT", "INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER", "TRUNCATE"},
    Role.DEVELOPER: {"SELECT", "INSERT", "UPDATE", "DELETE"},
    Role.ANALYST:   {"SELECT"},
}


def _detect_operations(sql: str) -> Set[str]:
    """Return the set of SQL operations found in *sql*."""
    ops: Set[str] = set()
    try:
        for stmt in sqlglot.parse(sql):
            if stmt is None:
                continue
            if isinstance(stmt, exp.Select):
                ops.add("SELECT")
            elif isinstance(stmt, exp.Insert):
                ops.add("INSERT")
            elif isinstance(stmt, exp.Update):
                ops.add("UPDATE")
            elif isinstance(stmt, exp.Delete):
                ops.add("DELETE")
            elif isinstance(stmt, exp.Drop):
                ops.add("DROP")
            elif isinstance(stmt, exp.Create):
                ops.add("CREATE")
            elif isinstance(stmt, exp.Alter):
                ops.add("ALTER")
            else:
                # Catch TRUNCATE and others by class name
                ops.add(type(stmt).__name__.upper())
    except Exception:
        pass
    return ops


def check_permission(role: Role, sql: str) -> tuple[bool, str]:
    """
    Returns (allowed, reason).
    allowed=True  → proceed
    allowed=False → reject with reason
    """
    allowed_ops = _ROLE_PERMISSIONS.get(role, set())
    required_ops = _detect_operations(sql)
    blocked = required_ops - allowed_ops
    if blocked:
        return False, (
            f"Role '{role.value}' does not have permission to execute: "
            f"{', '.join(sorted(blocked))}."
        )
    return True, ""
