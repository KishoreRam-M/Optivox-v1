"""
database/schema_extractor.py
-----------------------------
Extracts DDL-style schema descriptions from live databases.
Supports MySQL, PostgreSQL, and Oracle via INFORMATION_SCHEMA / catalog views.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List

from sqlalchemy import text, Engine

logger = logging.getLogger(__name__)


def extract_schema(engine: Engine, dialect: str) -> List[Dict[str, Any]]:
    """
    Return a list of table descriptors, each containing:
      table_name, columns (list), primary_keys (list), foreign_keys (list), ddl (str).
    """
    dialect = dialect.lower()
    if dialect == "mysql":
        return _extract_mysql(engine)
    elif dialect == "postgres":
        return _extract_postgres(engine)
    elif dialect == "oracle":
        return _extract_oracle(engine)
    else:
        raise ValueError(f"Unsupported dialect for schema extraction: {dialect}")


# ── MySQL ─────────────────────────────────────────────────────────────────

def _extract_mysql(engine: Engine) -> List[Dict[str, Any]]:
    with engine.connect() as conn:
        db_name = engine.url.database
        tables_q = text("""
            SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = :db AND TABLE_TYPE = 'BASE TABLE'
            ORDER BY TABLE_NAME
        """)
        tables = [r[0] for r in conn.execute(tables_q, {"db": db_name})]

        results = []
        for table in tables:
            cols_q = text("""
                SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_KEY, COLUMN_COMMENT
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = :db AND TABLE_NAME = :tbl
                ORDER BY ORDINAL_POSITION
            """)
            cols = conn.execute(cols_q, {"db": db_name, "tbl": table}).fetchall()

            pk_q = text("""
                SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                WHERE TABLE_SCHEMA = :db AND TABLE_NAME = :tbl AND CONSTRAINT_NAME = 'PRIMARY'
            """)
            pks = [r[0] for r in conn.execute(pk_q, {"db": db_name, "tbl": table})]

            fk_q = text("""
                SELECT kcu.COLUMN_NAME, kcu.REFERENCED_TABLE_NAME, kcu.REFERENCED_COLUMN_NAME
                FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
                JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                  ON kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME AND kcu.TABLE_SCHEMA = tc.TABLE_SCHEMA
                WHERE kcu.TABLE_SCHEMA = :db AND kcu.TABLE_NAME = :tbl
                  AND tc.CONSTRAINT_TYPE = 'FOREIGN KEY'
            """)
            fks = [
                {"column": r[0], "references": f"{r[1]}({r[2]})"}
                for r in conn.execute(fk_q, {"db": db_name, "tbl": table})
            ]

            ddl = _build_ddl(table, cols, pks, fks)
            results.append({
                "table_name": table,
                "columns": [{"name": c[0], "type": c[1], "nullable": c[2], "comment": c[4]} for c in cols],
                "primary_keys": pks,
                "foreign_keys": fks,
                "ddl": ddl,
            })
    return results


# ── PostgreSQL ────────────────────────────────────────────────────────────

def _extract_postgres(engine: Engine) -> List[Dict[str, Any]]:
    with engine.connect() as conn:
        tables_q = text("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """)
        tables = [r[0] for r in conn.execute(tables_q)]

        results = []
        for table in tables:
            cols_q = text("""
                SELECT column_name, data_type, is_nullable,
                       col_description((table_schema||'.'||table_name)::regclass::oid, ordinal_position)
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = :tbl
                ORDER BY ordinal_position
            """)
            cols = conn.execute(cols_q, {"tbl": table}).fetchall()

            pk_q = text("""
                SELECT kcu.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                  ON tc.constraint_name = kcu.constraint_name
                 AND tc.table_schema = kcu.table_schema
                WHERE tc.table_schema = 'public' AND tc.table_name = :tbl
                  AND tc.constraint_type = 'PRIMARY KEY'
            """)
            pks = [r[0] for r in conn.execute(pk_q, {"tbl": table})]

            fk_q = text("""
                SELECT kcu.column_name, ccu.table_name, ccu.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                  ON tc.constraint_name = kcu.constraint_name
                 AND tc.table_schema = kcu.table_schema
                JOIN information_schema.constraint_column_usage ccu
                  ON ccu.constraint_name = tc.constraint_name
                WHERE tc.table_schema = 'public' AND tc.table_name = :tbl
                  AND tc.constraint_type = 'FOREIGN KEY'
            """)
            fks = [
                {"column": r[0], "references": f"{r[1]}({r[2]})"}
                for r in conn.execute(fk_q, {"tbl": table})
            ]

            col_tuples = [(c[0], c[1], c[2], "", c[3] or "") for c in cols]
            ddl = _build_ddl(table, col_tuples, pks, fks)
            results.append({
                "table_name": table,
                "columns": [{"name": c[0], "type": c[1], "nullable": c[2], "comment": c[3]} for c in cols],
                "primary_keys": pks,
                "foreign_keys": fks,
                "ddl": ddl,
            })
    return results


# ── Oracle ────────────────────────────────────────────────────────────────

def _extract_oracle(engine: Engine) -> List[Dict[str, Any]]:
    with engine.connect() as conn:
        tables_q = text("SELECT TABLE_NAME FROM USER_TABLES ORDER BY TABLE_NAME")
        tables = [r[0] for r in conn.execute(tables_q)]

        results = []
        for table in tables:
            cols_q = text("""
                SELECT COLUMN_NAME, DATA_TYPE, NULLABLE
                FROM ALL_TAB_COLUMNS
                WHERE TABLE_NAME = :tbl
                ORDER BY COLUMN_ID
            """)
            cols = conn.execute(cols_q, {"tbl": table}).fetchall()

            pk_q = text("""
                SELECT acc.COLUMN_NAME
                FROM ALL_CONSTRAINTS ac
                JOIN ALL_CONS_COLUMNS acc ON ac.CONSTRAINT_NAME = acc.CONSTRAINT_NAME
                WHERE ac.TABLE_NAME = :tbl AND ac.CONSTRAINT_TYPE = 'P'
            """)
            pks = [r[0] for r in conn.execute(pk_q, {"tbl": table})]

            fk_q = text("""
                SELECT acc.COLUMN_NAME, ac2.TABLE_NAME, acc2.COLUMN_NAME
                FROM ALL_CONSTRAINTS ac
                JOIN ALL_CONS_COLUMNS acc ON ac.CONSTRAINT_NAME = acc.CONSTRAINT_NAME
                JOIN ALL_CONSTRAINTS ac2 ON ac.R_CONSTRAINT_NAME = ac2.CONSTRAINT_NAME
                JOIN ALL_CONS_COLUMNS acc2 ON ac2.CONSTRAINT_NAME = acc2.CONSTRAINT_NAME
                WHERE ac.TABLE_NAME = :tbl AND ac.CONSTRAINT_TYPE = 'R'
            """)
            fks = [
                {"column": r[0], "references": f"{r[1]}({r[2]})"}
                for r in conn.execute(fk_q, {"tbl": table})
            ]

            col_tuples = [(c[0], c[1], "Y" if c[2] == "Y" else "NO", "", "") for c in cols]
            ddl = _build_ddl(table, col_tuples, pks, fks)
            results.append({
                "table_name": table,
                "columns": [{"name": c[0], "type": c[1], "nullable": c[2]} for c in cols],
                "primary_keys": pks,
                "foreign_keys": fks,
                "ddl": ddl,
            })
    return results


# ── DDL builder ───────────────────────────────────────────────────────────

def _build_ddl(
    table: str,
    cols: list,
    pks: List[str],
    fks: List[Dict[str, str]],
) -> str:
    lines = [f"CREATE TABLE {table} ("]
    for col in cols:
        name, dtype, nullable, _, comment = col[0], col[1], col[2], col[3] if len(col) > 3 else "", col[4] if len(col) > 4 else ""
        null_str = "NULL" if nullable in ("YES", "Y") else "NOT NULL"
        comment_str = f"  -- {comment}" if comment else ""
        lines.append(f"  {name} {dtype} {null_str},{comment_str}")
    if pks:
        lines.append(f"  PRIMARY KEY ({', '.join(pks)}),")
    for fk in fks:
        lines.append(f"  FOREIGN KEY ({fk['column']}) REFERENCES {fk['references']},")
    # strip trailing comma on last line
    if lines[-1].endswith(","):
        lines[-1] = lines[-1][:-1]
    lines.append(");")
    return "\n".join(lines)
