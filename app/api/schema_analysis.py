"""
app/api/schema_analysis.py
---------------------------
Phase 6 — Schema Analysis endpoint helpers.

Returns:
  - table_count
  - estimated_row_counts (via SELECT COUNT(*) on each table — async, best-effort)
  - foreign_key_relationship_map {table → [{from_col, to_table, to_col}]}
  - missing_index_suggestions  [{table, column, reason}]
  - no_index_tables  [table_name]
  - isolated_tables  [table_name] (no FKs at all)
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List

logger = logging.getLogger(__name__)


def analyze_schema(
    tables: List[Dict[str, Any]],
    engine=None,
) -> Dict[str, Any]:
    """
    Produce a rich analysis of the extracted schema.

    Parameters
    ----------
    tables  : list of table dicts from schema_extractor.extract_schema()
    engine  : SQLAlchemy engine (optional — used for row count estimation)
    """
    table_count = len(tables)

    # ── FK relationship map ─────────────────────────────────────────────
    fk_map: Dict[str, List[Dict[str, str]]] = {}
    all_fk_tables: set[str] = set()

    for t in tables:
        name = t.get("table_name", "")
        fks = t.get("foreign_keys", [])
        if fks:
            fk_map[name] = []
            all_fk_tables.add(name)
            for fk in fks:
                fk_map[name].append({
                    "from_col": fk.get("column", ""),
                    "to_table": fk.get("ref_table", ""),
                    "to_col":   fk.get("ref_column", ""),
                })

    # Also mark tables that are referenced BY foreign keys
    for t in tables:
        for fk in t.get("foreign_keys", []):
            rt = fk.get("ref_table", "")
            if rt:
                all_fk_tables.add(rt)

    # ── Isolated tables (no FKs in OR out) ─────────────────────────────
    isolated_tables = [
        t["table_name"] for t in tables
        if t.get("table_name") not in all_fk_tables
    ]

    # ── Missing index suggestions ───────────────────────────────────────
    missing_index_suggestions: List[Dict[str, str]] = []
    no_index_tables: List[str] = []

    for t in tables:
        name = t.get("table_name", "")
        pks = set(t.get("primary_keys", []))
        indexed_cols = set(pks)  # PKs are always indexed

        # FK columns should be indexed
        for fk in t.get("foreign_keys", []):
            col = fk.get("column", "")
            if col and col not in indexed_cols:
                missing_index_suggestions.append({
                    "table":  name,
                    "column": col,
                    "reason": f"Foreign key column '{col}' has no index — JOINs on this column will cause full table scans.",
                })
                indexed_cols.add(col)

        # Tables with no PK at all
        if not pks:
            missing_index_suggestions.append({
                "table":  name,
                "column": "(none)",
                "reason": f"Table '{name}' has no primary key — queries cannot use index lookup.",
            })
            no_index_tables.append(name)

    # ── Row count estimates ─────────────────────────────────────────────
    estimated_row_counts: Dict[str, int] = {}
    if engine is not None:
        try:
            from sqlalchemy import text as sa_text
            with engine.connect() as conn:
                for t in tables:
                    tname = t.get("table_name", "")
                    try:
                        row = conn.execute(sa_text(f"SELECT COUNT(*) FROM {tname}")).fetchone()
                        estimated_row_counts[tname] = int(row[0]) if row else 0
                    except Exception:
                        estimated_row_counts[tname] = -1  # unavailable
        except Exception as exc:
            logger.warning("Row count estimation failed: %s", exc)

    return {
        "table_count":              table_count,
        "estimated_row_counts":     estimated_row_counts,
        "fk_relationship_map":      fk_map,
        "isolated_tables":          isolated_tables,
        "missing_index_suggestions": missing_index_suggestions,
        "no_index_tables":          no_index_tables,
    }
