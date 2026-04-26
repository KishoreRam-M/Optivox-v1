"""
rag/embedder.py
---------------
LanceDB-backed vector store for schema DDLs, query history, and dialect docs.
Uses all-MiniLM-L6-v2 from sentence-transformers for embeddings.
"""

from __future__ import annotations

import logging
import hashlib
from typing import Any, Dict, List, Optional

import lancedb
import numpy as np

logger = logging.getLogger(__name__)

DB_PATH = "./lancedb_data"

_TABLE_SCHEMA_DOCS = "schema_docs"
_TABLE_QUERY_HISTORY = "query_history"
_TABLE_DIALECT_DOCS = "dialect_docs"

_db: Optional[lancedb.DBConnection] = None
_embedder = None  # lazy-loaded


def _get_db() -> lancedb.DBConnection:
    global _db
    if _db is None:
        _db = lancedb.connect(DB_PATH)
    return _db


def _get_embedder():
    global _embedder
    if _embedder is None:
        from sentence_transformers import SentenceTransformer
        _embedder = SentenceTransformer("all-MiniLM-L6-v2")
        logger.info("Sentence transformer loaded.")
    return _embedder


def _embed(text: str) -> List[float]:
    model = _get_embedder()
    return model.encode(text, normalize_embeddings=True).tolist()


def _ensure_table(db: lancedb.DBConnection, name: str, sample: Dict[str, Any]):
    """Create table if it doesn't exist by opening or creating it."""
    try:
        return db.open_table(name)
    except Exception:
        import pyarrow as pa
        schema = pa.schema([
            pa.field("id", pa.string()),
            pa.field("text", pa.string()),
            pa.field("vector", pa.list_(pa.float32(), 384)),
            pa.field("metadata", pa.string()),
        ])
        return db.create_table(name, schema=schema)


# ── Public API ────────────────────────────────────────────────────────────

def embed_schema(tables: List[Dict[str, Any]], connection_key: str) -> None:
    """
    Embed each table's DDL string and upsert into schema_docs.
    Chunks by table so retrieval returns individual DDLs.
    """
    db = _get_db()
    tbl = _ensure_table(db, _TABLE_SCHEMA_DOCS, {})
    rows = []
    for t in tables:
        doc_id = hashlib.md5(f"{connection_key}:{t['table_name']}".encode()).hexdigest()
        ddl = t.get("ddl", "")
        rows.append({
            "id": doc_id,
            "text": ddl,
            "vector": _embed(ddl),
            "metadata": f'{{"connection": "{connection_key}", "table": "{t["table_name"]}"}}'
        })
    if rows:
        tbl.merge_insert("id").when_matched_update_all().when_not_matched_insert_all().execute(rows)
        logger.info("Upserted %d schema docs for %s.", len(rows), connection_key)


def embed_query_history(question: str, sql: str, session_id: str = "") -> None:
    """Embed NL question + SQL into query_history."""
    db = _get_db()
    tbl = _ensure_table(db, _TABLE_QUERY_HISTORY, {})
    combined = f"Question: {question}\nSQL: {sql}"
    doc_id = hashlib.md5(combined.encode()).hexdigest()
    row = {
        "id": doc_id,
        "text": combined,
        "vector": _embed(combined),
        "metadata": f'{{"session": "{session_id}"}}',
    }
    tbl.merge_insert("id").when_matched_update_all().when_not_matched_insert_all().execute([row])


def fetch_schema_context(question: str, top_k: int = 3) -> str:
    """Return top_k DDL snippets most relevant to *question*."""
    db = _get_db()
    try:
        tbl = db.open_table(_TABLE_SCHEMA_DOCS)
    except Exception:
        return ""
    results = tbl.search(_embed(question)).limit(top_k).to_list()
    return "\n\n".join(r["text"] for r in results)


def fetch_query_history(question: str, top_k: int = 2) -> str:
    """Return top_k similar past question/SQL pairs."""
    db = _get_db()
    try:
        tbl = db.open_table(_TABLE_QUERY_HISTORY)
    except Exception:
        return ""
    results = tbl.search(_embed(question)).limit(top_k).to_list()
    return "\n\n".join(r["text"] for r in results)
