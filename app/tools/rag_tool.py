"""
tools/rag_tool.py
-----------------
RAG retrieval functions for schema context and query history.
These are plain Python functions (no framework-specific base classes).
Used directly by LangGraph nodes.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def fetch_schema_context(question: str, top_k: int = 3) -> str:
    """
    Retrieve the most relevant database table DDLs for a natural language question.
    Always call this before generating SQL to ensure real column names are used.
    Returns a multi-line string of DDL snippets, or empty string if unavailable.
    """
    try:
        from app.rag.embedder import fetch_schema_context as _fetch
        result = _fetch(question, top_k=top_k)
        return result or ""
    except Exception as exc:
        logger.warning("fetch_schema_context failed: %s", exc)
        return ""


def fetch_query_history(question: str, top_k: int = 2) -> str:
    """
    Retrieve similar past question/SQL pairs as few-shot examples.
    Use these to guide SQL generation style and correctness.
    Returns a multi-line string of examples, or empty string if unavailable.
    """
    try:
        from app.rag.embedder import fetch_query_history as _fetch
        result = _fetch(question, top_k=top_k)
        return result or ""
    except Exception as exc:
        logger.warning("fetch_query_history failed: %s", exc)
        return ""
