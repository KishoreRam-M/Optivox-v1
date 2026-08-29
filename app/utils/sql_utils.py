"""
app/utils/sql_utils.py
-----------------------
Shared SQL utility functions used by both main.py and agents/graph.py.

Centralizing _extract_sql() here eliminates the duplication that existed
between the two modules (which had subtly different fallback behaviour).
"""

from __future__ import annotations

import re

# Pre-compiled once at module import — no per-call recompilation.
_SQL_FENCE_RE = re.compile(r"```(?:sql)?\s*\n?(.*?)```", re.DOTALL | re.IGNORECASE)

# Matches the first SQL keyword when there is no fenced block
_SQL_KEYWORD_RE = re.compile(
    r"\b(SELECT|INSERT|UPDATE|DELETE|WITH|CREATE|DROP|ALTER|TRUNCATE)\b.*",
    re.DOTALL | re.IGNORECASE,
)


def extract_sql(raw: str) -> str:
    """
    Robustly extract a SQL statement from an LLM response.

    Priority order:
      1. Content inside a ```sql ... ``` or ``` ... ``` fence.
      2. First SQL keyword onwards (strips leading prose/explanation).
      3. Full stripped response (last resort).

    This is the canonical implementation — both main.py and agents/graph.py
    import from here to guarantee identical behaviour.
    """
    raw = raw.strip()

    # 1. Fenced code block
    match = _SQL_FENCE_RE.search(raw)
    if match:
        return match.group(1).strip()

    # 2. Strip stray fence markers line-by-line (no fence boundaries found)
    lines = [line for line in raw.splitlines() if not line.strip().startswith("```")]
    cleaned = "\n".join(lines).strip()

    # 3. Try to start from the first SQL keyword
    kw_match = _SQL_KEYWORD_RE.search(cleaned)
    if kw_match:
        return kw_match.group(0).strip()

    return cleaned
