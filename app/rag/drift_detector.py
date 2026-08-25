"""
rag/drift_detector.py
----------------------
Async background task that re-extracts schema DDLs every 15 minutes
and triggers re-embedding when a table's DDL hash changes.

Free-tier optimizations:
  - Interval raised to 15 min (was 10 min) to reduce Pinecone + embedding API load.
  - Per-connection debounce: re-embedding is skipped if the same connection
    was already re-embedded within the last 60 seconds, preventing a rapidly-
    changing schema from exhausting the 5 RPM embedding quota.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import time
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)

_ddl_hashes: Dict[str, str]   = {}  # key → md5 of DDL
_last_embed_ts: Dict[str, float] = {}  # connection_key → last embed timestamp
_EMBED_DEBOUNCE_SEC = 60  # minimum seconds between re-embeds for the same connection


def _hash_ddl(ddl: str) -> str:
    return hashlib.md5(ddl.encode()).hexdigest()


async def drift_detection_loop(
    get_active_connections: Callable[[], List[Dict[str, Any]]],
    on_drift: Optional[Callable[[str, List[str]], None]] = None,
    interval_sec: int = 900,  # 15 minutes — reduced from 10 min to save free-tier quota
) -> None:
    """
    Run forever, checking for schema drift every *interval_sec* seconds.

    Parameters
    ----------
    get_active_connections
        Callable that returns a list of connection dicts currently active.
    on_drift
        Called with (connection_key, changed_tables) when drift is detected.
    interval_sec
        Poll interval, default 15 minutes (900s).
    """
    from app.database.connector import get_engine, _conn_key
    from app.database.schema_extractor import extract_schema
    from app.rag.embedder import embed_schema

    while True:
        await asyncio.sleep(interval_sec)
        connections = get_active_connections()
        for conn in connections:
            try:
                key = _conn_key(conn)
                engine = get_engine(conn)
                tables = extract_schema(engine, conn.get("dialect", "mysql"))
                changed = []
                for t in tables:
                    ddl = t.get("ddl", "")
                    h = _hash_ddl(ddl)
                    cache_key = f"{key}:{t['table_name']}"
                    if _ddl_hashes.get(cache_key) != h:
                        changed.append(t["table_name"])
                        _ddl_hashes[cache_key] = h

                if changed:
                    logger.info("Schema drift detected for %s: %s", key, changed)
                    # Debounce: skip re-embedding if we already did it recently
                    now = time.time()
                    last = _last_embed_ts.get(key, 0)
                    if now - last < _EMBED_DEBOUNCE_SEC:
                        logger.info(
                            "[drift] Skipping re-embed for '%s' — debounce active (%.0fs remaining).",
                            key, _EMBED_DEBOUNCE_SEC - (now - last),
                        )
                    else:
                        _last_embed_ts[key] = now
                        embed_schema(tables, key)
                    if on_drift:
                        on_drift(key, changed)
            except Exception as exc:
                logger.error("Drift detection error for connection: %s", exc)
