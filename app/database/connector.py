"""
database/connector.py
---------------------
Multi-dialect connection manager with SQLAlchemy connection pooling.
Supports MySQL, PostgreSQL, and Oracle.
"""

from __future__ import annotations

import time
import logging
from typing import Any, Dict, Optional

from sqlalchemy import create_engine, text, URL, Engine
from sqlalchemy.pool import QueuePool

logger = logging.getLogger(__name__)

# ── Driver map ────────────────────────────────────────────────────────────

_DIALECT_DRIVERS: Dict[str, str] = {
    "mysql":    "mysql+pymysql",
    "postgres": "postgresql+psycopg2",
    "oracle":   "oracle+oracledb",
}

_DEFAULT_PORTS: Dict[str, int] = {
    "mysql":    3306,
    "postgres": 5432,
    "oracle":   1521,
}

_ENGINE_CACHE_TTL = 1800  # seconds
_engine_cache: Dict[str, Dict[str, Any]] = {}


# ── Internal helpers ──────────────────────────────────────────────────────

def _conn_key(conn: Dict[str, Any]) -> str:
    dialect = conn.get("dialect", "mysql")
    port = conn.get("port", _DEFAULT_PORTS.get(dialect, 3306))
    return f"{conn['dialect']}://{conn['user']}@{conn['host']}:{port}/{conn['database']}"


def _build_url(conn: Dict[str, Any]) -> URL:
    dialect = conn.get("dialect", "mysql").lower()
    driver = _DIALECT_DRIVERS.get(dialect, "mysql+pymysql")
    port = conn.get("port", _DEFAULT_PORTS.get(dialect, 3306))
    return URL.create(
        driver,
        username=conn.get("user"),
        password=conn.get("password"),
        host=conn.get("host"),
        port=port,
        database=conn.get("database"),
    )


# ── Public API ────────────────────────────────────────────────────────────

def get_engine(conn: Dict[str, Any]) -> Engine:
    """
    Return a cached SQLAlchemy Engine for *conn*.
    Creates a new engine if the cached one is stale or unhealthy.
    """
    key = _conn_key(conn)
    now = time.time()

    entry = _engine_cache.get(key)
    if entry:
        if now - entry["ts"] < _ENGINE_CACHE_TTL:
            try:
                with entry["engine"].connect() as tc:
                    tc.execute(text("SELECT 1"))
                return entry["engine"]
            except Exception:
                logger.warning("Cached engine for %s is unhealthy — recreating.", key)
        entry["engine"].dispose()
        del _engine_cache[key]

    url = _build_url(conn)
    engine = create_engine(
        url,
        poolclass=QueuePool,
        pool_pre_ping=True,
        pool_size=3,
        max_overflow=5,
        pool_recycle=1800,
        echo=False,
    )
    # Validate immediately so callers get a clear error if creds are wrong
    with engine.connect() as tc:
        tc.execute(text("SELECT 1"))

    _engine_cache[key] = {"engine": engine, "ts": now}
    logger.info("Created new engine for %s.", key)
    return engine


def test_connection(conn: Dict[str, Any]) -> Dict[str, Any]:
    """
    Try to connect and return metadata about the connection.
    Raises on failure so the caller can wrap it in an HTTPException.
    """
    engine = get_engine(conn)
    with engine.connect() as tc:
        tc.execute(text("SELECT 1"))
    dialect = conn.get("dialect", "mysql")
    return {
        "dialect": dialect,
        "host": conn.get("host"),
        "database": conn.get("database"),
        "driver": _DIALECT_DRIVERS.get(dialect, "unknown"),
        "status": "connected",
    }
