"""
app/core/cache.py
-----------------
Lightweight in-memory response cache for Gemini API calls.

Strategy:
- Key  = (prompt_hash, model)
- TTL  = 30 minutes (covers repeated NL→SQL questions in a session)
- Max  = 200 entries (LRU eviction)

Free-tier impact:
  Repeated identical questions return the cached SQL without consuming
  any Gemini RPD (requests-per-day) quota — significant for the 1500 RPD
  limit on the free tier.

Model is resolved via GEMINI_MODEL env var (default: gemini-2.5-flash)
so switching models doesn't silently serve stale cross-model cache hits.
"""

from __future__ import annotations

import hashlib
import logging
import os
import time
from collections import OrderedDict
from typing import Any, Optional

import threading

logger = logging.getLogger(__name__)

_CACHE_TTL = 1800      # 30 minutes — long enough to cover a typical work session
_CACHE_MAX = 200       # max entries before LRU eviction

# Resolved once at import; avoids cross-model cache pollution
_DEFAULT_MODEL = os.environ.get("GEMINI_MODEL", "gemini-2.5-flash")


class LRUCache:
    """Thread-safe LRU cache with TTL."""

    def __init__(self, max_size: int = _CACHE_MAX, ttl: int = _CACHE_TTL):
        self._store: OrderedDict[str, tuple[Any, float]] = OrderedDict()
        self._max = max_size
        self._ttl = ttl
        self._lock = threading.Lock()

    def _key(self, prompt: str, model: str = "") -> str:
        effective_model = model or _DEFAULT_MODEL
        raw = f"{effective_model}:{prompt}"
        return hashlib.sha256(raw.encode()).hexdigest()[:32]

    def get(self, prompt: str, model: str = "") -> Optional[str]:
        k = self._key(prompt, model)
        with self._lock:
            if k not in self._store:
                return None
            value, ts = self._store[k]
            if time.time() - ts > self._ttl:
                del self._store[k]
                return None
            # Move to end (most recently used)
            self._store.move_to_end(k)
            return value

    def set(self, prompt: str, value: str, model: str = "") -> None:
        k = self._key(prompt, model)
        with self._lock:
            if k in self._store:
                self._store.move_to_end(k)
            self._store[k] = (value, time.time())
            if len(self._store) > self._max:
                evicted_key, _ = self._store.popitem(last=False)
                logger.debug("Cache evicted: %s", evicted_key)

    def size(self) -> int:
        with self._lock:
            return len(self._store)

    def clear(self) -> None:
        with self._lock:
            self._store.clear()


# Singleton
_cache = LRUCache()


def get_cache() -> LRUCache:
    return _cache
