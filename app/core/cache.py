"""
app/core/cache.py
-----------------
Lightweight in-memory response cache for Gemini API calls.

Strategy:
- Key = (prompt_hash, model)
- TTL = 5 minutes (fresh enough for SQL generation)
- Max 200 entries (LRU eviction)

This dramatically reduces Gemini free tier RPD consumption
when users ask similar questions repeatedly.
"""

from __future__ import annotations

import hashlib
import logging
import time
from collections import OrderedDict
from typing import Any, Optional

logger = logging.getLogger(__name__)

_CACHE_TTL = 300       # 5 minutes
_CACHE_MAX = 200       # max entries before LRU eviction


class LRUCache:
    """Thread-safe LRU cache with TTL."""

    def __init__(self, max_size: int = _CACHE_MAX, ttl: int = _CACHE_TTL):
        self._store: OrderedDict[str, tuple[Any, float]] = OrderedDict()
        self._max = max_size
        self._ttl = ttl

    def _key(self, prompt: str, model: str = "gemini-2.5-flash") -> str:
        raw = f"{model}:{prompt}"
        return hashlib.sha256(raw.encode()).hexdigest()[:32]

    def get(self, prompt: str, model: str = "gemini-2.5-flash") -> Optional[str]:
        k = self._key(prompt, model)
        if k not in self._store:
            return None
        value, ts = self._store[k]
        if time.time() - ts > self._ttl:
            del self._store[k]
            return None
        # Move to end (most recently used)
        self._store.move_to_end(k)
        return value

    def set(self, prompt: str, value: str, model: str = "gemini-2.5-flash") -> None:
        k = self._key(prompt, model)
        if k in self._store:
            self._store.move_to_end(k)
        self._store[k] = (value, time.time())
        if len(self._store) > self._max:
            evicted_key, _ = self._store.popitem(last=False)
            logger.debug("Cache evicted: %s", evicted_key)

    def size(self) -> int:
        return len(self._store)

    def clear(self) -> None:
        self._store.clear()


# Singleton
_cache = LRUCache()


def get_cache() -> LRUCache:
    return _cache
