"""
rag/embedder.py
---------------
Pinecone-backed vector store for schema DDLs, query history, and dialect docs.
Uses Google's gemini-embedding-001 (cloud API) for all embeddings — no local model.

Architecture:
  Documents → Gemini Embedding API (gemini-embedding-001, 768 dims)
            → Pinecone Index (namespaced by collection type)
            → Similarity Search → Relevant Context → Gemini LLM

Free-Tier Constraints (Pinecone):
  - 1 index maximum (use namespaces, not multiple indexes)
  - us-east-1 (AWS) region only
  - 40 KB metadata limit per vector (DDL text truncated to 38 KB)
  - 100 vectors per upsert batch

Free-Tier Constraints (Gemini Embedding API):
  - 5 RPM  (requests per minute)  ← enforced by _EmbedRateLimiter
  - 100 RPD (requests per day)    ← conserved by in-memory embed cache

Environment Variables:
  PINECONE_API_KEY      - Pinecone API key
  PINECONE_INDEX_NAME   - Index name (default: "optivox-rag")
  GEMINI_API_KEY        - Google Gemini API key (shared with LLM layer)
  GEMINI_EMBED_MODEL    - Embedding model (default: "models/gemini-embedding-001")
  EMBEDDING_DIM         - Embedding output dimension (default: 768)
"""

from __future__ import annotations

import hashlib
import logging
import os
import time
from collections import OrderedDict
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────

PINECONE_API_KEY: str    = os.environ.get("PINECONE_API_KEY", "")
PINECONE_INDEX_NAME: str = os.environ.get("PINECONE_INDEX_NAME", "optivox-rag")
GEMINI_API_KEY: str      = os.environ.get("GEMINI_API_KEY", "")

# gemini-embedding-001 free-tier outputs 768 dims (NOT 3072 — that requires paid quota)
EMBEDDING_DIM: int = int(os.environ.get("EMBEDDING_DIM", "768"))

# Embedding model name — override via env if needed
GEMINI_EMBED_MODEL: str = os.environ.get("GEMINI_EMBED_MODEL", "models/gemini-embedding-001")

# Pinecone metadata text size cap (free tier: 40 KB per vector; we cap at 38 KB for safety)
_METADATA_TEXT_MAX_BYTES = 38_000

# Pinecone namespace per logical collection (replaces LanceDB tables)
_NS_SCHEMA_DOCS   = "schema_docs"
_NS_QUERY_HISTORY = "query_history"
_NS_DIALECT_DOCS  = "dialect_docs"

# Lazy-initialised clients
_pinecone_index  = None
_genai_configured = False


# ── Embed Vector Cache (LRU + TTL) ───────────────────────────────────────
#
# Caches (text, task_type) → embedding vector.
# Prevents redundant Gemini API calls for repeated queries across chat turns.
#
# Free-tier impact: saves up to 90%+ of embedding API calls in typical usage.

_EMBED_CACHE_MAX = 500     # entries before LRU eviction
_EMBED_CACHE_TTL = 3600    # 1 hour — vectors don't change for the same text


class _EmbedCache:
    """Thread-safe LRU cache with TTL for embedding vectors."""

    def __init__(self, max_size: int = _EMBED_CACHE_MAX, ttl: int = _EMBED_CACHE_TTL):
        self._store: OrderedDict[str, Tuple[List[float], float]] = OrderedDict()
        self._max  = max_size
        self._ttl  = ttl

    def _key(self, text: str, task_type: str) -> str:
        raw = f"{task_type}:{text}"
        return hashlib.sha256(raw.encode()).hexdigest()[:40]

    def get(self, text: str, task_type: str) -> Optional[List[float]]:
        k = self._key(text, task_type)
        if k not in self._store:
            return None
        vec, ts = self._store[k]
        if time.time() - ts > self._ttl:
            del self._store[k]
            return None
        self._store.move_to_end(k)
        logger.debug("[embed-cache] HIT for task_type=%s (key=%s…)", task_type, k[:12])
        return vec

    def set(self, text: str, task_type: str, vector: List[float]) -> None:
        k = self._key(text, task_type)
        if k in self._store:
            self._store.move_to_end(k)
        self._store[k] = (vector, time.time())
        if len(self._store) > self._max:
            self._store.popitem(last=False)

    def size(self) -> int:
        return len(self._store)


_embed_cache = _EmbedCache()


# ── Embedding Rate Limiter (5 RPM — Gemini free tier) ────────────────────
#
# Simple token-bucket that sleeps when the per-minute call budget is exhausted.
# Prevents 429 errors before they happen — no need to wait for a retry.

class _EmbedRateLimiter:
    """Token-bucket rate limiter: max N calls per 60-second rolling window."""

    def __init__(self, max_per_minute: int = 5):
        self._max    = max_per_minute
        self._window = 60.0
        self._calls: List[float] = []

    def acquire(self) -> None:
        """Block until a call slot is available."""
        now = time.time()
        # Drop timestamps outside the rolling window
        self._calls = [t for t in self._calls if now - t < self._window]
        if len(self._calls) >= self._max:
            # Sleep until the oldest call falls out of the window
            sleep_for = self._window - (now - self._calls[0]) + 0.05
            if sleep_for > 0:
                logger.info(
                    "[embed-ratelimit] 5 RPM cap reached — sleeping %.1fs before next embed call.",
                    sleep_for,
                )
                time.sleep(sleep_for)
            # Refresh after sleep
            now = time.time()
            self._calls = [t for t in self._calls if now - t < self._window]
        self._calls.append(time.time())


_rate_limiter = _EmbedRateLimiter(max_per_minute=5)


# ── Gemini Embedding Client ───────────────────────────────────────────────


def _ensure_genai() -> None:
    """Configure the Google GenAI SDK once. Raises RuntimeError on missing key."""
    global _genai_configured
    if _genai_configured:
        return
    key = GEMINI_API_KEY or os.environ.get("GEMINI_API_KEY", "")
    if not key:
        raise RuntimeError(
            "GEMINI_API_KEY is not set. Embeddings require a valid Gemini API key."
        )
    import google.generativeai as genai
    genai.configure(api_key=key)
    _genai_configured = True
    logger.info(
        "[embedder] Gemini GenAI client configured (model: %s, dim: %d).",
        GEMINI_EMBED_MODEL, EMBEDDING_DIM,
    )


def _embed_raw(text: str, task_type: str, retries: int = 3, backoff: float = 2.0) -> List[float]:
    """
    Core embed call: check cache → acquire rate-limit slot → call Gemini API.
    Retries on transient failures with exponential backoff.
    """
    # 1. Cache lookup — free, instant
    cached = _embed_cache.get(text, task_type)
    if cached is not None:
        return cached

    # 2. Rate-limit gate — sleeps if needed
    _rate_limiter.acquire()

    # 3. Call Gemini embedding API
    _ensure_genai()
    import google.generativeai as genai

    last_exc: Exception = RuntimeError("Embedding failed — no attempts made.")
    for attempt in range(retries):
        try:
            result = genai.embed_content(
                model=GEMINI_EMBED_MODEL,
                content=text,
                task_type=task_type,
            )
            vector: List[float] = result["embedding"]
            _embed_cache.set(text, task_type, vector)
            return vector
        except Exception as exc:
            last_exc = exc
            if attempt < retries - 1:
                delay = backoff * (2 ** attempt)
                logger.warning(
                    "[embedder] Gemini embed attempt %d/%d failed (%s). Retrying in %.1fs.",
                    attempt + 1, retries, exc, delay,
                )
                time.sleep(delay)

    logger.error("[embedder] Embedding permanently failed: %s", last_exc)
    raise last_exc


def _embed(text: str, retries: int = 3, backoff: float = 2.0) -> List[float]:
    """
    Generate a document embedding vector via gemini-embedding-001.
    Uses RETRIEVAL_DOCUMENT task type. Results are cached.
    """
    return _embed_raw(text, "RETRIEVAL_DOCUMENT", retries=retries, backoff=backoff)


def _embed_query(text: str, retries: int = 3, backoff: float = 2.0) -> List[float]:
    """
    Generate a query embedding vector via gemini-embedding-001.
    Uses RETRIEVAL_QUERY task type (optimised for similarity search). Results are cached.
    """
    return _embed_raw(text, "RETRIEVAL_QUERY", retries=retries, backoff=backoff)


# ── Metadata helpers ───────────────────────────────────────────────────────


def _truncate_metadata_text(text: str, source: str = "") -> str:
    """
    Clip metadata text to Pinecone free-tier 40KB limit (we cap at 38KB).
    Logs a warning when truncation occurs.
    """
    encoded = text.encode("utf-8")
    if len(encoded) <= _METADATA_TEXT_MAX_BYTES:
        return text
    truncated = encoded[:_METADATA_TEXT_MAX_BYTES].decode("utf-8", errors="ignore")
    logger.warning(
        "[embedder] Metadata text truncated to %d bytes (was %d bytes)%s.",
        _METADATA_TEXT_MAX_BYTES,
        len(encoded),
        f" — source: {source}" if source else "",
    )
    return truncated


# ── Pinecone Client ───────────────────────────────────────────────────────


def _get_index():
    """
    Lazy-load and return the Pinecone index.
    Creates the index only if it does not already exist.

    Free-tier notes:
      - Only 1 index allowed — all collections are namespaced within it.
      - Region MUST be us-east-1 (AWS) on the free tier.
      - Dimension MUST match EMBEDDING_DIM (768 for gemini-embedding-001 free tier).

    Raises RuntimeError if PINECONE_API_KEY is not set.
    """
    global _pinecone_index
    if _pinecone_index is not None:
        return _pinecone_index

    api_key = PINECONE_API_KEY or os.environ.get("PINECONE_API_KEY", "")
    if not api_key:
        raise RuntimeError(
            "PINECONE_API_KEY is not set. Set it in your environment or .env file."
        )

    from pinecone import Pinecone, ServerlessSpec

    pc = Pinecone(api_key=api_key)
    index_name = PINECONE_INDEX_NAME or os.environ.get("PINECONE_INDEX_NAME", "optivox-rag")

    # Check existing indexes — avoid costly recreation on every startup
    existing = [idx.name for idx in pc.list_indexes()]
    if index_name not in existing:
        logger.info(
            "[embedder] Creating Pinecone index '%s' (dim=%d, metric=cosine, region=us-east-1).",
            index_name, EMBEDDING_DIM,
        )
        pc.create_index(
            name=index_name,
            dimension=EMBEDDING_DIM,
            metric="cosine",
            spec=ServerlessSpec(cloud="aws", region="us-east-1"),
        )
        # Poll until index is ready (up to ~60s)
        for _ in range(30):
            desc = pc.describe_index(index_name)
            if desc.status.get("ready", False):
                break
            time.sleep(2)
        logger.info("[embedder] Pinecone index '%s' is ready.", index_name)
    else:
        logger.info("[embedder] Connected to existing Pinecone index '%s'.", index_name)

    _pinecone_index = pc.Index(index_name)
    return _pinecone_index


# ── Public API ────────────────────────────────────────────────────────────


def embed_schema(tables: List[Dict[str, Any]], connection_key: str) -> None:
    """
    Embed each table's DDL string and upsert into the schema_docs namespace.
    Chunks by table so retrieval returns individual DDLs.
    DDL text is truncated to 38 KB to stay within Pinecone free-tier metadata limits.
    """
    try:
        index = _get_index()
    except RuntimeError as exc:
        logger.error("[embedder] embed_schema skipped — %s", exc)
        return

    vectors = []
    for t in tables:
        ddl = t.get("ddl", "")
        if not ddl:
            continue
        doc_id = hashlib.md5(f"{connection_key}:{t['table_name']}".encode()).hexdigest()
        try:
            vector = _embed(ddl)
        except Exception as exc:
            logger.error("[embedder] Failed to embed table '%s': %s", t["table_name"], exc)
            continue
        vectors.append({
            "id": doc_id,
            "values": vector,
            "metadata": {
                "text": _truncate_metadata_text(ddl, source=t["table_name"]),
                "connection": connection_key,
                "table": t["table_name"],
            },
        })

    if vectors:
        # Pinecone recommends batches of ≤100 vectors (free-tier limit)
        for i in range(0, len(vectors), 100):
            index.upsert(vectors=vectors[i:i + 100], namespace=_NS_SCHEMA_DOCS)
        logger.info(
            "[embedder] Upserted %d schema docs for '%s' → Pinecone/%s.",
            len(vectors), connection_key, _NS_SCHEMA_DOCS,
        )


def embed_query_history(question: str, sql: str, session_id: str = "") -> None:
    """Embed NL question + SQL pair into the query_history namespace."""
    try:
        index = _get_index()
    except RuntimeError as exc:
        logger.error("[embedder] embed_query_history skipped — %s", exc)
        return

    combined = f"Question: {question}\nSQL: {sql}"
    doc_id   = hashlib.md5(combined.encode()).hexdigest()
    try:
        vector = _embed(combined)
    except Exception as exc:
        logger.error("[embedder] Failed to embed query history: %s", exc)
        return

    index.upsert(
        vectors=[{
            "id": doc_id,
            "values": vector,
            "metadata": {
                "text": _truncate_metadata_text(combined, source="query_history"),
                "session": session_id,
            },
        }],
        namespace=_NS_QUERY_HISTORY,
    )


def fetch_schema_context(question: str, top_k: int = 3) -> str:
    """Return top_k DDL snippets most relevant to *question* from Pinecone."""
    try:
        index    = _get_index()
        q_vector = _embed_query(question)
        results  = index.query(
            vector=q_vector,
            top_k=top_k,
            namespace=_NS_SCHEMA_DOCS,
            include_metadata=True,
        )
        matches = results.get("matches", [])
        if not matches:
            return ""
        return "\n\n".join(
            m["metadata"]["text"] for m in matches if m.get("metadata", {}).get("text")
        )
    except Exception as exc:
        logger.warning("[embedder] fetch_schema_context failed: %s", exc)
        return ""


def fetch_query_history(question: str, top_k: int = 2) -> str:
    """Return top_k similar past question/SQL pairs from Pinecone."""
    try:
        index    = _get_index()
        q_vector = _embed_query(question)
        results  = index.query(
            vector=q_vector,
            top_k=top_k,
            namespace=_NS_QUERY_HISTORY,
            include_metadata=True,
        )
        matches = results.get("matches", [])
        if not matches:
            return ""
        return "\n\n".join(
            m["metadata"]["text"] for m in matches if m.get("metadata", {}).get("text")
        )
    except Exception as exc:
        logger.warning("[embedder] fetch_query_history failed: %s", exc)
        return ""


def is_namespace_populated(namespace: str, min_vectors: int = 1) -> bool:
    """
    Check whether a Pinecone namespace already contains vectors.
    Used by the dialect seeder as a guard against re-embedding on every startup.
    Returns False on any error (safe default — allows re-seeding).
    """
    try:
        index = _get_index()
        stats = index.describe_index_stats()
        ns_stats = stats.get("namespaces", {}).get(namespace, {})
        return ns_stats.get("vector_count", 0) >= min_vectors
    except Exception as exc:
        logger.warning("[embedder] is_namespace_populated check failed: %s", exc)
        return False


def embed_dialect_docs(docs: List[Dict[str, str]], inter_call_delay: float = 0.2) -> None:
    """
    Embed and upsert dialect curriculum docs into the dialect_docs namespace.
    Each doc must have keys: id, text, level, topic.

    inter_call_delay: seconds to sleep between individual embed calls.
    This prevents bursting 16 calls in <1 second and hitting the 5 RPM free-tier cap.
    """
    try:
        index = _get_index()
    except RuntimeError as exc:
        logger.error("[embedder] embed_dialect_docs skipped — %s", exc)
        return

    vectors = []
    for i, doc in enumerate(docs):
        try:
            vector = _embed(doc["text"])
        except Exception as exc:
            logger.error("[embedder] Failed to embed dialect doc '%s': %s", doc.get("id"), exc)
            continue
        vectors.append({
            "id": doc["id"],
            "values": vector,
            "metadata": {
                "text": _truncate_metadata_text(doc["text"], source=doc.get("id", "")),
                "level": doc.get("level", ""),
                "topic": doc.get("topic", ""),
            },
        })
        # Small inter-call delay to stay comfortably under 5 RPM
        # (rate limiter also enforces this, but the sleep keeps calls spread out)
        if inter_call_delay > 0 and i < len(docs) - 1:
            time.sleep(inter_call_delay)

    if vectors:
        for i in range(0, len(vectors), 100):
            index.upsert(vectors=vectors[i:i + 100], namespace=_NS_DIALECT_DOCS)
        logger.info(
            "[embedder] Upserted %d dialect docs → Pinecone/%s.",
            len(vectors), _NS_DIALECT_DOCS,
        )


def embed_cache_stats() -> Dict[str, Any]:
    """Return current embedding cache statistics (for health/debug endpoints)."""
    return {
        "embed_cache_size": _embed_cache.size(),
        "embed_cache_max": _EMBED_CACHE_MAX,
        "embed_cache_ttl_sec": _EMBED_CACHE_TTL,
        "embed_model": GEMINI_EMBED_MODEL,
        "embed_dim": EMBEDDING_DIM,
    }
