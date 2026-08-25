"""
agents/llm_client.py
--------------------
LLM client using langchain-google-genai.

Supports user-provided API keys (sent per-request from the frontend).
Falls back to the GEMINI_API_KEY environment variable if no key is provided.

Gemini 2.5 Flash free tier limits (LLM):
  - 15 RPM  (requests per minute)
  - 1M  TPM (tokens per minute)
  - 1500 RPD (requests per day)

Gemini embedding-001 free tier limits:
  - 5 RPM  (requests per minute)  ← enforced by _EmbedRateLimiter in embedder.py
  - 100 RPD (requests per day)    ← conserved by in-memory embed cache

Optimized settings:
  - temperature=0 for deterministic, reproducible SQL
  - max_retries=3 with exponential backoff for 429 handling
  - thinking_budget=0 disables expensive chain-of-thought on flash, saving tokens
"""

from __future__ import annotations

import os
import logging
import time
from typing import Optional

from langchain_google_genai import ChatGoogleGenerativeAI

logger = logging.getLogger(__name__)

_MODEL = "gemini-2.5-flash"
_THINKING_BUDGET = 0   # disable thinking on flash to save tokens + latency


def get_llm(api_key: Optional[str] = None) -> ChatGoogleGenerativeAI:
    """
    Return a ChatGoogleGenerativeAI instance.

    Args:
        api_key: Per-request API key from the frontend user.
                 If None, falls back to GEMINI_API_KEY env var.
    """
    key = api_key or os.environ.get("GEMINI_API_KEY", "")
    if not key:
        logger.warning("No Gemini API key available — LLM calls will fail.")

    return ChatGoogleGenerativeAI(
        model=_MODEL,
        google_api_key=key,
        temperature=0.0,
        max_retries=3,          # built-in retry with backoff for 429
        # Disable chain-of-thought thinking to save tokens on the free tier
        thinking={"thinking_budget": _THINKING_BUDGET},
    )


def call_with_retry(
    llm: ChatGoogleGenerativeAI,
    messages: list,
    max_attempts: int = 3,
    base_delay: float = 4.0,
) -> str:
    """
    Invoke the LLM with manual retry + exponential backoff.
    Handles Gemini free tier 429 (rate limit) errors gracefully.

    Returns the response content string.
    Raises the last exception if all attempts fail.
    """
    last_exc = None
    for attempt in range(max_attempts):
        try:
            response = llm.invoke(messages)
            return response.content.strip()
        except Exception as exc:
            last_exc = exc
            err_str = str(exc).lower()
            is_rate_limit = "429" in err_str or "quota" in err_str or "resource_exhausted" in err_str
            if is_rate_limit and attempt < max_attempts - 1:
                delay = base_delay * (2 ** attempt)
                logger.warning(
                    "[llm] Rate limited (attempt %d/%d). Waiting %.1fs…",
                    attempt + 1, max_attempts, delay,
                )
                time.sleep(delay)
            else:
                break
    raise last_exc
