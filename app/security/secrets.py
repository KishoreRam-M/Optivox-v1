"""
security/secrets.py
-------------------
Credential loader with a clean interface designed for easy swap to
AWS Secrets Manager or HashiCorp Vault in production.

Usage:
    from app.security.secrets import get_secret
    api_key = get_secret("GEMINI_API_KEY")
"""

from __future__ import annotations

import os
import logging
from functools import lru_cache

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


@lru_cache(maxsize=None)
def get_secret(key: str, default: str = "") -> str:
    """
    Return a secret value by key.

    Dev:  reads from os.environ (populated by .env via dotenv).
    Prod: swap this function's body to call Vault / AWS SM.
    """
    value = os.environ.get(key, default)
    if not value:
        logger.warning("Secret '%s' is not set — using empty string.", key)
    return value


# ── Convenience accessors ──────────────────────────────────────────────────

def gemini_api_key() -> str:
    return get_secret("GEMINI_API_KEY")
