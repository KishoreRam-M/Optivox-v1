"""
security/rbac_middleware.py
----------------------------
Phase 7 — Full RBAC FastAPI middleware.

Reads X-User-Role header on every protected endpoint.
Falls back to ADMIN if no header is present (dev mode).

In production, replace header parsing with JWT verification.
"""

from __future__ import annotations

import logging
from typing import Callable

from fastapi import Request, Response
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

from app.security.rbac import Role, check_permission

logger = logging.getLogger(__name__)

# Endpoints that require RBAC enforcement
_PROTECTED_PATHS = {"/api/execute", "/api/query", "/api/query/crew"}


class RBACMiddleware(BaseHTTPMiddleware):
    """
    FastAPI middleware that:
    1. Reads X-User-Role header and attaches role to request.state
    2. On /api/execute, pre-checks role vs SQL before handler runs
       (handler also checks — defence in depth)
    """

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        # Read role from header (dev fallback → ADMIN)
        raw_role = request.headers.get("X-User-Role", "admin").lower()
        try:
            role = Role(raw_role)
        except ValueError:
            role = Role.ADMIN
            logger.debug("Unknown role '%s' — defaulting to ADMIN.", raw_role)

        request.state.role = role

        # Pre-flight check for execute endpoint (SQL is in body)
        if request.method == "POST" and request.url.path == "/api/execute":
            try:
                body = await request.body()
                import json
                payload = json.loads(body)
                sql = payload.get("sql", "")
                if sql:
                    allowed, reason = check_permission(role, sql)
                    if not allowed:
                        logger.warning("RBAC blocked: role=%s reason=%s", role.value, reason)
                        return JSONResponse(
                            status_code=403,
                            content={"detail": "Access denied: your role does not permit this operation."},
                        )
            except Exception:
                pass  # Body parsing failure → let handler deal with it

            # Re-attach body so the handler can still read it
            from starlette.requests import Request as StarletteRequest
            from io import BytesIO
            request._body = body  # type: ignore[attr-defined]

        response = await call_next(request)
        # Attach role in response header for debugging
        response.headers["X-Applied-Role"] = role.value
        return response
