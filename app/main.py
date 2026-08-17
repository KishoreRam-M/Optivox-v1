"""
app/main.py
-----------
OptiVox DB — Agentic AI Backend (v4.1 — Vercel/Free-Tier Optimized)

Key optimizations for Gemini 2.5 Flash free tier:
  - User-provided Gemini API key via X-Gemini-API-Key header
  - Response caching (LRU + TTL) to reduce RPD consumption
  - Retry + exponential backoff for 429 rate-limit errors
  - Token-optimized prompts in all LiteLLM calls
  - Graceful degradation when API key is missing
  - CORS reads ALLOWED_ORIGINS from env (safe for production)
  - Configurable paths for LanceDB and Audit DB
  - asyncio.get_running_loop() throughout (Python 3.10+ compatible)
  - ws_tutor bug fixed (no asyncio.run inside running loop)
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import sys
import time
import traceback
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv

load_dotenv()

# ── Windows UTF-8 fix ─────────────────────────────────────────────────────
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")

from fastapi import FastAPI, HTTPException, Header, Request, WebSocket, WebSocketDisconnect, BackgroundTasks, status
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from app.models.auth import ConnectionModel, QueryRequest, ExecuteRequest
from app.models.query_plan import ValidatedQueryPlan
from app.database.connector import test_connection, get_engine, _conn_key
from app.tools.sql_parser import validate_sql_ast
from app.audit.audit_log import init_audit_db, log_audit_event, classify_severity
from app.api.playground import router as playground_router
from app.api.csv_db import router as csv_db_router
from app.core.cache import get_cache

import litellm

# ── Logging ───────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("OptiVox")

# ── Global state ──────────────────────────────────────────────────────────

_executor = ThreadPoolExecutor(max_workers=4)
_session_histories: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
_active_connections: List[Dict[str, Any]] = []
_ws_clients: List[WebSocket] = []

MAX_SESSION_HISTORY = 5

# ── CORS ──────────────────────────────────────────────────────────────────

def _get_allowed_origins() -> List[str]:
    raw = os.environ.get("ALLOWED_ORIGINS", "*")
    return ["*"] if raw == "*" else [o.strip() for o in raw.split(",") if o.strip()]


# ── API key helper ────────────────────────────────────────────────────────

def _resolve_api_key(header_key: Optional[str]) -> str:
    """Return the user-provided key, or fall back to the env var."""
    return (header_key or "").strip() or os.environ.get("GEMINI_API_KEY", "")


def _require_api_key(header_key: Optional[str]) -> str:
    """Resolve key and raise 401 if neither source has one."""
    key = _resolve_api_key(header_key)
    if not key:
        raise HTTPException(
            status_code=401,
            detail="No Gemini API key provided. Pass your key in the X-Gemini-API-Key header.",
        )
    return key


# ── LiteLLM call with retry ───────────────────────────────────────────────

def _litellm_call(
    messages: list,
    api_key: str,
    model: str = "gemini/gemini-2.5-flash",
    stream: bool = False,
    max_attempts: int = 3,
    base_delay: float = 4.0,
):
    """LiteLLM call with exponential-backoff retry for 429 errors."""
    last_exc = None
    for attempt in range(max_attempts):
        try:
            return litellm.completion(
                model=model,
                messages=messages,
                api_key=api_key,
                stream=stream,
                temperature=0.0,
            )
        except Exception as exc:
            last_exc = exc
            err_str = str(exc).lower()
            if ("429" in err_str or "quota" in err_str or "rate" in err_str) and attempt < max_attempts - 1:
                delay = base_delay * (2 ** attempt)
                logger.warning("[litellm] Rate limited (attempt %d). Waiting %.1fs", attempt + 1, delay)
                time.sleep(delay)
            else:
                break
    raise last_exc


# ── Lifespan ──────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("OptiVox DB starting up (free-tier optimized).")
    init_audit_db()

    # Pre-seed dialect docs (RAG)
    from app.rag.dialect_seeder import seed_dialect_docs
    try:
        seed_dialect_docs()
    except Exception as e:
        logger.error("Failed to seed dialect docs: %s", e)

    # Pre-warm LangGraph pipeline
    try:
        from app.agents.graph import get_graph
        get_graph()
        logger.info("LangGraph pipeline ready.")
    except Exception as e:
        logger.error("LangGraph warm-up failed: %s", e)

    asyncio.create_task(_drift_loop())
    asyncio.create_task(_cleanup_rate_limits())
    yield
    logger.info("OptiVox DB shutting down.")
    _executor.shutdown(wait=False)


async def _drift_loop():
    from app.rag.drift_detector import drift_detection_loop

    async def notify_clients(conn_key: str, changed_tables: List[str]):
        msg = json.dumps({"type": "schema_drift", "connection": conn_key, "changed_tables": changed_tables})
        for ws in list(_ws_clients):
            try:
                await ws.send_text(msg)
            except Exception:
                pass

    def on_drift(conn_key: str, changed_tables: List[str]):
        asyncio.create_task(notify_clients(conn_key, changed_tables))

    await drift_detection_loop(
        get_active_connections=lambda: _active_connections,
        on_drift=on_drift,
        interval_sec=600,
    )


# ── FastAPI app ───────────────────────────────────────────────────────────

app = FastAPI(
    title="OptiVox DB — Agentic AI",
    description="NL→SQL with LangGraph agents. Optimized for Gemini 2.5 Flash free tier.",
    version="4.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=_get_allowed_origins(),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*", "X-Gemini-API-Key"],
)

app.include_router(playground_router)
app.include_router(csv_db_router)

# ── Rate limiting ─────────────────────────────────────────────────────────

_rate_limits: Dict[str, List[float]] = defaultdict(list)
RATE_LIMIT_MAX = 60
RATE_LIMIT_WINDOW = 60


def _check_rate_limit(client_ip: str) -> bool:
    now = time.time()
    _rate_limits[client_ip] = [t for t in _rate_limits[client_ip] if now - t < RATE_LIMIT_WINDOW]
    if len(_rate_limits[client_ip]) >= RATE_LIMIT_MAX:
        return False
    _rate_limits[client_ip].append(now)
    return True


async def _cleanup_rate_limits():
    while True:
        await asyncio.sleep(600)
        now = time.time()
        stale = [ip for ip, times in _rate_limits.items() if not [t for t in times if now - t < RATE_LIMIT_WINDOW]]
        for ip in stale:
            _rate_limits.pop(ip, None)


# ── Middleware ────────────────────────────────────────────────────────────

@app.middleware("http")
async def security_and_logging_middleware(request: Request, call_next):
    start = time.time()
    try:
        response = await call_next(request)
    except Exception as exc:
        logger.error("Unhandled: %s", exc, exc_info=True)
        response = JSONResponse(status_code=500, content={"detail": "Internal server error."})
    response.headers.update({
        "X-Content-Type-Options": "nosniff",
        "X-Frame-Options": "DENY",
        "X-XSS-Protection": "1; mode=block",
        "Strict-Transport-Security": "max-age=31536000; includeSubDomains",
    })
    logger.info("%s %s → %d (%.3fs)", request.method, request.url.path, response.status_code, time.time() - start)
    return response


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content={"detail": "Invalid request payload.", "errors": exc.errors()},
    )


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})


# ── Health ────────────────────────────────────────────────────────────────

@app.get("/health", tags=["System"])
async def health():
    cache = get_cache()
    return {
        "status": "ok",
        "version": "4.1.0",
        "model": "gemini-2.5-flash",
        "cache_size": cache.size(),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


# ── Database connect ──────────────────────────────────────────────────────

@app.post("/api/connect", tags=["Database"])
async def connect_database(conn: ConnectionModel, background_tasks: BackgroundTasks):
    """Test connection and trigger background schema embedding."""
    try:
        info = test_connection(conn.model_dump())
        conn_dict = conn.model_dump()
        key = _conn_key(conn_dict)
        if not any(_conn_key(c) == key for c in _active_connections):
            _active_connections.append(conn_dict)
        background_tasks.add_task(_embed_schema_bg, conn_dict)
        return {"status": "connected", "info": info}
    except Exception as exc:
        logger.error("Connection failed: %s", exc)
        raise HTTPException(status_code=400, detail=f"Connection failed: {exc}")


async def _embed_schema_bg(conn: Dict[str, Any]):
    try:
        from app.database.schema_extractor import extract_schema
        from app.rag.embedder import embed_schema
        loop = asyncio.get_running_loop()
        engine = await loop.run_in_executor(_executor, lambda: get_engine(conn))
        tables = await loop.run_in_executor(_executor, lambda: extract_schema(engine, conn.get("dialect", "mysql")))
        conn_key = _conn_key(conn)
        await loop.run_in_executor(_executor, lambda: embed_schema(tables, conn_key))
        logger.info("Schema embedded: %s (%d tables)", conn_key, len(tables))
    except Exception as exc:
        logger.error("Schema embedding failed: %s", exc)


# ── NL-to-SQL (fast single call) ─────────────────────────────────────────

@app.post("/api/query", tags=["Query"])
async def generate_query(
    req: QueryRequest,
    request: Request,
    x_gemini_api_key: Optional[str] = Header(default=None),
):
    """Fast SQL generation — single LiteLLM call. Uses cache for duplicate questions."""
    client_ip = request.client.host if request.client else "unknown"
    if not _check_rate_limit(client_ip):
        raise HTTPException(status_code=429, detail="Rate limit exceeded.")

    api_key = _require_api_key(x_gemini_api_key)
    session_id = req.session_id or "default"
    history = _session_histories[session_id][-MAX_SESSION_HISTORY:]

    try:
        from app.rag.embedder import fetch_schema_context, fetch_query_history
        schema_context = fetch_schema_context(req.question)
        few_shot = fetch_query_history(req.question)
    except Exception:
        schema_context, few_shot = "", ""

    history_str = json.dumps(history[-3:]) if history else "None"
    prompt = f"""Generate a {req.dialect.upper()} SQL query.

Request: {req.question}
Schema: {schema_context or "No schema — use best judgment."}
{f"Examples:{chr(10)}{few_shot}" if few_shot else ""}
History: {history_str}

Return ONLY the SQL. No markdown. No explanation."""

    # Check cache first
    cache = get_cache()
    cached = cache.get(prompt)
    if cached:
        logger.info("[/api/query] Cache hit")
        sql = cached
    else:
        try:
            loop = asyncio.get_running_loop()
            response = await loop.run_in_executor(
                _executor,
                lambda: _litellm_call([{"role": "user", "content": prompt}], api_key),
            )
            sql = response.choices[0].message.content.strip()
            sql = re.sub(r"```(?:sql)?\s*", "", sql, flags=re.IGNORECASE)
            sql = re.sub(r"```", "", sql).strip()
            cache.set(prompt, sql)
        except Exception as exc:
            logger.error("/api/query failed: %s", traceback.format_exc())
            raise HTTPException(status_code=500, detail=_friendly_error(exc))

    is_safe, reason = validate_sql_ast(sql, req.dialect)
    return {"sql": sql, "dialect": req.dialect, "safe": is_safe, "safety_reason": reason, "session_id": session_id}


def _friendly_error(exc: Exception) -> str:
    """Convert LLM errors to user-friendly messages."""
    msg = str(exc).lower()
    if "429" in msg or "quota" in msg or "rate" in msg:
        return "Gemini free tier rate limit reached. Please wait 60 seconds and try again."
    if "401" in msg or "api_key" in msg or "invalid" in msg:
        return "Invalid Gemini API key. Please check your key in Settings."
    if "timeout" in msg:
        return "Request timed out. Please try again."
    return "AI service error. Please try again in a moment."


# ── Agentic query (LangGraph) ─────────────────────────────────────────────

@app.post("/api/query/agent", tags=["Query"])
async def generate_query_agent(
    req: QueryRequest,
    request: Request,
    x_gemini_api_key: Optional[str] = Header(default=None),
):
    """Full LangGraph pipeline — Architect → Generator → Reviewer with self-healing."""
    client_ip = request.client.host if request.client else "unknown"
    if not _check_rate_limit(client_ip):
        raise HTTPException(status_code=429, detail="Rate limit exceeded.")

    api_key = _require_api_key(x_gemini_api_key)
    session_id = req.session_id or "default"
    history = _session_histories[session_id][-MAX_SESSION_HISTORY:]
    history_str = json.dumps(history[-3:]) if history else "None"

    try:
        from app.agents.graph import run_agent_pipeline
        loop = asyncio.get_running_loop()
        plan: ValidatedQueryPlan = await asyncio.wait_for(
            loop.run_in_executor(
                _executor,
                lambda: run_agent_pipeline(req.question, req.dialect, history_str, api_key),
            ),
            timeout=120.0,
        )
        _session_histories[session_id].append({"role": "user", "content": req.question, "sql": plan.sql})
        return plan.model_dump()
    except asyncio.TimeoutError:
        raise HTTPException(status_code=408, detail="Agent pipeline timed out (120s). Try fast mode.")
    except Exception as exc:
        logger.error("/api/query/agent failed: %s", traceback.format_exc())
        raise HTTPException(status_code=500, detail=_friendly_error(exc))


# Backward-compat alias
@app.post("/api/query/crew", tags=["Query"], include_in_schema=False)
async def generate_query_crew_compat(
    req: QueryRequest,
    request: Request,
    x_gemini_api_key: Optional[str] = Header(default=None),
):
    return await generate_query_agent(req, request, x_gemini_api_key)


# ── Execute ───────────────────────────────────────────────────────────────

@app.post("/api/execute", tags=["Query"])
async def execute_query(req: ExecuteRequest, request: Request):
    """Execute validated SQL against the connected database."""
    client_ip = request.client.host if request.client else "unknown"
    if not _check_rate_limit(client_ip):
        raise HTTPException(status_code=429, detail="Rate limit exceeded.")

    is_safe, safety_reason = validate_sql_ast(req.sql, req.connection.dialect)
    severity = classify_severity(req.sql)
    if severity == "DANGER":
        log_audit_event(
            session_id=req.session_id or "default",
            severity=severity, sql_text=req.sql, dialect=req.connection.dialect,
            host=req.connection.host, database=req.connection.database, approved=is_safe,
        )
    if not is_safe:
        raise HTTPException(status_code=400, detail=f"Unsafe SQL: {safety_reason}")

    try:
        from sqlalchemy import text as sa_text
        loop = asyncio.get_running_loop()

        def _run():
            engine = get_engine(req.connection.model_dump())
            start = time.time()
            statements = [s.strip() for s in req.sql.split(';') if s.strip()]
            last_result, total_rows_affected, executed = None, 0, 0
            with engine.connect() as conn:
                for stmt in statements:
                    result = conn.execute(sa_text(stmt))
                    executed += 1
                    if result.returns_rows:
                        columns = list(result.keys())
                        rows = [list(row) for row in result.fetchall()]
                        last_result = {"columns": columns, "rows": rows, "row_count": len(rows)}
                    else:
                        total_rows_affected += result.rowcount if result.rowcount != -1 else 0
                conn.commit()
            duration_ms = int((time.time() - start) * 1000)
            if last_result:
                return {**last_result, "duration_ms": duration_ms, "statements_executed": executed}
            return {"columns": [], "rows": [], "rows_affected": total_rows_affected, "duration_ms": duration_ms, "statements_executed": executed}

        data = await loop.run_in_executor(_executor, _run)
        session_id = req.session_id or "default"
        try:
            from app.rag.embedder import embed_query_history
            await loop.run_in_executor(_executor, lambda: embed_query_history(req.sql, req.sql, session_id))
        except Exception:
            pass
        return {"status": "success", **data}
    except Exception as exc:
        logger.error("Execute failed: %s", exc)
        raise HTTPException(status_code=400, detail=f"Query execution failed: {str(exc)}")


# ── Schema endpoint ───────────────────────────────────────────────────────

@app.post("/api/schema", tags=["Database"])
async def get_schema(conn: ConnectionModel):
    try:
        from app.database.schema_extractor import extract_schema
        loop = asyncio.get_running_loop()
        engine = await loop.run_in_executor(_executor, lambda: get_engine(conn.model_dump()))
        tables = await loop.run_in_executor(_executor, lambda: extract_schema(engine, conn.dialect))
        return {"status": "ok", "tables": tables, "table_count": len(tables)}
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


# ── ADIA: NL→SQL ──────────────────────────────────────────────────────────

class ADIANLRequest(BaseModel):
    question: str
    dialect: str = Field("mysql", pattern="^(mysql|postgres|oracle|mssql)$")
    connection: Optional[Any] = None
    session_id: Optional[str] = None
    mode: str = Field("fast", pattern="^(fast|agent)$")


@app.post("/api/adia/nl-sql", tags=["ADIA"])
async def adia_nl_sql(
    req: ADIANLRequest,
    request: Request,
    x_gemini_api_key: Optional[str] = Header(default=None),
):
    """NL→SQL. mode=fast (1 LLM call) or mode=agent (LangGraph pipeline)."""
    client_ip = request.client.host if request.client else "unknown"
    if not _check_rate_limit(client_ip):
        raise HTTPException(status_code=429, detail="Rate limit exceeded.")

    api_key = _require_api_key(x_gemini_api_key)
    session_id = req.session_id or "default"
    history = _session_histories[session_id][-MAX_SESSION_HISTORY:]

    try:
        from app.rag.embedder import fetch_schema_context, fetch_query_history
        schema_context = fetch_schema_context(req.question)
        few_shot = fetch_query_history(req.question)
    except Exception:
        schema_context, few_shot = "", ""

    if req.mode == "agent":
        history_str = json.dumps(history[-3:]) if history else "None"
        try:
            from app.agents.graph import run_agent_pipeline
            loop = asyncio.get_running_loop()
            plan: ValidatedQueryPlan = await asyncio.wait_for(
                loop.run_in_executor(_executor, lambda: run_agent_pipeline(req.question, req.dialect, history_str, api_key)),
                timeout=120.0,
            )
            _session_histories[session_id].append({"role": "user", "content": req.question, "sql": plan.sql})
            return {
                "section": "nl_sql", "mode": "agent",
                "sql": plan.sql, "dialect": plan.dialect,
                "approved": plan.approved, "is_destructive": plan.is_destructive,
                "rejection_reason": plan.rejection_reason, "session_id": session_id,
            }
        except asyncio.TimeoutError:
            raise HTTPException(status_code=408, detail="Agent pipeline timed out.")
        except Exception as exc:
            logger.error("ADIA NL-SQL agent failed: %s", traceback.format_exc())
            raise HTTPException(status_code=500, detail=_friendly_error(exc))
    else:
        # Fast mode
        history_str = json.dumps(history[-3:]) if history else "None"
        prompt = f"""Generate a {req.dialect.upper()} SQL query.

Request: {req.question}
Schema: {schema_context or "No schema."}
{f"Examples:{chr(10)}{few_shot}" if few_shot else ""}
History: {history_str}

Return ONLY the SQL. No markdown."""
        cache = get_cache()
        cached = cache.get(prompt)
        if cached:
            sql = cached
        else:
            try:
                loop = asyncio.get_running_loop()
                resp = await loop.run_in_executor(
                    _executor,
                    lambda: _litellm_call([{"role": "user", "content": prompt}], api_key),
                )
                sql = resp.choices[0].message.content.strip()
                sql = re.sub(r"```(?:sql)?\s*", "", sql, flags=re.IGNORECASE)
                sql = re.sub(r"```", "", sql).strip()
                cache.set(prompt, sql)
            except Exception as exc:
                logger.error("ADIA NL-SQL fast failed: %s", traceback.format_exc())
                raise HTTPException(status_code=500, detail=_friendly_error(exc))
        is_safe, safety_reason = validate_sql_ast(sql, req.dialect)
        _session_histories[session_id].append({"role": "user", "content": req.question, "sql": sql})
        return {
            "section": "nl_sql", "mode": "fast",
            "sql": sql, "dialect": req.dialect,
            "safe": is_safe, "safety_reason": safety_reason, "session_id": session_id,
        }


# ── ADIA: Teach ───────────────────────────────────────────────────────────

class ADIATeachRequest(BaseModel):
    question: str
    session_id: Optional[str] = None


@app.post("/api/adia/teach", tags=["ADIA"])
async def adia_teach(
    req: ADIATeachRequest,
    request: Request,
    x_gemini_api_key: Optional[str] = Header(default=None),
):
    """Database tutor — structured SQL lesson with examples."""
    client_ip = request.client.host if request.client else "unknown"
    if not _check_rate_limit(client_ip):
        raise HTTPException(status_code=429, detail="Rate limit exceeded.")

    api_key = _require_api_key(x_gemini_api_key)
    session_id = req.session_id or "default"
    history = _session_histories[session_id][-3:]  # reduced to 3 for token efficiency

    # Check cache for identical questions
    cache = get_cache()
    cache_key = f"teach:{req.question}"
    cached = cache.get(cache_key)
    if cached and not history:
        return {"section": "teach", "answer": cached, "session_id": session_id, "cached": True}

    messages = [
        {"role": "system", "content": "You are a SQL expert educator. Give concise numbered lessons (max 4 points) with SQL examples. Be direct — no preamble."},
    ]
    for h in history:
        if h.get("role") and h.get("content"):
            messages.append({"role": h["role"], "content": h["content"]})
    messages.append({"role": "user", "content": req.question})

    try:
        loop = asyncio.get_running_loop()
        resp = await loop.run_in_executor(_executor, lambda: _litellm_call(messages, api_key))
        answer = resp.choices[0].message.content.strip()
        if not history:
            cache.set(cache_key, answer)
        _session_histories[session_id].append({"role": "assistant", "content": answer})
        return {"section": "teach", "answer": answer, "session_id": session_id}
    except Exception as exc:
        logger.error("ADIA teach failed: %s", traceback.format_exc())
        raise HTTPException(status_code=500, detail=_friendly_error(exc))


# ── ADIA: Optimize ────────────────────────────────────────────────────────

class ADIAOptimizeRequest(BaseModel):
    sql: str
    dialect: str = Field("mysql", pattern="^(mysql|postgres|oracle|mssql)$")
    explain_output: Optional[str] = None
    session_id: Optional[str] = None


@app.post("/api/adia/optimize", tags=["ADIA"])
async def adia_optimize(
    req: ADIAOptimizeRequest,
    request: Request,
    x_gemini_api_key: Optional[str] = Header(default=None),
):
    """Query optimizer — returns structured performance analysis as JSON."""
    client_ip = request.client.host if request.client else "unknown"
    if not _check_rate_limit(client_ip):
        raise HTTPException(status_code=429, detail="Rate limit exceeded.")

    api_key = _require_api_key(x_gemini_api_key)

    explain_ctx = f"\nEXPLAIN output:\n{req.explain_output}" if req.explain_output else ""
    prompt = f"""Analyze this {req.dialect.upper()} SQL and return ONLY a JSON object:
{{"issues":[{{"type":"...","description":"..."}}],"rewritten_sql":"...","tips":["..."],"index_suggestions":[{{"table":"...","column":"...","reason":"..."}}]}}

SQL: {req.sql}{explain_ctx}

JSON only. No markdown."""

    cache = get_cache()
    cached = cache.get(prompt)
    if cached:
        try:
            return {"section": "optimize", "dialect": req.dialect, **json.loads(cached)}
        except Exception:
            pass

    try:
        loop = asyncio.get_running_loop()
        resp = await loop.run_in_executor(_executor, lambda: _litellm_call([{"role": "user", "content": prompt}], api_key))
        raw = resp.choices[0].message.content.strip()
        raw = re.sub(r"```(?:json)?\s*", "", raw, flags=re.IGNORECASE)
        raw = re.sub(r"```", "", raw).strip()
        cache.set(prompt, raw)
        try:
            analysis = json.loads(raw)
        except json.JSONDecodeError:
            analysis = {"raw_response": raw}
        return {"section": "optimize", "dialect": req.dialect, **analysis}
    except Exception as exc:
        logger.error("ADIA optimize failed: %s", traceback.format_exc())
        raise HTTPException(status_code=500, detail=_friendly_error(exc))


# ── ADIA: Schema Analysis ─────────────────────────────────────────────────

@app.post("/api/adia/schema-analysis", tags=["ADIA"])
async def adia_schema_analysis(
    conn: ConnectionModel,
    request: Request,
    x_gemini_api_key: Optional[str] = Header(default=None),
):
    """Schema analysis with AI-generated DBA recommendations."""
    client_ip = request.client.host if request.client else "unknown"
    if not _check_rate_limit(client_ip):
        raise HTTPException(status_code=429, detail="Rate limit exceeded.")

    api_key = _require_api_key(x_gemini_api_key)

    try:
        from app.database.schema_extractor import extract_schema
        from app.api.schema_analysis import analyze_schema
        loop = asyncio.get_running_loop()
        engine = await loop.run_in_executor(_executor, lambda: get_engine(conn.model_dump()))
        tables = await loop.run_in_executor(_executor, lambda: extract_schema(engine, conn.dialect))
        analysis = await loop.run_in_executor(_executor, lambda: analyze_schema(tables, engine))

        issues_summary = ""
        if analysis.get("missing_index_suggestions"):
            issues_summary = "Missing indexes: " + "; ".join(f"{s['table']}.{s['column']}" for s in analysis["missing_index_suggestions"][:5])
        if analysis.get("isolated_tables"):
            issues_summary += " | Isolated: " + ", ".join(analysis["isolated_tables"][:5])

        recommendations: List[str] = []
        if issues_summary:
            try:
                rec_prompt = f"DB has issues: {issues_summary}\nGive 3 DBA recommendations. Return JSON array of strings only."
                rec_resp = await loop.run_in_executor(
                    _executor,
                    lambda: _litellm_call([{"role": "user", "content": rec_prompt}], api_key),
                )
                raw_rec = rec_resp.choices[0].message.content.strip()
                raw_rec = re.sub(r"```(?:json)?\s*", "", raw_rec, flags=re.IGNORECASE)
                raw_rec = re.sub(r"```", "", raw_rec).strip()
                recommendations = json.loads(raw_rec)
            except Exception:
                recommendations = []

        return {
            "section": "schema_analysis", "status": "ok",
            **analysis,
            "ai_recommendations": recommendations,
            "performance_tips": [
                "Add indexes on all foreign key columns.",
                "Every table needs a primary key.",
                "Isolated tables may indicate orphaned data — review.",
                "Partition large tables (>10M rows) on filtered columns.",
            ],
        }
    except Exception as exc:
        logger.error("Schema analysis failed: %s", exc)
        raise HTTPException(status_code=400, detail="Schema analysis failed. Verify your connection.")


# ── WebSocket: query streaming ────────────────────────────────────────────

@app.websocket("/ws/query")
async def ws_query(websocket: WebSocket):
    """WebSocket: stream agent status updates and return final SQL plan."""
    await websocket.accept()
    _ws_clients.append(websocket)
    try:
        while True:
            data = await websocket.receive_text()
            payload = json.loads(data)
            req = QueryRequest(**payload)
            api_key = _resolve_api_key(payload.get("api_key", ""))
            if not api_key:
                await websocket.send_text(json.dumps({"status": "error", "message": "No API key provided."}))
                continue

            session_id = req.session_id or "default"
            history = _session_histories[session_id][-MAX_SESSION_HISTORY:]
            history_str = json.dumps(history[-3:]) if history else "None"

            async def send(msg: str, s: str = "progress"):
                await websocket.send_text(json.dumps({"status": s, "message": msg}))

            await send("Architect is analyzing your request…", "architect")
            try:
                from app.agents.graph import run_agent_pipeline
                loop = asyncio.get_running_loop()
                await send("Generator is writing SQL…", "generator")
                plan: ValidatedQueryPlan = await asyncio.wait_for(
                    loop.run_in_executor(_executor, lambda: run_agent_pipeline(req.question, req.dialect, history_str, api_key)),
                    timeout=120.0,
                )
                await send("Reviewer is checking safety…", "reviewer")
                await asyncio.sleep(0.05)
                _session_histories[session_id].append({"role": "user", "content": req.question, "sql": plan.sql})
                await websocket.send_text(json.dumps({"status": "done", "plan": plan.model_dump()}))
            except asyncio.TimeoutError:
                await websocket.send_text(json.dumps({"status": "error", "message": "Timed out (120s). Try fast mode."}))
            except Exception as exc:
                await websocket.send_text(json.dumps({"status": "error", "message": _friendly_error(exc)}))

    except WebSocketDisconnect:
        if websocket in _ws_clients:
            _ws_clients.remove(websocket)


# ── WebSocket: tutor streaming ────────────────────────────────────────────

@app.websocket("/ws/tutor")
async def ws_tutor(websocket: WebSocket):
    """WebSocket: stream tutor responses token-by-token."""
    await websocket.accept()
    try:
        while True:
            data = await websocket.receive_text()
            payload = json.loads(data)
            question = payload.get("question", "")
            session_id = payload.get("session_id", "default")
            api_key = _resolve_api_key(payload.get("api_key", ""))
            if not api_key:
                await websocket.send_text(json.dumps({"status": "error", "message": "No API key provided."}))
                continue

            history = _session_histories[session_id][-3:]
            messages = [
                {"role": "system", "content": "SQL educator. Concise numbered lessons with SQL examples."},
            ]
            for h in history:
                if h.get("role") and h.get("content"):
                    messages.append({"role": h["role"], "content": h["content"]})
            messages.append({"role": "user", "content": question})

            # FIXED: collect chunks sync in executor, then send async
            loop = asyncio.get_running_loop()

            def _stream_sync():
                chunks = []
                try:
                    resp = _litellm_call(messages, api_key, stream=True)
                    for chunk in resp:
                        delta = chunk.choices[0].delta.content or ""
                        if delta:
                            chunks.append(delta)
                except Exception as exc:
                    chunks.append(f"\n[Error: {_friendly_error(exc)}]")
                return chunks

            try:
                chunks = await loop.run_in_executor(_executor, _stream_sync)
                full_text = ""
                for delta in chunks:
                    full_text += delta
                    await websocket.send_text(json.dumps({"status": "token", "token": delta}))
                await websocket.send_text(json.dumps({"status": "done", "full": full_text}))
                _session_histories[session_id].append({"role": "assistant", "content": full_text})
            except Exception as exc:
                await websocket.send_text(json.dumps({"status": "error", "message": _friendly_error(exc)}))
    except WebSocketDisconnect:
        pass


# ── WebSocket: unified chat ───────────────────────────────────────────────

@app.websocket("/ws/chat")
async def ws_chat(websocket: WebSocket):
    """Unified chat: classifies and routes to query pipeline, tutor, or optimizer."""
    await websocket.accept()
    _ws_clients.append(websocket)
    try:
        while True:
            data = await websocket.receive_text()
            payload = json.loads(data)
            question = payload.get("question", "")
            dialect = payload.get("dialect", "mysql")
            session_id = payload.get("session_id", "default")
            api_key = _resolve_api_key(payload.get("api_key", ""))
            if not api_key:
                await websocket.send_text(json.dumps({"status": "error", "message": "No API key provided."}))
                continue

            await websocket.send_text(json.dumps({"status": "routing", "message": "Classifying request…"}))
            loop = asyncio.get_running_loop()

            def _classify():
                resp = _litellm_call([
                    {"role": "system", "content": "Classify as QUERY, TUTOR, or ANALYZE. Return ONLY one word."},
                    {"role": "user", "content": question},
                ], api_key)
                return resp.choices[0].message.content.strip().upper()

            intent = await loop.run_in_executor(_executor, _classify)
            await websocket.send_text(json.dumps({"status": "classified", "intent": intent}))

            if intent == "TUTOR":
                def _tutor():
                    resp = _litellm_call([
                        {"role": "system", "content": "SQL educator. Give concise numbered lessons."},
                        {"role": "user", "content": question},
                    ], api_key)
                    return resp.choices[0].message.content.strip()
                answer = await loop.run_in_executor(_executor, _tutor)
                await websocket.send_text(json.dumps({"status": "done", "agent": "tutor", "answer": answer}))

            elif intent == "ANALYZE":
                await websocket.send_text(json.dumps({
                    "status": "done", "agent": "optimizer",
                    "answer": "Connect to a database and run a query first to enable optimizer analysis.",
                }))

            else:  # QUERY
                try:
                    from app.agents.graph import run_agent_pipeline
                    history = _session_histories[session_id][-3:]
                    history_str = json.dumps(history) if history else "None"
                    await websocket.send_text(json.dumps({"status": "architect", "message": "Planning…"}))
                    plan: ValidatedQueryPlan = await asyncio.wait_for(
                        loop.run_in_executor(_executor, lambda: run_agent_pipeline(question, dialect, history_str, api_key)),
                        timeout=120.0,
                    )
                    await websocket.send_text(json.dumps({"status": "done", "agent": "query_pipeline", "plan": plan.model_dump()}))
                except Exception as exc:
                    await websocket.send_text(json.dumps({"status": "error", "message": _friendly_error(exc)}))

    except WebSocketDisconnect:
        if websocket in _ws_clients:
            _ws_clients.remove(websocket)


# ── Entry point ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)
