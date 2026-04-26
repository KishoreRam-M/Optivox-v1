"""
app/main.py
-----------
OptiVox DB — Agentic AI Backend
FastAPI application with:
  - HTTP endpoints (connect, query, execute, schema, ERD)
  - WebSocket endpoints (/ws/query, /ws/tutor, /ws/chat)
  - RAG background embedding after connection
  - Schema drift detection loop
  - CORS middleware
"""

from __future__ import annotations

import asyncio
import json
import logging
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

from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from app.models.auth import ConnectionModel, QueryRequest, ExecuteRequest
from app.models.query_plan import ValidatedQueryPlan
from app.database.connector import test_connection, get_engine, _conn_key
from app.security.secrets import gemini_api_key
from app.security.rbac import Role, check_permission
from app.tools.sql_parser import validate_sql_ast
from app.audit.audit_log import init_audit_db, log_audit_event, classify_severity
from app.security.rbac_middleware import RBACMiddleware

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
_ws_clients: List[WebSocket] = []  # drift notification subscribers

MAX_SESSION_HISTORY = 5

# ── Lifespan ──────────────────────────────────────────────────────────────


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("OptiVox DB backend starting up.")
    init_audit_db()
    
    # Pre-seed dialect docs
    from app.rag.dialect_seeder import seed_dialect_docs
    try:
        seed_dialect_docs()
    except Exception as e:
        logger.error(f"Failed to seed dialect docs: {e}")

    # Start schema drift detection background task
    asyncio.create_task(_drift_loop())
    yield
    logger.info("OptiVox DB backend shutting down.")
    _executor.shutdown(wait=False)


async def _drift_loop():
    from app.rag.drift_detector import drift_detection_loop

    async def notify_clients(conn_key: str, changed_tables: List[str]):
        msg = json.dumps({
            "type": "schema_drift",
            "connection": conn_key,
            "changed_tables": changed_tables,
        })
        for ws in _ws_clients:
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
    description="Natural language to SQL with autonomous CrewAI agents.",
    version="4.0.0",
    lifespan=lifespan,
)

app.add_middleware(RBACMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

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


# ── Health check ──────────────────────────────────────────────────────────


@app.get("/health", tags=["System"])
async def health():
    return {"status": "ok", "version": "4.0.0", "timestamp": datetime.now(timezone.utc).isoformat()}


# ── Database connect endpoint ─────────────────────────────────────────────


@app.post("/api/connect", tags=["Database"])
async def connect_database(conn: ConnectionModel, background_tasks: BackgroundTasks):
    """
    Test database connection and trigger async schema embedding.
    Returns detected dialect and connection status.
    """
    try:
        info = test_connection(conn.model_dump())
        conn_dict = conn.model_dump()

        # Register active connection for drift detection
        key = _conn_key(conn_dict)
        if not any(_conn_key(c) == key for c in _active_connections):
            _active_connections.append(conn_dict)

        # Background schema embedding
        background_tasks.add_task(_embed_schema_bg, conn_dict)

        return {"status": "connected", "info": info}
    except Exception as exc:
        logger.error("Connection failed: %s", exc)
        raise HTTPException(status_code=400, detail=f"Connection failed: {exc}")


async def _embed_schema_bg(conn: Dict[str, Any]):
    """Background task: extract schema DDLs and embed into LanceDB."""
    try:
        from app.database.schema_extractor import extract_schema
        from app.rag.embedder import embed_schema

        loop = asyncio.get_event_loop()
        engine = await loop.run_in_executor(_executor, lambda: get_engine(conn))
        tables = await loop.run_in_executor(
            _executor, lambda: extract_schema(engine, conn.get("dialect", "mysql"))
        )
        conn_key = _conn_key(conn)
        await loop.run_in_executor(_executor, lambda: embed_schema(tables, conn_key))
        logger.info("Schema embedded for %s (%d tables).", conn_key, len(tables))
    except Exception as exc:
        logger.error("Schema embedding failed: %s", exc)


# ── NL-to-SQL endpoint (Phase 1 — single LLM call) ───────────────────────


@app.post("/api/query", tags=["Query"])
async def generate_query(req: QueryRequest, request: Request):
    """
    Phase 1: Generate SQL from natural language using LiteLLM + Gemini.
    Phase 3 upgrades this to use the full CrewAI Crew.
    """
    client_ip = request.client.host if request.client else "unknown"
    if not _check_rate_limit(client_ip):
        raise HTTPException(status_code=429, detail="Rate limit exceeded.")

    # Pull session history
    session_id = req.session_id or "default"
    history = _session_histories[session_id][-MAX_SESSION_HISTORY:]

    # Retrieve schema context via RAG
    try:
        from app.rag.embedder import fetch_schema_context, fetch_query_history
        schema_context = fetch_schema_context(req.question)
        few_shot = fetch_query_history(req.question)
    except Exception:
        schema_context, few_shot = "", ""

    history_str = json.dumps(history) if history else "No previous history."
    few_shot_str = f"\n\nFew-shot examples from history:\n{few_shot}" if few_shot else ""

    prompt = f"""You are an expert SQL developer. Generate a single valid {req.dialect.upper()} SQL query.

User request: {req.question}

Relevant schema (from vector search):
{schema_context or "No schema context available — use your best judgment."}
{few_shot_str}

Conversation history: {history_str}

Rules:
- Return ONLY the SQL statement. No explanations, no markdown fences.
- Use only table and column names that appear in the schema context.
- Be dialect-aware: use {req.dialect.upper()} syntax.
"""

    try:
        api_key = gemini_api_key()
        response = await asyncio.get_event_loop().run_in_executor(
            _executor,
            lambda: litellm.completion(
                model="gemini/gemini-2.5-flash",
                messages=[{"role": "user", "content": prompt}],
                api_key=api_key,
            ),
        )
        sql = response.choices[0].message.content.strip()
        # Strip markdown fences if present
        sql = re.sub(r"```(?:sql)?\s*", "", sql, flags=re.IGNORECASE)
        sql = re.sub(r"```", "", sql).strip()

        # Safety check
        is_safe, reason = validate_sql_ast(sql, req.dialect)

        return {
            "sql": sql,
            "dialect": req.dialect,
            "safe": is_safe,
            "safety_reason": reason,
            "session_id": session_id,
        }
    except Exception as exc:
        logger.error("LLM query generation failed: %s", traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(exc))


# ── Agentic query endpoint (Phase 3 — CrewAI) ────────────────────────────


@app.post("/api/query/crew", tags=["Query"])
async def generate_query_crew(req: QueryRequest, request: Request):
    """
    Phase 3: Full CrewAI pipeline — Architect → Generator → Reviewer.
    Use this endpoint once CrewAI is fully configured.
    """
    client_ip = request.client.host if request.client else "unknown"
    if not _check_rate_limit(client_ip):
        raise HTTPException(status_code=429, detail="Rate limit exceeded.")

    session_id = req.session_id or "default"
    history = _session_histories[session_id][-MAX_SESSION_HISTORY:]
    history_str = json.dumps(history) if history else "None"

    try:
        from app.agents.hierarchical_crew import run_hierarchical_query
        loop = asyncio.get_event_loop()
        plan: ValidatedQueryPlan = await asyncio.wait_for(
            loop.run_in_executor(
                _executor,
                lambda: run_hierarchical_query(req.question, req.dialect, history_str),
            ),
            timeout=120.0,
        )

        _session_histories[session_id].append({
            "role": "user", "content": req.question, "sql": plan.sql,
        })

        return plan.model_dump()
    except asyncio.TimeoutError:
        raise HTTPException(status_code=408, detail="Crew pipeline timed out.")
    except Exception as exc:
        logger.error("CrewAI pipeline failed: %s", traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(exc))


# ── Execute endpoint ──────────────────────────────────────────────────────


@app.post("/api/execute", tags=["Query"])
async def execute_query(req: ExecuteRequest, request: Request):
    """
    Execute a validated SQL string against the connected database.
    Returns columns and rows as JSON.
    """
    client_ip = request.client.host if request.client else "unknown"
    if not _check_rate_limit(client_ip):
        raise HTTPException(status_code=429, detail="Rate limit exceeded.")

    # RBAC check (default to ADMIN when no auth header present — Phase 7 wires this fully)
    role = Role.ADMIN
    allowed, reason = check_permission(role, req.sql)
    if not allowed:
        logger.warning("Execute RBAC block: role=%s reason=%s", role.value, reason)
        raise HTTPException(status_code=403, detail="Access denied: your role does not permit this operation.")

    # Safety validation
    is_safe, safety_reason = validate_sql_ast(req.sql, req.connection.dialect)
    
    # Audit logging for DANGER queries
    severity = classify_severity(req.sql)
    if severity == "DANGER":
        log_audit_event(
            session_id=req.session_id or "default",
            role=role.value,
            severity=severity,
            sql_text=req.sql,
            dialect=req.connection.dialect,
            host=req.connection.host,
            database=req.connection.database,
            approved=is_safe
        )

    if not is_safe:
        raise HTTPException(status_code=400, detail=f"Unsafe SQL: {safety_reason}")

    try:
        from sqlalchemy import text as sa_text
        loop = asyncio.get_event_loop()

        def _run():
            engine = get_engine(req.connection.model_dump())
            start = time.time()
            with engine.connect() as conn:
                result = conn.execute(sa_text(req.sql))
                duration_ms = int((time.time() - start) * 1000)
                if result.returns_rows:
                    columns = list(result.keys())
                    rows = [list(row) for row in result.fetchall()]
                    return {"columns": columns, "rows": rows, "row_count": len(rows), "duration_ms": duration_ms}
                else:
                    conn.commit()
                    return {"columns": [], "rows": [], "rows_affected": result.rowcount, "duration_ms": duration_ms}

        data = await loop.run_in_executor(_executor, _run)

        # Embed successful query into history
        session_id = req.session_id or "default"
        try:
            from app.rag.embedder import embed_query_history
            await loop.run_in_executor(
                _executor,
                lambda: embed_query_history(req.sql, req.sql, session_id),
            )
        except Exception:
            pass

        return {"status": "success", **data}
    except Exception as exc:
        logger.error("Execution failed: %s", exc)
        raise HTTPException(status_code=400, detail="Query execution failed. Please check your SQL and try again.")


# ── Schema endpoint ───────────────────────────────────────────────────────


@app.post("/api/schema", tags=["Database"])
async def get_schema(conn: ConnectionModel):
    """Extract and return the full schema of the connected database."""
    try:
        from app.database.schema_extractor import extract_schema
        loop = asyncio.get_event_loop()
        engine = await loop.run_in_executor(_executor, lambda: get_engine(conn.model_dump()))
        tables = await loop.run_in_executor(
            _executor, lambda: extract_schema(engine, conn.dialect)
        )
        return {"status": "ok", "tables": tables, "table_count": len(tables)}
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


# ── ADIA: Natural Language SQL Generation ────────────────────────────────


class ADIANLRequest(BaseModel):
    """Request for NL→SQL generation."""
    question: str
    dialect: str = Field("mysql", pattern="^(mysql|postgres|oracle)$")
    connection: Optional[Any] = None  # ConnectionModel forwarded for context
    session_id: Optional[str] = None
    mode: str = Field("fast", pattern="^(fast|crew)$")  # fast=single LLM, crew=CrewAI


@app.post("/api/adia/nl-sql", tags=["ADIA"])
async def adia_nl_sql(req: ADIANLRequest, request: Request):
    """
    ADIA Section 1 — Natural Language SQL Generation.
    Converts a plain-English question into a validated SQL query.
    Mode: 'fast' uses a single LLM call; 'crew' runs the full CrewAI pipeline.
    """
    client_ip = request.client.host if request.client else "unknown"
    if not _check_rate_limit(client_ip):
        raise HTTPException(status_code=429, detail="Rate limit exceeded.")

    session_id = req.session_id or "default"
    history = _session_histories[session_id][-MAX_SESSION_HISTORY:]

    try:
        from app.rag.embedder import fetch_schema_context, fetch_query_history
        schema_context = fetch_schema_context(req.question)
        few_shot = fetch_query_history(req.question)
    except Exception:
        schema_context, few_shot = "", ""

    if req.mode == "crew":
        history_str = json.dumps(history) if history else "None"
        try:
            from app.agents.hierarchical_crew import run_hierarchical_query
            loop = asyncio.get_event_loop()
            plan: ValidatedQueryPlan = await asyncio.wait_for(
                loop.run_in_executor(
                    _executor,
                    lambda: run_hierarchical_query(req.question, req.dialect, history_str),
                ),
                timeout=120.0,
            )
            _session_histories[session_id].append({"role": "user", "content": req.question, "sql": plan.sql})
            return {
                "section": "nl_sql",
                "mode": "crew",
                "sql": plan.sql,
                "dialect": plan.dialect,
                "approved": plan.approved,
                "is_destructive": plan.is_destructive,
                "rejection_reason": plan.rejection_reason,
                "session_id": session_id,
            }
        except asyncio.TimeoutError:
            raise HTTPException(status_code=408, detail="Agent pipeline timed out.")
        except Exception as exc:
            logger.error("ADIA NL-SQL crew failed: %s", traceback.format_exc())
            raise HTTPException(status_code=500, detail="SQL generation failed. Please try again.")
    else:
        # Fast mode — single LLM call
        history_str = json.dumps(history) if history else "No previous history."
        few_shot_str = f"\n\nFew-shot examples:\n{few_shot}" if few_shot else ""
        prompt = f"""You are an expert SQL developer. Generate a single valid {req.dialect.upper()} SQL query.

User request: {req.question}
Relevant schema:
{schema_context or "No schema — use best judgment."}
{few_shot_str}
Conversation history: {history_str}

Rules: Return ONLY the SQL. No markdown. No explanation. Dialect: {req.dialect.upper()}."""
        try:
            api_key = gemini_api_key()
            response = await asyncio.get_event_loop().run_in_executor(
                _executor,
                lambda: litellm.completion(
                    model="gemini/gemini-2.5-flash",
                    messages=[{"role": "user", "content": prompt}],
                    api_key=api_key,
                ),
            )
            sql = response.choices[0].message.content.strip()
            sql = re.sub(r"```(?:sql)?\s*", "", sql, flags=re.IGNORECASE)
            sql = re.sub(r"```", "", sql).strip()
            is_safe, safety_reason = validate_sql_ast(sql, req.dialect)
            _session_histories[session_id].append({"role": "user", "content": req.question, "sql": sql})
            return {
                "section": "nl_sql",
                "mode": "fast",
                "sql": sql,
                "dialect": req.dialect,
                "safe": is_safe,
                "safety_reason": safety_reason,
                "session_id": session_id,
            }
        except Exception as exc:
            logger.error("ADIA NL-SQL fast failed: %s", traceback.format_exc())
            raise HTTPException(status_code=500, detail="SQL generation failed. Please try again.")


# ── ADIA: Teaching ────────────────────────────────────────────────────────


class ADIATeachRequest(BaseModel):
    question: str
    session_id: Optional[str] = None


@app.post("/api/adia/teach", tags=["ADIA"])
async def adia_teach(req: ADIATeachRequest, request: Request):
    """
    ADIA Section 2 — Teaching.
    Returns a structured SQL/database lesson with examples for the given topic.
    """
    client_ip = request.client.host if request.client else "unknown"
    if not _check_rate_limit(client_ip):
        raise HTTPException(status_code=429, detail="Rate limit exceeded.")

    session_id = req.session_id or "default"
    history = _session_histories[session_id][-MAX_SESSION_HISTORY:]

    messages = [
        {
            "role": "system",
            "content": (
                "You are an expert database educator. "
                "Respond with a concise, numbered lesson (max 5 points). "
                "Include at least one concrete SQL example per concept. "
                "Be precise and pedagogically progressive. No preamble."
            ),
        }
    ]
    for h in history:
        if h.get("role") and h.get("content"):
            messages.append({"role": h["role"], "content": h["content"]})
    messages.append({"role": "user", "content": req.question})

    try:
        api_key = gemini_api_key()
        response = await asyncio.get_event_loop().run_in_executor(
            _executor,
            lambda: litellm.completion(
                model="gemini/gemini-2.5-flash",
                messages=messages,
                api_key=api_key,
            ),
        )
        answer = response.choices[0].message.content.strip()
        _session_histories[session_id].append({"role": "assistant", "content": answer})
        return {"section": "teach", "answer": answer, "session_id": session_id}
    except Exception as exc:
        logger.error("ADIA teach failed: %s", traceback.format_exc())
        raise HTTPException(status_code=500, detail="Teaching response failed. Please try again.")


# ── ADIA: Query Optimization ─────────────────────────────────────────────


class ADIAOptimizeRequest(BaseModel):
    sql: str
    dialect: str = Field("mysql", pattern="^(mysql|postgres|oracle)$")
    explain_output: Optional[str] = None  # Optional EXPLAIN plan text
    session_id: Optional[str] = None


@app.post("/api/adia/optimize", tags=["ADIA"])
async def adia_optimize(req: ADIAOptimizeRequest, request: Request):
    """
    ADIA Section 3 — Query Optimization.
    Analyzes a SQL query (and optional EXPLAIN output) and returns
    structured performance tips and rewrite suggestions.
    """
    client_ip = request.client.host if request.client else "unknown"
    if not _check_rate_limit(client_ip):
        raise HTTPException(status_code=429, detail="Rate limit exceeded.")

    explain_ctx = f"\n\nEXPLAIN output:\n{req.explain_output}" if req.explain_output else ""
    prompt = f"""You are a database performance specialist. Analyze this {req.dialect.upper()} SQL query and return ONLY a JSON object with these exact keys:
- "issues": list of {{"type": str, "description": str}} — identified performance problems
- "rewritten_sql": str — optimized version of the query (or same if already optimal)
- "tips": list of str — concise actionable performance tips (max 5)
- "index_suggestions": list of {{"table": str, "column": str, "reason": str}}

SQL:\n{req.sql}{explain_ctx}

Return valid JSON only. No markdown. No explanation outside the JSON."""

    try:
        api_key = gemini_api_key()
        response = await asyncio.get_event_loop().run_in_executor(
            _executor,
            lambda: litellm.completion(
                model="gemini/gemini-2.5-flash",
                messages=[{"role": "user", "content": prompt}],
                api_key=api_key,
            ),
        )
        raw = response.choices[0].message.content.strip()
        # Strip markdown fences if present
        raw = re.sub(r"```(?:json)?\s*", "", raw, flags=re.IGNORECASE)
        raw = re.sub(r"```", "", raw).strip()
        try:
            analysis = json.loads(raw)
        except json.JSONDecodeError:
            analysis = {"raw_response": raw}
        return {"section": "optimize", "dialect": req.dialect, **analysis}
    except Exception as exc:
        logger.error("ADIA optimize failed: %s", traceback.format_exc())
        raise HTTPException(status_code=500, detail="Optimization analysis failed. Please try again.")


# ── ADIA: Schema Analysis & Optimization ─────────────────────────────────


class ADIASchemaAnalysisRequest(BaseModel):
    connection: Any  # ConnectionModel


@app.post("/api/adia/schema-analysis", tags=["ADIA"])
async def adia_schema_analysis(conn: ConnectionModel, request: Request):
    """
    ADIA Section 4 — Schema Analysis & Optimization.
    Returns actionable insights, performance tips, FK maps, missing indexes,
    isolated tables, and AI-generated recommendations.
    """
    client_ip = request.client.host if request.client else "unknown"
    if not _check_rate_limit(client_ip):
        raise HTTPException(status_code=429, detail="Rate limit exceeded.")

    try:
        from app.database.schema_extractor import extract_schema
        from app.api.schema_analysis import analyze_schema

        loop = asyncio.get_event_loop()
        engine = await loop.run_in_executor(_executor, lambda: get_engine(conn.model_dump()))
        tables = await loop.run_in_executor(
            _executor, lambda: extract_schema(engine, conn.dialect)
        )
        analysis = await loop.run_in_executor(
            _executor, lambda: analyze_schema(tables, engine)
        )

        # AI-generated recommendations
        issues_summary = ""
        if analysis.get("missing_index_suggestions"):
            issues_summary = "Missing indexes: " + "; ".join(
                f"{s['table']}.{s['column']}" for s in analysis["missing_index_suggestions"][:5]
            )
        if analysis.get("isolated_tables"):
            issues_summary += " | Isolated tables: " + ", ".join(analysis["isolated_tables"][:5])

        recommendations: List[str] = []
        if issues_summary:
            try:
                api_key = gemini_api_key()
                rec_prompt = (
                    f"Database schema has these issues: {issues_summary}\n"
                    "List 3 concise, actionable DBA recommendations. Return as a JSON array of strings only."
                )
                rec_resp = await loop.run_in_executor(
                    _executor,
                    lambda: litellm.completion(
                        model="gemini/gemini-2.5-flash",
                        messages=[{"role": "user", "content": rec_prompt}],
                        api_key=api_key,
                    ),
                )
                raw_rec = rec_resp.choices[0].message.content.strip()
                raw_rec = re.sub(r"```(?:json)?\s*", "", raw_rec, flags=re.IGNORECASE)
                raw_rec = re.sub(r"```", "", raw_rec).strip()
                recommendations = json.loads(raw_rec)
            except Exception:
                recommendations = []

        return {
            "section": "schema_analysis",
            "status": "ok",
            **analysis,
            "ai_recommendations": recommendations,
            "performance_tips": [
                "Add indexes on all foreign key columns to avoid full table scans on JOINs.",
                "Every table should have a primary key for efficient index lookups.",
                "Isolated tables with no relationships may indicate orphaned data — review.",
                "Consider partitioning large tables (>10M rows) on frequently filtered columns.",
            ],
        }
    except Exception as exc:
        logger.error("ADIA schema-analysis failed: %s", exc)
        raise HTTPException(status_code=400, detail="Schema analysis failed. Verify your connection and try again.")


# ── WebSocket — query streaming ───────────────────────────────────────────


@app.websocket("/ws/query")
async def ws_query(websocket: WebSocket):
    """
    WebSocket endpoint for live agent status streaming.
    Accepts QueryRequest JSON, streams status messages, returns final ValidatedQueryPlan.
    """
    await websocket.accept()
    _ws_clients.append(websocket)
    try:
        while True:
            data = await websocket.receive_text()
            payload = json.loads(data)
            req = QueryRequest(**payload)
            session_id = req.session_id or "default"
            history = _session_histories[session_id][-MAX_SESSION_HISTORY:]
            history_str = json.dumps(history) if history else "None"

            async def send(msg: str, status: str = "progress"):
                await websocket.send_text(json.dumps({"status": status, "message": msg}))

            await send("Architect is analyzing your request…", "architect")

            def _step_cb(step):
                pass  # reserved for future per-step streaming

            try:
                from app.agents.hierarchical_crew import run_hierarchical_query
                loop = asyncio.get_event_loop()

                await send("Generator is writing SQL…", "generator")
                plan: ValidatedQueryPlan = await asyncio.wait_for(
                    loop.run_in_executor(
                        _executor,
                        lambda: run_hierarchical_query(req.question, req.dialect, history_str),
                    ),
                    timeout=120.0,
                )
                await send("Reviewer is checking safety…", "reviewer")
                await asyncio.sleep(0.1)

                _session_histories[session_id].append({
                    "role": "user", "content": req.question, "sql": plan.sql,
                })

                await websocket.send_text(json.dumps({
                    "status": "done",
                    "plan": plan.model_dump(),
                }))
            except asyncio.TimeoutError:
                await websocket.send_text(json.dumps({"status": "error", "message": "Timed out."}))
            except Exception as exc:
                await websocket.send_text(json.dumps({"status": "error", "message": str(exc)}))

    except WebSocketDisconnect:
        _ws_clients.remove(websocket)


# ── WebSocket — tutor streaming ───────────────────────────────────────────


@app.websocket("/ws/tutor")
async def ws_tutor(websocket: WebSocket):
    """Stream Tutor agent responses token-by-token."""
    await websocket.accept()
    try:
        while True:
            data = await websocket.receive_text()
            payload = json.loads(data)
            question = payload.get("question", "")
            session_id = payload.get("session_id", "default")

            api_key = gemini_api_key()

            async def stream_tutor():
                history = _session_histories[session_id][-MAX_SESSION_HISTORY:]
                messages = [
                    {
                        "role": "system",
                        "content": (
                            "You are an expert database educator. "
                            "Explain concepts in clear, numbered micro-lessons with SQL examples. "
                            "Be concise and pedagogically progressive."
                        ),
                    }
                ]
                for h in history:
                    messages.append({"role": "user", "content": h.get("content", "")})
                messages.append({"role": "user", "content": question})

                import litellm
                response = litellm.completion(
                    model="gemini/gemini-2.5-flash",
                    messages=messages,
                    api_key=api_key,
                    stream=True,
                )
                full = ""
                for chunk in response:
                    delta = chunk.choices[0].delta.content or ""
                    full += delta
                    await websocket.send_text(json.dumps({"status": "token", "token": delta}))
                await websocket.send_text(json.dumps({"status": "done", "full": full}))
                _session_histories[session_id].append({"role": "assistant", "content": full})

            loop = asyncio.get_event_loop()
            await loop.run_in_executor(_executor, lambda: asyncio.run(stream_tutor()))

    except WebSocketDisconnect:
        pass


# ── WebSocket — unified chat ──────────────────────────────────────────────


@app.websocket("/ws/chat")
async def ws_chat(websocket: WebSocket):
    """
    Unified chat WebSocket. Router classifies request and delegates to
    the correct agent (Query Crew, Tutor, or Optimizer).
    """
    await websocket.accept()
    _ws_clients.append(websocket)
    try:
        while True:
            data = await websocket.receive_text()
            payload = json.loads(data)
            question = payload.get("question", "")
            dialect = payload.get("dialect", "mysql")
            session_id = payload.get("session_id", "default")

            await websocket.send_text(json.dumps({"status": "routing", "message": "Classifying request…"}))

            # Lightweight classification via LiteLLM
            api_key = gemini_api_key()
            loop = asyncio.get_event_loop()

            def _classify():
                resp = litellm.completion(
                    model="gemini/gemini-2.5-flash",
                    messages=[
                        {
                            "role": "system",
                            "content": (
                                "Classify the user's request as QUERY, TUTOR, or ANALYZE. "
                                "Return ONLY one word."
                            ),
                        },
                        {"role": "user", "content": question},
                    ],
                    api_key=api_key,
                )
                return resp.choices[0].message.content.strip().upper()

            intent = await loop.run_in_executor(_executor, _classify)
            await websocket.send_text(json.dumps({"status": "classified", "intent": intent}))

            if intent == "TUTOR":
                def _tutor():
                    resp = litellm.completion(
                        model="gemini/gemini-2.5-flash",
                        messages=[
                            {"role": "system", "content": "You are an expert database educator. Give step-by-step SQL lessons."},
                            {"role": "user", "content": question},
                        ],
                        api_key=api_key,
                    )
                    return resp.choices[0].message.content.strip()
                answer = await loop.run_in_executor(_executor, _tutor)
                await websocket.send_text(json.dumps({"status": "done", "agent": "tutor", "answer": answer}))

            elif intent == "ANALYZE":
                await websocket.send_text(json.dumps({
                    "status": "done",
                    "agent": "optimizer",
                    "answer": "Connect to a database and run a query first to enable optimizer analysis.",
                }))

            else:  # QUERY
                try:
                    from app.agents.hierarchical_crew import run_hierarchical_query
                    history = _session_histories[session_id][-MAX_SESSION_HISTORY:]
                    history_str = json.dumps(history) if history else "None"
                    await websocket.send_text(json.dumps({"status": "architect", "message": "Architect is planning…"}))
                    plan: ValidatedQueryPlan = await asyncio.wait_for(
                        loop.run_in_executor(
                            _executor,
                            lambda: run_hierarchical_query(question, dialect, history_str),
                        ),
                        timeout=120.0,
                    )
                    await websocket.send_text(json.dumps({
                        "status": "done",
                        "agent": "query_crew",
                        "plan": plan.model_dump(),
                    }))
                except Exception as exc:
                    await websocket.send_text(json.dumps({"status": "error", "message": str(exc)}))

    except WebSocketDisconnect:
        if websocket in _ws_clients:
            _ws_clients.remove(websocket)


# ── Entry point ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)
