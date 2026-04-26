"""models/auth.py — Auth & connection Pydantic models."""

from __future__ import annotations

from typing import Optional
from pydantic import BaseModel, Field


class ConnectionModel(BaseModel):
    """Database connection credentials sent by the client."""
    host: str
    port: int = 3306
    user: str
    password: str
    database: str
    dialect: str = Field("mysql", pattern="^(mysql|postgres|oracle)$")


class QueryRequest(BaseModel):
    """Single natural-language query request."""
    question: str
    dialect: str = Field("mysql", pattern="^(mysql|postgres|oracle)$")
    connection: ConnectionModel
    session_id: Optional[str] = None


class ExecuteRequest(BaseModel):
    """Execute a pre-validated SQL string."""
    sql: str
    connection: ConnectionModel
    session_id: Optional[str] = None
