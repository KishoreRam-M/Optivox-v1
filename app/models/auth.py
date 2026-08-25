"""models/auth.py — Auth & connection Pydantic models."""

from __future__ import annotations

from typing import Optional
from pydantic import BaseModel, Field, field_validator


class ConnectionModel(BaseModel):
    """Database connection credentials sent by the client."""
    host: str
    port: int = 3306
    user: str
    password: str
    database: str
    dialect: str = Field("mysql", pattern="^(mysql|postgres|oracle|mssql)$")

    @field_validator("host", mode="before")
    @classmethod
    def _strip_host(cls, v: str) -> str:
        v = str(v).strip()
        if not v:
            raise ValueError("host must not be empty")
        return v


class QueryRequest(BaseModel):
    """Single natural-language query request."""
    question: str
    dialect: str = Field("mysql", pattern="^(mysql|postgres|oracle|mssql)$")
    connection: ConnectionModel
    session_id: Optional[str] = None


class ExecuteRequest(BaseModel):
    """Execute a pre-validated SQL string."""
    sql: str
    connection: ConnectionModel
    session_id: Optional[str] = None
