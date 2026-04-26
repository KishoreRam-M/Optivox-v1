"""
models/query_plan.py
--------------------
Typed data contracts passed between CrewAI agents.
All agent output_pydantic fields must reference these models.
"""

from __future__ import annotations

from typing import List, Optional
from pydantic import BaseModel, Field


class ExecutionPlan(BaseModel):
    """Produced by the Architect agent."""
    steps: List[str] = Field(description="Ordered list of natural-language steps to execute.")
    tables_involved: List[str] = Field(description="Table names the query will touch.")
    requires_transaction: bool = Field(
        default=False,
        description="True when multiple write steps must be atomic.",
    )
    risk_level: str = Field(
        default="low",
        description="low | medium | high | critical",
    )


class ValidatedQueryPlan(BaseModel):
    """Produced by the Generator and approved by the Reviewer."""
    sql: str = Field(description="The final SQL statement ready for execution.")
    dialect: str = Field(description="SQL dialect: mysql | postgres | oracle")
    is_destructive: bool = Field(
        default=False,
        description="True for DROP, TRUNCATE, or DELETE without WHERE.",
    )
    estimated_rows_affected: Optional[int] = Field(
        default=None,
        description="Best-effort estimate; None when unknown.",
    )
    approved: bool = Field(default=True)
    rejection_reason: Optional[str] = Field(
        default=None,
        description="Populated by Reviewer when approved=False.",
    )
