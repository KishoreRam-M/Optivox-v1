"""
agents/generator.py
-------------------
The SQL Generator agent converts the Architect's ExecutionPlan
into a validated SQL statement using dialect-aware generation.
"""

from __future__ import annotations

from crewai import Agent
from app.tools.rag_tool import RAGSchemaTool, RAGHistoryTool


def build_generator() -> Agent:
    return Agent(
        role="Senior SQL Developer",
        goal=(
            "Write a single, precise, dialect-correct SQL statement that fulfills the Architect's execution plan. "
            "Use the schema context tool to verify all table and column names before writing SQL. "
            "Use query history for few-shot examples."
        ),
        backstory=(
            "You are a master SQL craftsman with deep expertise in MySQL, PostgreSQL, and Oracle. "
            "You write clean, optimized, injection-free SQL and you never invent column names that aren't in the schema."
        ),
        llm="gemini/gemini-2.5-flash",
        tools=[RAGSchemaTool(), RAGHistoryTool()],
        verbose=True,
        allow_delegation=False,
    )
