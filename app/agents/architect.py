"""
agents/architect.py
-------------------
The Architect agent analyzes the user's natural language request,
consults the RAG schema context, and produces an ExecutionPlan.
"""

from __future__ import annotations

from crewai import Agent
from app.tools.rag_tool import RAGSchemaTool


def build_architect() -> Agent:
    return Agent(
        role="Senior Database Architect",
        goal=(
            "Analyze the user's natural language request and the available database schema. "
            "Produce a precise, ordered execution plan including which tables are involved, "
            "what the risk level is, and whether a transaction is required."
        ),
        backstory=(
            "You are a veteran database architect with 20+ years designing high-throughput, "
            "mission-critical database systems. You always consult the live schema before planning "
            "and you never make assumptions about table or column names."
        ),
        llm="gemini/gemini-2.5-flash",
        tools=[RAGSchemaTool()],
        verbose=True,
        allow_delegation=False,
    )
