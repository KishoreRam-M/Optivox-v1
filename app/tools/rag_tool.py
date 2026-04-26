"""
tools/rag_tool.py
-----------------
CrewAI-compatible RAG retrieval tool for schema context and query history.
"""

from __future__ import annotations

from crewai.tools import BaseTool
from pydantic import BaseModel, Field


class SchemaQueryInput(BaseModel):
    question: str = Field(description="The natural language question to retrieve schema context for.")


class RAGSchemaTool(BaseTool):
    name: str = "fetch_schema_context"
    description: str = (
        "Retrieves the most relevant database table DDLs for a given natural language question. "
        "Always call this before generating SQL to ensure you reference real column names."
    )
    args_schema: type[BaseModel] = SchemaQueryInput

    def _run(self, question: str) -> str:
        from app.rag.embedder import fetch_schema_context
        context = fetch_schema_context(question, top_k=3)
        return context if context else "No schema context available. Use the schema passed in the task description."


class RAGHistoryTool(BaseTool):
    name: str = "fetch_query_history"
    description: str = (
        "Retrieves similar past question/SQL pairs as few-shot examples. "
        "Use these to guide your SQL generation style and correctness."
    )
    args_schema: type[BaseModel] = SchemaQueryInput

    def _run(self, question: str) -> str:
        from app.rag.embedder import fetch_query_history
        history = fetch_query_history(question, top_k=2)
        return history if history else "No query history available."
