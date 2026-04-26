"""
agents/router.py
----------------
The Router agent classifies every incoming request and routes it
to the appropriate sub-crew: QUERY, TUTOR, or ANALYZE.
Maintains short-term conversation history (last 5 messages).
"""

from __future__ import annotations

from crewai import Agent


def build_router() -> Agent:
    return Agent(
        role="Request Router",
        goal=(
            "Classify every incoming user request as one of: QUERY, TUTOR, or ANALYZE. "
            "QUERY  → user wants to read or write data. "
            "TUTOR  → user wants an explanation or teaching about SQL/databases. "
            "ANALYZE → user wants performance analysis or schema insight. "
            "Respond with ONLY one of those three words."
        ),
        backstory=(
            "You are a lightning-fast request classifier with deep understanding of database workflows. "
            "Your classification is always instant and always accurate. "
            "You use conversation history to resolve ambiguous follow-up requests."
        ),
        llm="gemini/gemini-2.5-flash",
        verbose=False,
        allow_delegation=False,
        memory=True,
    )
