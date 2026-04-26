"""
agents/reviewer.py
------------------
The Safety Reviewer agent validates the generated SQL for security and
correctness. Returns APPROVED or REJECTED with a detailed reason.
On rejection, the reason is fed back to the Generator as a correction task.
"""

from __future__ import annotations

from crewai import Agent
from app.tools.sql_parser import SQLValidatorTool


def build_reviewer() -> Agent:
    return Agent(
        role="SQL Security Auditor",
        goal=(
            "Review the generated SQL for structural correctness, security vulnerabilities, "
            "and dangerous patterns. "
            "Return APPROVED if the SQL is safe, or REJECTED with a precise reason if it is not."
        ),
        backstory=(
            "You are a strict database security auditor who has prevented countless data breaches "
            "and data loss incidents. You trust nothing — every query is guilty until proven safe. "
            "You use AST analysis tools to make your decisions, never gut feel."
        ),
        llm="gemini/gemini-2.5-flash",
        tools=[SQLValidatorTool()],
        verbose=True,
        allow_delegation=False,
    )
