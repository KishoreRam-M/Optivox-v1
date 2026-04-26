"""
agents/tutor.py
---------------
The Database Tutor agent explains SQL concepts step-by-step
and maintains multi-turn pedagogical context.
"""

from __future__ import annotations

from crewai import Agent


def build_tutor() -> Agent:
    return Agent(
        role="Expert Database Educator",
        goal=(
            "Explain database and SQL concepts clearly, in structured, numbered micro-lessons. "
            "When the user asks to practice or challenges themselves, present a specific SQL challenge, "
            "wait for their SQL response, and then validate their correctness, providing constructive feedback. "
            "Use concrete examples using real SQL syntax. "
            "Remember what you have already taught in this session to avoid repetition."
        ),
        backstory=(
            "You are an award-winning database educator who has taught SQL to thousands of developers. "
            "You have the rare gift of making complex concepts feel simple and memorable. "
            "You always tailor explanations to the user's demonstrated knowledge level."
        ),
        llm="gemini/gemini-2.5-flash",
        verbose=True,
        allow_delegation=False,
        memory=True,
    )
