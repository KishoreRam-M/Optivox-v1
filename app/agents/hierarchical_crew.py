"""
agents/hierarchical_crew.py
----------------------------
Phase 7 — Hierarchical crew with Router as the manager agent.

Architecture:
  Router (manager, Gemini 2.5 Pro)
    ├── Query Sub-Crew  (Architect → Generator → Reviewer)
    ├── Tutor Sub-Crew  (Tutor)
    └── Analyst Sub-Crew (Optimizer)

The Router delegates to the correct sub-crew based on intent classification.
"""

from __future__ import annotations

import logging
import re
from typing import Optional

from crewai import Crew, Task, Process, Agent

from app.agents.architect import build_architect
from app.agents.generator import build_generator
from app.agents.reviewer import build_reviewer
from app.agents.tutor import build_tutor
from app.agents.optimizer import build_optimizer
from app.models.query_plan import ValidatedQueryPlan

logger = logging.getLogger(__name__)

MAX_SELF_HEAL_ATTEMPTS = 3  # Phase 7: 3 attempts (up from 2 in Phase 4)


def _extract_sql(raw: str) -> str:
    m = re.search(r"```(?:sql)?\s*(.*?)```", raw, re.DOTALL | re.IGNORECASE)
    if m:
        return m.group(1).strip()
    m = re.search(r"(SELECT|INSERT|UPDATE|DELETE|WITH)\b.*", raw, re.DOTALL | re.IGNORECASE)
    if m:
        return m.group(0).strip()
    return raw.strip()


def build_manager_agent() -> Agent:
    """Router as the hierarchical manager with Gemini 2.5 Pro for stronger reasoning."""
    return Agent(
        role="Database Request Manager",
        goal=(
            "Classify the user's request and delegate to the correct sub-crew: "
            "QUERY for SQL generation, TUTOR for teaching, ANALYZE for optimization. "
            "Synthesize the sub-crew's output into a final, polished response."
        ),
        backstory=(
            "You are the senior orchestrator of the OptiVox AI platform. "
            "You have deep understanding of database workflows and always route requests "
            "to the most appropriate specialist. You synthesize results clearly."
        ),
        llm="gemini/gemini-2.5-flash",  # use flash as pro may not always be available
        verbose=True,
        allow_delegation=True,
        memory=True,
    )


def run_hierarchical_query(
    question: str,
    dialect: str,
    session_history: str = "",
    step_callback=None,
) -> ValidatedQueryPlan:
    """
    Run the full hierarchical CrewAI pipeline.
    
    Phase 7 self-healing:
    - Up to 3 correction attempts on Reviewer rejection
    - On all 3 failures, Architect re-plans with alternative strategy
    - Returns ValidatedQueryPlan with approved/rejection_reason
    """
    architect = build_architect()
    generator = build_generator()
    reviewer = build_reviewer()

    plan_task = Task(
        description=f"""Analyze the user's request and produce an execution plan.

User request: {question}
SQL dialect: {dialect}
Session history: {session_history or "None"}

Use the fetch_schema_context tool to retrieve relevant table DDLs before planning.
Output a structured plan with: steps (list), tables_involved (list), requires_transaction (bool), risk_level (str).

If this is a re-plan (after previous failures), use a COMPLETELY DIFFERENT approach:
- Try a different join strategy or subquery structure
- Simplify the query if complexity caused failures
- Consider breaking into multiple simpler queries""",
        expected_output="A JSON-compatible execution plan.",
        agent=architect,
    )

    gen_task = Task(
        description=f"""Based on the Architect's plan, generate the SQL query.

Dialect: {dialect}
User request: {question}

Use fetch_schema_context to verify column/table names.
Use fetch_query_history for few-shot examples.
Return ONLY the final SQL — no explanation, no markdown.""",
        expected_output="A single valid SQL statement, no markdown fences.",
        agent=generator,
        context=[plan_task],
    )

    review_task = Task(
        description="""Review the SQL. Use validate_sql_ast.
Return exactly 'APPROVED' or 'REJECTED: <reason>'.""",
        expected_output="APPROVED or REJECTED: <reason>",
        agent=reviewer,
        context=[gen_task],
    )

    crew = Crew(
        agents=[architect, generator, reviewer],
        tasks=[plan_task, gen_task, review_task],
        process=Process.sequential,
        verbose=True,
        memory=True,  # Phase 7: enable CrewAI memory
        step_callback=step_callback,
    )

    result = crew.kickoff()
    review_output = str(result.raw).strip()

    # ── Self-healing correction loop (Phase 7: max 3 attempts) ──────────
    correction_attempt = 0
    while review_output.upper().startswith("REJECTED") and correction_attempt < MAX_SELF_HEAL_ATTEMPTS:
        rejection_reason = (
            review_output[len("REJECTED:"):].strip()
            if ":" in review_output
            else review_output
        )
        logger.warning(
            "Reviewer rejected SQL (attempt %d/%d): %s",
            correction_attempt + 1, MAX_SELF_HEAL_ATTEMPTS, rejection_reason,
        )
        correction_attempt += 1

        if correction_attempt == MAX_SELF_HEAL_ATTEMPTS:
            # Final attempt: Architect re-plans with completely different strategy
            logger.warning("All correction attempts exhausted — Architect re-planning.")
            replan_task = Task(
                description=f"""EMERGENCY RE-PLAN: All previous SQL attempts were rejected.
                
Rejection reason: {rejection_reason}
Original request: {question}
Dialect: {dialect}

Create a COMPLETELY DIFFERENT execution plan using:
- Alternative tables or query approach
- Simpler SQL structure
- Different JOIN strategy
- Subquery vs CTE trade-off

The previous approach failed. Think creatively.""",
                expected_output="An alternative execution plan.",
                agent=architect,
            )
            replan_gen = Task(
                description=f"""Generate NEW SQL based on the alternative plan.
Dialect: {dialect}
Rejection from previous attempt: {rejection_reason}
Return ONLY the SQL.""",
                expected_output="SQL statement only.",
                agent=generator,
                context=[replan_task],
            )
            replan_review = Task(
                description="Review the regenerated SQL. Return APPROVED or REJECTED: <reason>.",
                expected_output="APPROVED or REJECTED: <reason>",
                agent=reviewer,
                context=[replan_gen],
            )
            replan_crew = Crew(
                agents=[architect, generator, reviewer],
                tasks=[replan_task, replan_gen, replan_review],
                process=Process.sequential,
                verbose=True,
                memory=True,
                step_callback=step_callback,
            )
            result = replan_crew.kickoff()
            review_output = str(result.raw).strip()
            gen_task = replan_gen
            break

        else:
            # Standard correction
            correction_gen = Task(
                description=f"""SQL was rejected by the Safety Reviewer.
Rejection reason: {rejection_reason}
Original request: {question}
Dialect: {dialect}

Fix the SQL. Return ONLY the corrected SQL.""",
                expected_output="Corrected SQL, no markdown.",
                agent=generator,
            )
            correction_review = Task(
                description="Review corrected SQL. Return APPROVED or REJECTED: <reason>.",
                expected_output="APPROVED or REJECTED: <reason>",
                agent=reviewer,
                context=[correction_gen],
            )
            correction_crew = Crew(
                agents=[generator, reviewer],
                tasks=[correction_gen, correction_review],
                process=Process.sequential,
                verbose=True,
                memory=True,
                step_callback=step_callback,
            )
            result = correction_crew.kickoff()
            review_output = str(result.raw).strip()
            gen_task = correction_gen

    approved = not review_output.upper().startswith("REJECTED")
    rejection_reason_final: Optional[str] = None
    if not approved:
        rejection_reason_final = (
            review_output[len("REJECTED:"):].strip()
            if ":" in review_output
            else review_output
        )

    gen_result = gen_task.output.raw if gen_task.output else ""
    sql = _extract_sql(gen_result)

    return ValidatedQueryPlan(
        sql=sql,
        dialect=dialect,
        is_destructive=any(kw in sql.upper() for kw in ("DROP", "TRUNCATE", "DELETE")),
        approved=approved,
        rejection_reason=rejection_reason_final,
    )
