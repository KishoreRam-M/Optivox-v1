"""
app/api/playground.py
---------------------
SQL Playground — Sandboxed learning environment.

Features:
  - In-memory SQLite databases seeded with curriculum datasets
  - POST /api/playground/run      — execute SQL in the sandbox
  - GET  /api/playground/tasks    — return all challenge tasks
  - POST /api/playground/check    — grade a user's SQL answer
  - POST /api/playground/hint     — AI-powered contextual hint
  - GET  /api/playground/schema   — return schema for the active dataset
"""

from __future__ import annotations

import asyncio
import json
import re
import sqlite3
import textwrap
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

router = APIRouter(prefix="/api/playground", tags=["Playground"])

_executor = ThreadPoolExecutor(max_workers=4)

# ── Curriculum datasets ────────────────────────────────────────────────────

CURRICULUM_DDL = """
-- Employees dataset
CREATE TABLE IF NOT EXISTS employees (
    id          INTEGER PRIMARY KEY,
    name        TEXT    NOT NULL,
    department  TEXT    NOT NULL,
    salary      REAL    NOT NULL,
    hire_date   TEXT    NOT NULL,
    manager_id  INTEGER REFERENCES employees(id)
);

CREATE TABLE IF NOT EXISTS departments (
    id     INTEGER PRIMARY KEY,
    name   TEXT    NOT NULL,
    budget REAL    NOT NULL
);

CREATE TABLE IF NOT EXISTS projects (
    id          INTEGER PRIMARY KEY,
    title       TEXT    NOT NULL,
    department  TEXT    NOT NULL,
    budget      REAL    NOT NULL,
    status      TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS project_assignments (
    employee_id INTEGER REFERENCES employees(id),
    project_id  INTEGER REFERENCES projects(id),
    role        TEXT    NOT NULL,
    hours       INTEGER NOT NULL,
    PRIMARY KEY (employee_id, project_id)
);

CREATE TABLE IF NOT EXISTS sales (
    id          INTEGER PRIMARY KEY,
    employee_id INTEGER REFERENCES employees(id),
    amount      REAL    NOT NULL,
    sale_date   TEXT    NOT NULL,
    product     TEXT    NOT NULL
);
"""

CURRICULUM_SEED = """
INSERT OR IGNORE INTO departments VALUES
  (1,'Engineering',500000),
  (2,'Marketing',200000),
  (3,'HR',150000),
  (4,'Finance',300000);

INSERT OR IGNORE INTO employees VALUES
  (1,'Alice Chen',   'Engineering', 95000, '2019-03-15', NULL),
  (2,'Bob Smith',    'Engineering', 82000, '2020-07-01', 1),
  (3,'Carol Davis',  'Marketing',   72000, '2021-01-20', NULL),
  (4,'Dan Lee',      'Marketing',   65000, '2022-05-10', 3),
  (5,'Eva Martinez', 'HR',          60000, '2020-11-01', NULL),
  (6,'Frank Wilson', 'Finance',     88000, '2018-09-05', NULL),
  (7,'Grace Kim',    'Engineering', 91000, '2019-06-22', 1),
  (8,'Henry Brown',  'Finance',     79000, '2021-04-14', 6),
  (9,'Iris Yang',    'Marketing',   70000, '2020-03-30', 3),
  (10,'James Park',  'HR',          58000, '2023-01-02', 5);

INSERT OR IGNORE INTO projects VALUES
  (1,'Platform Rebuild',  'Engineering', 200000,'active'),
  (2,'Brand Campaign',    'Marketing',   80000, 'active'),
  (3,'HR Portal',         'HR',          40000, 'completed'),
  (4,'Data Pipeline',     'Engineering', 150000,'active'),
  (5,'Finance Dashboard', 'Finance',     60000, 'planning');

INSERT OR IGNORE INTO project_assignments VALUES
  (1,1,'Lead',   120),
  (2,1,'Dev',    200),
  (7,1,'Dev',    180),
  (3,2,'Lead',    90),
  (4,2,'Support', 60),
  (9,2,'Support', 40),
  (5,3,'Lead',   100),
  (10,3,'Dev',    80),
  (2,4,'Dev',    160),
  (7,4,'Lead',   140),
  (6,5,'Lead',    50),
  (8,5,'Dev',     70);

INSERT OR IGNORE INTO sales VALUES
  (1, 2, 12500, '2024-01-15', 'SaaS Pro'),
  (2, 2, 8400,  '2024-02-03', 'SaaS Basic'),
  (3, 4, 21000, '2024-01-28', 'Enterprise'),
  (4, 4, 9800,  '2024-03-10', 'SaaS Pro'),
  (5, 9, 5600,  '2024-02-14', 'SaaS Basic'),
  (6, 9, 13400, '2024-03-25', 'SaaS Pro'),
  (7, 3, 19000, '2024-01-05', 'Enterprise'),
  (8, 2, 7200,  '2024-03-18', 'SaaS Basic'),
  (9, 4, 15000, '2024-02-22', 'Enterprise'),
  (10,3, 11000, '2024-03-30', 'SaaS Pro');
"""


def _get_sandbox_connection() -> sqlite3.Connection:
    """Create a fresh in-memory SQLite DB seeded with curriculum data."""
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.executescript(CURRICULUM_DDL + CURRICULUM_SEED)
    conn.commit()
    return conn


SCHEMA_INFO = {
    "employees": {
        "columns": ["id", "name", "department", "salary", "hire_date", "manager_id"],
        "description": "Company employees with salary and department info",
    },
    "departments": {
        "columns": ["id", "name", "budget"],
        "description": "Department budget records",
    },
    "projects": {
        "columns": ["id", "title", "department", "budget", "status"],
        "description": "Active and completed company projects",
    },
    "project_assignments": {
        "columns": ["employee_id", "project_id", "role", "hours"],
        "description": "Which employees are assigned to which projects",
    },
    "sales": {
        "columns": ["id", "employee_id", "amount", "sale_date", "product"],
        "description": "Sales records with amounts and products",
    },
}

# ── Task curriculum ────────────────────────────────────────────────────────

TASKS: List[Dict[str, Any]] = [
    # ── Level 1: SELECT Basics ─────────────────────────────────────────────
    {
        "id": "t01", "level": 1, "xp": 50, "category": "SELECT Basics",
        "title": "Meet the Employees",
        "description": "Retrieve **all columns** from the `employees` table.",
        "hint": "Use `SELECT *` to select every column.",
        "expected_shape": {"min_rows": 10, "columns_include": ["id", "name", "department", "salary"]},
        "solution": "SELECT * FROM employees;",
        "validation_type": "shape",
    },
    {
        "id": "t02", "level": 1, "xp": 60, "category": "SELECT Basics",
        "title": "Name & Salary Only",
        "description": "Select only the `name` and `salary` columns from `employees`.",
        "hint": "List specific column names separated by commas after SELECT.",
        "expected_shape": {"exact_rows": 10, "exact_columns": ["name", "salary"]},
        "solution": "SELECT name, salary FROM employees;",
        "validation_type": "shape",
    },
    {
        "id": "t03", "level": 1, "xp": 70, "category": "SELECT Basics",
        "title": "Monthly Pay",
        "description": "The `salary` column is yearly. Select `name` and calculate their monthly salary by dividing `salary` by 12. Alias the calculation as `monthly_salary`.",
        "hint": "Use `salary / 12 AS monthly_salary`.",
        "expected_shape": {"exact_rows": 10, "columns_include": ["name", "monthly_salary"]},
        "solution": "SELECT name, salary / 12 AS monthly_salary FROM employees;",
        "validation_type": "shape",
    },
    {
        "id": "t04", "level": 1, "xp": 70, "category": "SELECT Basics",
        "title": "Unique Departments",
        "description": "List all the unique departments found in the `employees` table. Return just the `department` column.",
        "hint": "Use the `DISTINCT` keyword right after SELECT.",
        "expected_shape": {"exact_rows": 4, "exact_columns": ["department"]},
        "solution": "SELECT DISTINCT department FROM employees;",
        "validation_type": "shape",
    },

    # ── Level 2: Filtering & Logic ─────────────────────────────────────────
    {
        "id": "t05", "level": 2, "xp": 80, "category": "Filtering & Logic",
        "title": "High Earners",
        "description": "Find all employees with a `salary` greater than **80,000**. Select all columns.",
        "hint": "Use a `WHERE` clause to filter rows: `WHERE salary > 80000`.",
        "expected_shape": {"exact_rows": 4, "columns_include": ["name", "salary"]},
        "solution": "SELECT * FROM employees WHERE salary > 80000;",
        "validation_type": "shape",
    },
    {
        "id": "t06", "level": 2, "xp": 90, "category": "Filtering & Logic",
        "title": "Engineering Seniors",
        "description": "List employees who are in the `Engineering` department **AND** earn more than **90,000**. Show their `name` and `salary`.",
        "hint": "Combine conditions using `AND`: `WHERE department = 'Engineering' AND salary > 90000`.",
        "expected_shape": {"exact_rows": 2, "columns_include": ["name", "salary"]},
        "solution": "SELECT name, salary FROM employees WHERE department = 'Engineering' AND salary > 90000;",
        "validation_type": "shape",
    },
    {
        "id": "t07", "level": 2, "xp": 90, "category": "Filtering & Logic",
        "title": "HR and Finance",
        "description": "Find employees who belong to either `HR` or `Finance`. Select their `name` and `department`.",
        "hint": "Use the `IN` operator: `WHERE department IN ('HR', 'Finance')`.",
        "expected_shape": {"exact_rows": 4, "columns_include": ["name", "department"]},
        "solution": "SELECT name, department FROM employees WHERE department IN ('HR', 'Finance');",
        "validation_type": "shape",
    },
    {
        "id": "t08", "level": 2, "xp": 100, "category": "Filtering & Logic",
        "title": "Starts with 'A' or 'B'",
        "description": "Find all employees whose `name` starts with either 'A' or 'B'. Return their `name`.",
        "hint": "Use the `LIKE` operator with `%` wildcard: `name LIKE 'A%' OR name LIKE 'B%'`.",
        "expected_shape": {"exact_rows": 2, "columns_include": ["name"]},
        "solution": "SELECT name FROM employees WHERE name LIKE 'A%' OR name LIKE 'B%';",
        "validation_type": "shape",
    },
    {
        "id": "t09", "level": 2, "xp": 100, "category": "Filtering & Logic",
        "title": "The Bosses",
        "description": "Find employees who do not have a manager (their `manager_id` is null). Return their `name`.",
        "hint": "To check for nulls, you must use `IS NULL`, not `= NULL`.",
        "expected_shape": {"exact_rows": 5, "columns_include": ["name"]},
        "solution": "SELECT name FROM employees WHERE manager_id IS NULL;",
        "validation_type": "shape",
    },

    # ── Level 3: Sorting & Limiting ────────────────────────────────────────
    {
        "id": "t10", "level": 3, "xp": 90, "category": "Sorting & Limiting",
        "title": "Sort by Salary",
        "description": "Select `name` and `salary` of all employees, ordered from highest salary to lowest.",
        "hint": "Add `ORDER BY salary DESC` to the end of your query.",
        "expected_shape": {"exact_rows": 10, "columns_include": ["name", "salary"]},
        "solution": "SELECT name, salary FROM employees ORDER BY salary DESC;",
        "validation_type": "shape",
    },
    {
        "id": "t11", "level": 3, "xp": 110, "category": "Sorting & Limiting",
        "title": "Multi-Column Sort",
        "description": "Select all employees. Order them primarily by `department` alphabetically (A-Z), and secondarily by `salary` descending (highest first).",
        "hint": "You can list multiple columns in ORDER BY: `ORDER BY department ASC, salary DESC`.",
        "expected_shape": {"exact_rows": 10, "columns_include": ["department", "salary"]},
        "solution": "SELECT * FROM employees ORDER BY department ASC, salary DESC;",
        "validation_type": "shape",
    },
    {
        "id": "t12", "level": 3, "xp": 100, "category": "Sorting & Limiting",
        "title": "Top 3 Earners",
        "description": "Show the `name` and `salary` of the **top 3** highest-paid employees.",
        "hint": "Use `ORDER BY salary DESC` followed by `LIMIT 3`.",
        "expected_shape": {"exact_rows": 3, "columns_include": ["name", "salary"]},
        "solution": "SELECT name, salary FROM employees ORDER BY salary DESC LIMIT 3;",
        "validation_type": "shape",
    },
    {
        "id": "t13", "level": 3, "xp": 120, "category": "Sorting & Limiting",
        "title": "The Runner Up",
        "description": "Find the employee with the **2nd highest** salary. Return `name` and `salary`.",
        "hint": "Order descending, then use `LIMIT 1 OFFSET 1` to skip the first row.",
        "expected_shape": {"exact_rows": 1, "columns_include": ["name", "salary"]},
        "solution": "SELECT name, salary FROM employees ORDER BY salary DESC LIMIT 1 OFFSET 1;",
        "validation_type": "shape",
    },

    # ── Level 4: Aggregations & Grouping ───────────────────────────────────
    {
        "id": "t14", "level": 4, "xp": 110, "category": "Aggregations & Grouping",
        "title": "Total Headcount",
        "description": "Count the total number of employees in the company. Alias the column as `total_employees`.",
        "hint": "Use `SELECT COUNT(*) AS total_employees FROM ...`.",
        "expected_shape": {"exact_rows": 1, "columns_include": ["total_employees"]},
        "solution": "SELECT COUNT(*) AS total_employees FROM employees;",
        "validation_type": "shape",
    },
    {
        "id": "t15", "level": 4, "xp": 120, "category": "Aggregations & Grouping",
        "title": "Company Payroll",
        "description": "Calculate the sum of all salaries, and the average salary. Alias them as `total_payroll` and `avg_salary`.",
        "hint": "Use `SUM(salary)` and `AVG(salary)` in your SELECT clause.",
        "expected_shape": {"exact_rows": 1, "columns_include": ["total_payroll", "avg_salary"]},
        "solution": "SELECT SUM(salary) AS total_payroll, AVG(salary) AS avg_salary FROM employees;",
        "validation_type": "shape",
    },
    {
        "id": "t16", "level": 4, "xp": 140, "category": "Aggregations & Grouping",
        "title": "Headcount per Department",
        "description": "Count how many employees are in each department. Return `department` and `headcount`.",
        "hint": "Select `department` and `COUNT(*) AS headcount`, then add `GROUP BY department`.",
        "expected_shape": {"exact_rows": 4, "columns_include": ["department", "headcount"]},
        "solution": "SELECT department, COUNT(*) AS headcount FROM employees GROUP BY department;",
        "validation_type": "shape",
    },
    {
        "id": "t17", "level": 4, "xp": 160, "category": "Aggregations & Grouping",
        "title": "Expensive Departments",
        "description": "Find departments whose **average salary** is strictly greater than **75,000**. Return `department` and the average salary as `avg_sal`.",
        "hint": "Use `GROUP BY department` and add a `HAVING AVG(salary) > 75000` clause.",
        "expected_shape": {"exact_rows": 2, "columns_include": ["department", "avg_sal"]},
        "solution": "SELECT department, AVG(salary) AS avg_sal FROM employees GROUP BY department HAVING AVG(salary) > 75000;",
        "validation_type": "shape",
    },

    # ── Level 5: Multi-table JOINs ─────────────────────────────────────────
    {
        "id": "t18", "level": 5, "xp": 150, "category": "Multi-table JOINs",
        "title": "Department Budgets",
        "description": "List each employee's `name`, their `department`, and the department's `budget`. Join `employees` with the `departments` table.",
        "hint": "Use `JOIN departments d ON e.department = d.name`.",
        "expected_shape": {"exact_rows": 10, "columns_include": ["name", "department", "budget"]},
        "solution": "SELECT e.name, e.department, d.budget FROM employees e JOIN departments d ON e.department = d.name;",
        "validation_type": "shape",
    },
    {
        "id": "t19", "level": 5, "xp": 170, "category": "Multi-table JOINs",
        "title": "Sales Champions",
        "description": "Show every employee's `name` and their `amount` from `sales`. Include employees who made 0 sales (the amount will be NULL).",
        "hint": "Use a `LEFT JOIN` on `sales` so employees without sales aren't filtered out.",
        "expected_shape": {"min_rows": 10, "columns_include": ["name", "amount"]},
        "solution": "SELECT e.name, s.amount FROM employees e LEFT JOIN sales s ON e.id = s.employee_id;",
        "validation_type": "shape",
    },
    {
        "id": "t20", "level": 5, "xp": 200, "category": "Multi-table JOINs",
        "title": "Project Assignments",
        "description": "List the `name` of the employee, the `role` they are playing, and the `title` of the project. This requires joining three tables.",
        "hint": "Join `employees` to `project_assignments`, and then join `projects`.",
        "expected_shape": {"min_rows": 5, "columns_include": ["name", "role", "title"]},
        "solution": "SELECT e.name, pa.role, p.title FROM employees e JOIN project_assignments pa ON e.id = pa.employee_id JOIN projects p ON pa.project_id = p.id;",
        "validation_type": "shape",
    },
    {
        "id": "t21", "level": 5, "xp": 220, "category": "Multi-table JOINs",
        "title": "Who's Your Boss?",
        "description": "Return a list of employees who have a manager. Show the employee's `name` as `employee_name` and their manager's `name` as `manager_name`.",
        "hint": "You need to do a 'Self Join'. Join `employees e` with `employees m` on `e.manager_id = m.id`.",
        "expected_shape": {"exact_rows": 5, "columns_include": ["employee_name", "manager_name"]},
        "solution": "SELECT e.name AS employee_name, m.name AS manager_name FROM employees e JOIN employees m ON e.manager_id = m.id;",
        "validation_type": "shape",
    },

    # ── Level 6: Subqueries & CTEs ─────────────────────────────────────────
    {
        "id": "t22", "level": 6, "xp": 180, "category": "Subqueries & CTEs",
        "title": "Above Average (Subquery)",
        "description": "Find all employees whose salary is **above the company average**. Return `name` and `salary`.",
        "hint": "Use a subquery in the WHERE clause: `WHERE salary > (SELECT AVG(salary) FROM employees)`.",
        "expected_shape": {"min_rows": 1, "columns_include": ["name", "salary"]},
        "solution": "SELECT name, salary FROM employees WHERE salary > (SELECT AVG(salary) FROM employees);",
        "validation_type": "shape",
    },
    {
        "id": "t23", "level": 6, "xp": 200, "category": "Subqueries & CTEs",
        "title": "Underbudget Projects",
        "description": "Find projects where the `budget` is less than the average project budget. Return `title` and `budget`.",
        "hint": "Use `WHERE budget < (SELECT AVG(budget) FROM projects)`.",
        "expected_shape": {"min_rows": 1, "columns_include": ["title", "budget"]},
        "solution": "SELECT title, budget FROM projects WHERE budget < (SELECT AVG(budget) FROM projects);",
        "validation_type": "shape",
    },
    {
        "id": "t24", "level": 6, "xp": 230, "category": "Subqueries & CTEs",
        "title": "Subquery with IN",
        "description": "Find the names of employees who work in departments that have a budget greater than `200,000`. Do this using a subquery (no JOIN).",
        "hint": "Use `WHERE department IN (SELECT name FROM departments WHERE budget > 200000)`.",
        "expected_shape": {"exact_rows": 4, "columns_include": ["name"]},
        "solution": "SELECT name FROM employees WHERE department IN (SELECT name FROM departments WHERE budget > 200000);",
        "validation_type": "shape",
    },
    {
        "id": "t25", "level": 6, "xp": 250, "category": "Subqueries & CTEs",
        "title": "Basic CTE",
        "description": "Rewrite the 'Above Average' task using a Common Table Expression (CTE). Define `avg_table` first, then join it.",
        "hint": "Syntax: `WITH avg_table AS (SELECT AVG(salary) AS avg_sal FROM employees) SELECT e.name, e.salary FROM employees e, avg_table a WHERE e.salary > a.avg_sal;`",
        "expected_shape": {"min_rows": 1, "columns_include": ["name", "salary"]},
        "solution": "WITH avg_table AS (SELECT AVG(salary) AS avg_sal FROM employees) SELECT e.name, e.salary FROM employees e JOIN avg_table a ON e.salary > a.avg_sal;",
        "validation_type": "shape",
    },

    # ── Level 7: Window Functions & Advanced ───────────────────────────────
    {
        "id": "t26", "level": 7, "xp": 250, "category": "Window Functions & Adv",
        "title": "Department Salary Ranks",
        "description": "For each employee, rank them by salary within their department (highest salary is rank 1). Return `name`, `department`, `salary`, and `dept_rank`.",
        "hint": "Use the window function: `RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS dept_rank`.",
        "expected_shape": {"exact_rows": 10, "columns_include": ["name", "dept_rank"]},
        "solution": "SELECT name, department, salary, RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS dept_rank FROM employees;",
        "validation_type": "shape",
    },
    {
        "id": "t27", "level": 7, "xp": 260, "category": "Window Functions & Adv",
        "title": "Running Total Sales",
        "description": "Return `sale_date`, `amount`, and a running total of sales chronologically alias it as `running_total`.",
        "hint": "Use `SUM(amount) OVER (ORDER BY sale_date) AS running_total`.",
        "expected_shape": {"min_rows": 1, "columns_include": ["sale_date", "amount", "running_total"]},
        "solution": "SELECT sale_date, amount, SUM(amount) OVER (ORDER BY sale_date) AS running_total FROM sales;",
        "validation_type": "shape",
    },
    {
        "id": "t28", "level": 7, "xp": 220, "category": "Window Functions & Adv",
        "title": "Handling Nulls (COALESCE)",
        "description": "Return all employee `name`s and their `manager_id`. But if they don't have a manager (it's null), display `-1` instead.",
        "hint": "Use `COALESCE(manager_id, -1)`.",
        "expected_shape": {"exact_rows": 10, "columns_include": ["name"]},
        "solution": "SELECT name, COALESCE(manager_id, -1) AS manager_id FROM employees;",
        "validation_type": "shape",
    },
    {
        "id": "t29", "level": 7, "xp": 280, "category": "Window Functions & Adv",
        "title": "Conditional Logic (CASE)",
        "description": "Select `name`, `salary`, and create a new column `salary_band`. If salary > 85000 it should be 'High', > 65000 'Medium', else 'Low'.",
        "hint": "Use `CASE WHEN salary > 85000 THEN 'High' WHEN salary > 65000 THEN 'Medium' ELSE 'Low' END AS salary_band`.",
        "expected_shape": {"exact_rows": 10, "columns_include": ["name", "salary_band"]},
        "solution": "SELECT name, salary, CASE WHEN salary > 85000 THEN 'High' WHEN salary > 65000 THEN 'Medium' ELSE 'Low' END AS salary_band FROM employees;",
        "validation_type": "shape",
    },
    {
        "id": "t30", "level": 7, "xp": 350, "category": "Window Functions & Adv",
        "title": "The Grand Challenge",
        "description": "Find the total sales amount per department. Show `department` name and `total_sales`. If a department has no sales, it should still appear with 0 or null total.",
        "hint": "Combine `departments`, `employees`, and `sales`. Use LEFT JOINs all the way down, then SUM(amount) GROUP BY department name.",
        "expected_shape": {"exact_rows": 4, "columns_include": ["department"]},
        "solution": "SELECT d.name AS department, SUM(s.amount) AS total_sales FROM departments d LEFT JOIN employees e ON d.name = e.department LEFT JOIN sales s ON e.id = s.employee_id GROUP BY d.name;",
        "validation_type": "shape",
    },
]


# ── Pydantic models ────────────────────────────────────────────────────────

class RunRequest(BaseModel):
    sql: str


class CheckRequest(BaseModel):
    task_id: str
    sql: str


class HintRequest(BaseModel):
    task_id: str
    sql: Optional[str] = None
    context: Optional[str] = None


# ── Helpers ────────────────────────────────────────────────────────────────

def _clean_sql(sql: str) -> str:
    sql = re.sub(r"```(?:sql)?\s*", "", sql, flags=re.IGNORECASE)
    sql = re.sub(r"```", "", sql).strip()
    return sql


def _run_sql(sql: str) -> Dict[str, Any]:
    sql = _clean_sql(sql)
    # Block destructive statements
    upper = sql.upper().strip()
    for kw in ("DROP", "DELETE", "TRUNCATE", "ALTER", "CREATE", "INSERT", "UPDATE", "PRAGMA", "ATTACH"):
        if upper.startswith(kw):
            raise ValueError(f"Statement type '{kw}' is not allowed in the playground.")

    conn = _get_sandbox_connection()
    try:
        cursor = conn.execute(sql)
        if cursor.description:
            columns = [d[0] for d in cursor.description]
            rows = [list(row) for row in cursor.fetchall()]
            return {"columns": columns, "rows": rows, "row_count": len(rows)}
        return {"columns": [], "rows": [], "row_count": 0}
    finally:
        conn.close()


def _validate_result(result: Dict[str, Any], task: Dict[str, Any]) -> tuple[bool, str]:
    shape = task.get("expected_shape", {})
    columns = [c.lower() for c in result.get("columns", [])]
    rows = result.get("rows", [])

    # Exact row count
    if "exact_rows" in shape and len(rows) != shape["exact_rows"]:
        return False, f"Expected exactly {shape['exact_rows']} row(s), got {len(rows)}."

    # Minimum row count
    if "min_rows" in shape and len(rows) < shape["min_rows"]:
        return False, f"Expected at least {shape['min_rows']} row(s), got {len(rows)}."

    # Exact columns
    if "exact_columns" in shape:
        required = [c.lower() for c in shape["exact_columns"]]
        if set(columns) != set(required):
            return False, f"Expected columns {required}, got {columns}."

    # Columns include
    if "columns_include" in shape:
        for req_col in shape["columns_include"]:
            if req_col.lower() not in columns:
                return False, f"Result is missing required column '{req_col}'."

    return True, "Correct! Great work."


# ── Routes ─────────────────────────────────────────────────────────────────

@router.get("/schema")
async def get_playground_schema():
    """Return the schema of the sandbox database."""
    return {"status": "ok", "schema": SCHEMA_INFO}


@router.get("/tasks")
async def list_tasks():
    """Return the full curriculum task list."""
    # Strip solutions before sending to client
    safe = []
    for t in TASKS:
        s = {k: v for k, v in t.items() if k not in ("solution", "validation_type")}
        safe.append(s)
    return {"status": "ok", "tasks": safe, "total": len(safe)}


@router.post("/run")
async def run_sql(req: RunRequest):
    """Execute arbitrary SELECT SQL in the sandboxed SQLite database."""
    try:
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(_executor, lambda: _run_sql(req.sql))
        return {"status": "ok", **result}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"SQL error: {e}")


@router.post("/check")
async def check_answer(req: CheckRequest):
    """Grade the user's SQL answer against the expected task output."""
    task = next((t for t in TASKS if t["id"] == req.task_id), None)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found.")

    try:
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(_executor, lambda: _run_sql(req.sql))
    except Exception as e:
        return {"correct": False, "message": f"SQL error: {e}", "xp_earned": 0}

    correct, message = _validate_result(result, task)
    return {
        "correct": correct,
        "message": message,
        "xp_earned": task["xp"] if correct else 0,
        "result": result,
    }


@router.post("/hint")
async def get_hint(req: HintRequest, request: Request):
    """AI-powered contextual hint for a playground task."""
    task = next((t for t in TASKS if t["id"] == req.task_id), None)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found.")

    # First tier: static hint
    static_hint = task.get("hint", "No hint available.")

    # Second tier: AI hint if the user provides their current SQL attempt
    if req.sql and req.sql.strip():
        try:
            import litellm
            from app.security.secrets import gemini_api_key

            prompt = textwrap.dedent(f"""
                You are a friendly SQL tutor helping a student with this task:

                Task: {task['title']}
                Description: {task['description']}

                The student wrote this SQL:
                {req.sql}

                Give a SHORT, encouraging hint (2-3 sentences max) pointing out what is wrong
                or what to improve. Do NOT reveal the answer. Be beginner-friendly.
            """).strip()

            api_key = gemini_api_key()
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                _executor,
                lambda: litellm.completion(
                    model="gemini/gemini-2.5-flash",
                    messages=[{"role": "user", "content": prompt}],
                    api_key=api_key,
                ),
            )
            ai_hint = response.choices[0].message.content.strip()
            return {"hint": ai_hint, "tier": "ai"}
        except Exception:
            pass

    return {"hint": static_hint, "tier": "static"}
