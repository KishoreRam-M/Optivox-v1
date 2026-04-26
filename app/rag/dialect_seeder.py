"""
rag/dialect_seeder.py
----------------------
Pre-loads dialect_docs LanceDB table with SQL curriculum content:
  - Beginner: basics, SELECT, WHERE, ORDER BY, LIMIT/ROWNUM, ILIKE
  - Intermediate: JOINs, GROUP BY, subqueries, CTEs
  - Advanced: window functions, indexes, query optimization patterns

Also seeds dialect-specific syntax for MySQL, PostgreSQL, and Oracle.
"""

from __future__ import annotations

import logging
from typing import List, Dict

logger = logging.getLogger(__name__)

# ── Curriculum ────────────────────────────────────────────────────────────

DIALECT_DOCS: List[Dict[str, str]] = [

    # ── Beginner ─────────────────────────────────────────────────────────
    {
        "id": "beginner_select",
        "level": "beginner",
        "topic": "SELECT basics",
        "text": (
            "SELECT basics:\n"
            "SELECT column1, column2 FROM table_name;\n"
            "Use * to select all columns: SELECT * FROM employees;\n"
            "Use DISTINCT to remove duplicates: SELECT DISTINCT department FROM employees;\n"
            "Add aliases: SELECT first_name AS name FROM employees;"
        ),
    },
    {
        "id": "beginner_where",
        "level": "beginner",
        "topic": "WHERE filtering",
        "text": (
            "WHERE clause for filtering rows:\n"
            "SELECT * FROM orders WHERE status = 'active';\n"
            "Combine conditions: WHERE price > 100 AND category = 'electronics';\n"
            "Use IN: WHERE country IN ('US', 'UK', 'CA');\n"
            "Use BETWEEN: WHERE order_date BETWEEN '2024-01-01' AND '2024-12-31';\n"
            "Use LIKE for pattern matching: WHERE name LIKE 'John%';"
        ),
    },
    {
        "id": "beginner_order_limit",
        "level": "beginner",
        "topic": "ORDER BY and LIMIT",
        "text": (
            "Sorting and limiting results:\n"
            "ORDER BY: SELECT * FROM products ORDER BY price DESC;\n"
            "Multiple sort keys: ORDER BY category ASC, price DESC;\n"
            "MySQL/PostgreSQL LIMIT: SELECT * FROM orders LIMIT 10;\n"
            "MySQL LIMIT with offset: SELECT * FROM orders LIMIT 10 OFFSET 20;\n"
            "Oracle ROWNUM (legacy): SELECT * FROM employees WHERE ROWNUM <= 10;\n"
            "Oracle 12c+ FETCH FIRST: SELECT * FROM employees FETCH FIRST 10 ROWS ONLY;"
        ),
    },
    {
        "id": "beginner_aggregates",
        "level": "beginner",
        "topic": "Aggregate functions",
        "text": (
            "Aggregate functions summarize multiple rows:\n"
            "COUNT(*) — total rows: SELECT COUNT(*) FROM orders;\n"
            "SUM: SELECT SUM(total_amount) FROM orders WHERE year = 2024;\n"
            "AVG: SELECT AVG(salary) FROM employees;\n"
            "MIN / MAX: SELECT MIN(price), MAX(price) FROM products;\n"
            "GROUP BY: SELECT department, COUNT(*) FROM employees GROUP BY department;\n"
            "HAVING (filter on aggregates): HAVING COUNT(*) > 5;"
        ),
    },

    # ── Dialect-specific syntax ───────────────────────────────────────────
    {
        "id": "dialect_mysql_limit",
        "level": "beginner",
        "topic": "MySQL LIMIT syntax",
        "text": (
            "MySQL pagination with LIMIT and OFFSET:\n"
            "SELECT * FROM products ORDER BY created_at DESC LIMIT 20 OFFSET 40;\n"
            "Shorthand: LIMIT offset, count → LIMIT 40, 20\n"
            "MySQL does NOT support FETCH FIRST syntax — always use LIMIT.\n"
            "Example: top 5 orders by value:\n"
            "SELECT order_id, total FROM orders ORDER BY total DESC LIMIT 5;"
        ),
    },
    {
        "id": "dialect_postgres_ilike",
        "level": "beginner",
        "topic": "PostgreSQL ILIKE case-insensitive search",
        "text": (
            "PostgreSQL ILIKE for case-insensitive pattern matching:\n"
            "SELECT * FROM customers WHERE email ILIKE '%gmail.com';\n"
            "ILIKE is PostgreSQL-specific — MySQL uses LIKE (case-insensitive by default on utf8 collations).\n"
            "PostgreSQL also supports SIMILAR TO for regex-like patterns.\n"
            "Example: find all users whose name starts with 'john' (any case):\n"
            "SELECT * FROM users WHERE username ILIKE 'john%';\n"
            "Combine: WHERE first_name ILIKE 'a%' OR last_name ILIKE 'a%';"
        ),
    },
    {
        "id": "dialect_oracle_rownum",
        "level": "beginner",
        "topic": "Oracle ROWNUM and FETCH FIRST",
        "text": (
            "Oracle row limiting:\n"
            "Legacy ROWNUM (Oracle 11g and earlier):\n"
            "  SELECT * FROM (SELECT * FROM employees ORDER BY salary DESC) WHERE ROWNUM <= 10;\n"
            "Note: ROWNUM must be in outer query when combined with ORDER BY.\n"
            "Modern Oracle 12c+ FETCH FIRST (preferred):\n"
            "  SELECT * FROM employees ORDER BY salary DESC FETCH FIRST 10 ROWS ONLY;\n"
            "  With offset: OFFSET 20 ROWS FETCH NEXT 10 ROWS ONLY;\n"
            "Oracle does NOT support LIMIT — always use ROWNUM or FETCH FIRST."
        ),
    },
    {
        "id": "dialect_postgres_limit",
        "level": "beginner",
        "topic": "PostgreSQL LIMIT syntax",
        "text": (
            "PostgreSQL LIMIT and OFFSET:\n"
            "SELECT * FROM orders ORDER BY created_at DESC LIMIT 10 OFFSET 20;\n"
            "PostgreSQL also supports FETCH FIRST (SQL standard):\n"
            "  SELECT * FROM orders ORDER BY id FETCH FIRST 5 ROWS ONLY;\n"
            "PostgreSQL DISTINCT ON (unique to PG):\n"
            "  SELECT DISTINCT ON (customer_id) * FROM orders ORDER BY customer_id, created_at DESC;"
        ),
    },

    # ── Intermediate ─────────────────────────────────────────────────────
    {
        "id": "intermediate_inner_join",
        "level": "intermediate",
        "topic": "INNER JOIN",
        "text": (
            "INNER JOIN returns rows where both tables have matching values:\n"
            "SELECT o.order_id, c.name FROM orders o\n"
            "INNER JOIN customers c ON o.customer_id = c.id;\n"
            "Only rows with a match in both tables are returned.\n"
            "Use table aliases (o, c) for readability with multiple joins.\n"
            "Multi-join: SELECT o.id, c.name, p.title\n"
            "FROM orders o\n"
            "JOIN customers c ON o.customer_id = c.id\n"
            "JOIN products p ON o.product_id = p.id;"
        ),
    },
    {
        "id": "intermediate_left_join",
        "level": "intermediate",
        "topic": "LEFT JOIN / RIGHT JOIN / FULL OUTER JOIN",
        "text": (
            "LEFT JOIN: all rows from left table, matched rows from right (NULL if no match):\n"
            "SELECT c.name, o.order_id FROM customers c\n"
            "LEFT JOIN orders o ON c.id = o.customer_id;\n"
            "→ Returns ALL customers, even those with no orders.\n\n"
            "RIGHT JOIN: all rows from right table (less common, prefer LEFT JOIN).\n\n"
            "FULL OUTER JOIN: all rows from both tables:\n"
            "SELECT * FROM a FULL OUTER JOIN b ON a.id = b.id;\n"
            "Note: MySQL does not support FULL OUTER JOIN natively — emulate with UNION."
        ),
    },
    {
        "id": "intermediate_subquery",
        "level": "intermediate",
        "topic": "Subqueries",
        "text": (
            "Subqueries (nested SELECT statements):\n"
            "Scalar subquery: SELECT name, (SELECT AVG(salary) FROM employees) AS avg_sal FROM employees;\n"
            "IN subquery: SELECT * FROM orders WHERE customer_id IN (SELECT id FROM customers WHERE country = 'US');\n"
            "EXISTS: SELECT * FROM customers c WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id);\n"
            "Correlated subquery (references outer query):\n"
            "SELECT * FROM employees e WHERE salary > (SELECT AVG(salary) FROM employees WHERE department = e.department);"
        ),
    },
    {
        "id": "intermediate_cte",
        "level": "intermediate",
        "topic": "CTEs (Common Table Expressions)",
        "text": (
            "CTEs with WITH clause — named subqueries for readability:\n"
            "WITH high_value_customers AS (\n"
            "  SELECT customer_id, SUM(total) AS lifetime_value\n"
            "  FROM orders GROUP BY customer_id HAVING SUM(total) > 10000\n"
            ")\n"
            "SELECT c.name, h.lifetime_value FROM high_value_customers h\n"
            "JOIN customers c ON h.customer_id = c.id;\n\n"
            "Multiple CTEs:\n"
            "WITH cte1 AS (...), cte2 AS (...) SELECT ...;\n\n"
            "Recursive CTE (for hierarchical data):\n"
            "WITH RECURSIVE tree AS (\n"
            "  SELECT id, parent_id, name FROM categories WHERE parent_id IS NULL\n"
            "  UNION ALL\n"
            "  SELECT c.id, c.parent_id, c.name FROM categories c JOIN tree t ON c.parent_id = t.id\n"
            ")\n"
            "SELECT * FROM tree;"
        ),
    },

    # ── Advanced ─────────────────────────────────────────────────────────
    {
        "id": "advanced_window_functions",
        "level": "advanced",
        "topic": "Window functions",
        "text": (
            "Window functions compute values across a set of related rows without collapsing them:\n\n"
            "ROW_NUMBER() — unique sequential number:\n"
            "SELECT name, salary, ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) AS rank\n"
            "FROM employees;\n\n"
            "RANK() and DENSE_RANK() — handle ties differently:\n"
            "RANK() skips numbers after ties; DENSE_RANK() does not.\n\n"
            "LAG / LEAD — access previous/next row values:\n"
            "SELECT date, revenue, LAG(revenue, 1) OVER (ORDER BY date) AS prev_revenue FROM sales;\n\n"
            "SUM() OVER — running total:\n"
            "SELECT date, amount, SUM(amount) OVER (ORDER BY date ROWS UNBOUNDED PRECEDING) AS running_total\n"
            "FROM transactions;"
        ),
    },
    {
        "id": "advanced_indexes",
        "level": "advanced",
        "topic": "Indexes and query performance",
        "text": (
            "Indexes speed up queries but slow down writes:\n\n"
            "Create index: CREATE INDEX idx_orders_customer ON orders(customer_id);\n"
            "Unique index: CREATE UNIQUE INDEX idx_users_email ON users(email);\n"
            "Composite index: CREATE INDEX idx_orders_date_status ON orders(order_date, status);\n\n"
            "When indexes help:\n"
            "- WHERE clause on indexed column\n"
            "- JOIN ON indexed columns\n"
            "- ORDER BY on indexed column\n\n"
            "When indexes hurt:\n"
            "- Low-cardinality columns (e.g., boolean flags)\n"
            "- Very small tables\n"
            "- Heavy INSERT/UPDATE workloads\n\n"
            "EXPLAIN to analyze: EXPLAIN SELECT * FROM orders WHERE customer_id = 42;\n"
            "Look for 'ALL' (full scan) vs 'ref' or 'index' in MySQL EXPLAIN output."
        ),
    },
    {
        "id": "advanced_explain",
        "level": "advanced",
        "topic": "EXPLAIN and query optimization",
        "text": (
            "Understanding EXPLAIN output:\n\n"
            "MySQL EXPLAIN:\n"
            "EXPLAIN SELECT * FROM orders o JOIN customers c ON o.customer_id = c.id WHERE c.country = 'US';\n"
            "Key columns: type (ALL=full scan, ref=index lookup), rows (estimated), key (index used).\n\n"
            "PostgreSQL EXPLAIN ANALYZE:\n"
            "EXPLAIN ANALYZE SELECT * FROM orders WHERE created_at > NOW() - INTERVAL '30 days';\n"
            "Shows actual vs estimated rows and execution time.\n\n"
            "Optimization patterns:\n"
            "1. Add index on JOIN and WHERE columns\n"
            "2. Avoid SELECT * — select only needed columns\n"
            "3. Use covering indexes for frequently-queried column combinations\n"
            "4. Avoid functions on indexed columns in WHERE (WHERE YEAR(date)=2024 prevents index use)\n"
            "5. Use CTEs or subqueries to pre-filter large tables"
        ),
    },
    {
        "id": "advanced_transactions",
        "level": "advanced",
        "topic": "Transactions and ACID",
        "text": (
            "Transactions ensure atomic, consistent, isolated, durable operations:\n\n"
            "Basic transaction:\n"
            "BEGIN;\n"
            "UPDATE accounts SET balance = balance - 500 WHERE id = 1;\n"
            "UPDATE accounts SET balance = balance + 500 WHERE id = 2;\n"
            "COMMIT;\n\n"
            "On error, rollback:\n"
            "ROLLBACK;\n\n"
            "Savepoints for partial rollback:\n"
            "SAVEPOINT sp1;\n"
            "-- some work --\n"
            "ROLLBACK TO sp1;\n\n"
            "Isolation levels (strictest to loosest):\n"
            "SERIALIZABLE → REPEATABLE READ → READ COMMITTED → READ UNCOMMITTED\n"
            "Most databases default to READ COMMITTED or REPEATABLE READ."
        ),
    },
]


def seed_dialect_docs() -> None:
    """
    Embed all DIALECT_DOCS into LanceDB dialect_docs table.
    Safe to call multiple times — uses merge_insert (upsert).
    """
    try:
        from app.rag.embedder import _get_db, _embed, _ensure_table, _TABLE_DIALECT_DOCS
        db = _get_db()
        tbl = _ensure_table(db, _TABLE_DIALECT_DOCS, {})
        rows = []
        for doc in DIALECT_DOCS:
            rows.append({
                "id": doc["id"],
                "text": doc["text"],
                "vector": _embed(doc["text"]),
                "metadata": f'{{"level": "{doc["level"]}", "topic": "{doc["topic"]}"}}',
            })
        if rows:
            tbl.merge_insert("id").when_matched_update_all().when_not_matched_insert_all().execute(rows)
            logger.info("Seeded %d dialect docs into LanceDB.", len(rows))
    except Exception as exc:
        logger.error("Dialect doc seeding failed: %s", exc)
