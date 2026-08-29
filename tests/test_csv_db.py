import pytest
import sqlite3
import os
import tempfile
from app.api.csv_db import _run_query

@pytest.fixture
def temp_db():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE users (id INTEGER, name TEXT)")
    conn.execute("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")
    conn.commit()
    conn.close()
    
    yield path
    
    os.remove(path)

def test_run_query_select(temp_db):
    result = _run_query(temp_db, "SELECT * FROM users")
    assert result["row_count"] == 2
    assert result["columns"] == ["id", "name"]
    assert len(result["rows"]) == 2

def test_run_query_limit_added(temp_db):
    # Should automatically add LIMIT 500
    result = _run_query(temp_db, "SELECT * FROM users")
    assert result["row_count"] == 2
    
def test_run_query_forbidden_dml(temp_db):
    with pytest.raises(ValueError, match="Statement type 'DROP' is not allowed"):
        _run_query(temp_db, "DROP TABLE users")
        
    with pytest.raises(ValueError, match="Statement type 'DELETE' is not allowed"):
        _run_query(temp_db, "DELETE FROM users")
        
    # Test CTE with DML
    with pytest.raises(ValueError, match="Statement type 'DELETE' is not allowed"):
        _run_query(temp_db, "WITH cte AS (SELECT 1) DELETE FROM users")

def test_run_query_union_injection(temp_db):
    with pytest.raises(ValueError, match="UNION SELECT is not allowed"):
        _run_query(temp_db, "SELECT * FROM users UNION SELECT 1, 'Hacked'")

def test_run_query_stacked_injection(temp_db):
    with pytest.raises(ValueError, match="Multiple statements"):
        _run_query(temp_db, "SELECT * FROM users; DROP TABLE users;")
        
def test_run_query_semicolon_in_string(temp_db):
    # Semicolon in string should be allowed
    conn = sqlite3.connect(temp_db)
    conn.execute("INSERT INTO users VALUES (3, 'Eve; DROP TABLE users;')")
    conn.commit()
    conn.close()
    
    result = _run_query(temp_db, "SELECT * FROM users WHERE name = 'Eve; DROP TABLE users;'")
    assert result["row_count"] == 1
