import pytest
from app.utils.sql_utils import extract_sql

def test_extract_sql_fenced():
    raw = "Here is your query:\n```sql\nSELECT * FROM users;\n```\nExplanation..."
    assert extract_sql(raw) == "SELECT * FROM users;"
    
def test_extract_sql_unfenced_with_prose():
    raw = "The best query to run is:\nSELECT * FROM orders WHERE status = 'pending';\nThis gets the pending orders."
    assert extract_sql(raw) == "SELECT * FROM orders WHERE status = 'pending';\nThis gets the pending orders."

def test_extract_sql_lowercase():
    raw = "select id from users"
    assert extract_sql(raw) == "select id from users"

def test_extract_sql_stray_ticks():
    raw = "```\nSELECT * FROM logs\n```"
    assert extract_sql(raw) == "SELECT * FROM logs"
