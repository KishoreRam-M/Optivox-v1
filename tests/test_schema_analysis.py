import pytest
from app.api.schema_analysis import analyze_schema, _parse_fk_reference

def test_parse_fk_reference():
    assert _parse_fk_reference("users(id)") == ("users", "id")
    assert _parse_fk_reference("public.orders(customer_id)") == ("public.orders", "customer_id")
    assert _parse_fk_reference("table_with_no_parens") == ("table_with_no_parens", "")
    assert _parse_fk_reference("") == ("", "")

def test_analyze_schema_fks_and_isolation():
    tables = [
        {
            "table_name": "users",
            "columns": [{"name": "id", "type": "INTEGER"}],
            "foreign_keys": []
        },
        {
            "table_name": "orders",
            "columns": [{"name": "id", "type": "INTEGER"}, {"name": "user_id", "type": "INTEGER"}],
            "foreign_keys": [{"column": "user_id", "references": "users(id)"}]
        },
        {
            "table_name": "audit_logs",
            "columns": [{"name": "id", "type": "INTEGER"}, {"name": "action", "type": "VARCHAR"}],
            "foreign_keys": []
        }
    ]

    analysis = analyze_schema(tables)
    
    # Check FK Map
    fk_map = analysis.get("fk_relationship_map", {})
    assert "orders" in fk_map
    assert len(fk_map["orders"]) == 1
    assert fk_map["orders"][0]["from_col"] == "user_id"
    assert fk_map["orders"][0]["to_table"] == "users"
    assert fk_map["orders"][0]["to_col"] == "id"
    
    # Check Isolated Tables
    isolated = analysis.get("isolated_tables", [])
    assert "audit_logs" in isolated
    assert "users" not in isolated  # Referenced by orders
    assert "orders" not in isolated # References users
