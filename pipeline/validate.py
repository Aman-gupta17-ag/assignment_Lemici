"""Data validation helpers for pipeline."""
import os
import sqlite3

DB_PATH = os.environ.get("DB_PATH", "data/raw/mospi.db")


def validate_db() -> bool:
    """Check that DB exists and has required tables."""
    if not os.path.isfile(DB_PATH):
        return False
    conn = sqlite3.connect(DB_PATH)
    cur = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name IN ('documents','files','tables')"
    )
    tables = {r[0] for r in cur.fetchall()}
    conn.close()
    return tables == {"documents", "files", "tables"}
