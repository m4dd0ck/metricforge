"""Initialize DuckDB database with sample data."""

import duckdb
from pathlib import Path

def init_database(db_path: str = "data/metricforge.duckdb"):
    """Create a DuckDB database with sample data loaded."""
    conn = duckdb.connect(db_path)

    # Load parquet files
    conn.execute("""
        CREATE OR REPLACE TABLE orders AS
        SELECT * FROM read_parquet('data/orders.parquet')
    """)

    conn.execute("""
        CREATE OR REPLACE TABLE sessions AS
        SELECT * FROM read_parquet('data/sessions.parquet')
    """)

    print(f"Database initialized at {db_path}")

    # Show stats
    orders = conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
    sessions = conn.execute("SELECT COUNT(*) FROM sessions").fetchone()[0]
    print(f"  - {orders} orders")
    print(f"  - {sessions} sessions")

    conn.close()

if __name__ == "__main__":
    init_database()
