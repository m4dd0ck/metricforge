"""Generate sample e-commerce data for MetricForge testing."""

import random
from datetime import date, timedelta
from pathlib import Path

import duckdb


def generate_sample_data(output_dir: Path | None = None) -> duckdb.DuckDBPyConnection:
    """Generate sample e-commerce data.

    Args:
        output_dir: Directory to save Parquet files, or None for in-memory only.

    Returns:
        DuckDB connection with loaded data.
    """
    random.seed(42)  # Reproducible data

    # Generate orders data
    orders_data = generate_orders(1000)

    # Generate sessions data
    sessions_data = generate_sessions(5000)

    # Create DuckDB connection and load data
    conn = duckdb.connect(":memory:")

    # Create orders table
    conn.execute("""
        CREATE TABLE orders (
            order_id INTEGER PRIMARY KEY,
            customer_id INTEGER,
            product_id INTEGER,
            amount DECIMAL(10, 2),
            quantity INTEGER,
            status VARCHAR,
            country VARCHAR,
            category VARCHAR,
            payment_method VARCHAR,
            order_date DATE
        )
    """)

    conn.executemany(
        """
        INSERT INTO orders VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        orders_data,
    )

    # Create sessions table
    conn.execute("""
        CREATE TABLE sessions (
            session_id INTEGER PRIMARY KEY,
            user_id INTEGER,
            page_view_count INTEGER,
            traffic_source VARCHAR,
            device_type VARCHAR,
            session_date DATE
        )
    """)

    conn.executemany(
        """
        INSERT INTO sessions VALUES (?, ?, ?, ?, ?, ?)
        """,
        sessions_data,
    )

    # Export to Parquet if output_dir provided
    if output_dir:
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        conn.execute(f"COPY orders TO '{output_dir}/orders.parquet' (FORMAT PARQUET)")
        conn.execute(f"COPY sessions TO '{output_dir}/sessions.parquet' (FORMAT PARQUET)")
        print(f"Data exported to {output_dir}")

    return conn


def generate_orders(count: int) -> list[tuple]:
    """Generate order records."""
    statuses = ["completed", "completed", "completed", "completed", "pending", "cancelled"]
    countries = ["US", "US", "US", "UK", "UK", "DE", "FR", "CA", "AU"]
    categories = ["Electronics", "Clothing", "Home", "Books", "Sports", "Beauty"]
    payment_methods = ["credit_card", "credit_card", "paypal", "debit_card", "apple_pay"]

    start_date = date(2024, 1, 1)
    end_date = date(2024, 12, 31)
    date_range = (end_date - start_date).days

    orders = []
    for i in range(1, count + 1):
        order_date = start_date + timedelta(days=random.randint(0, date_range))
        status = random.choice(statuses)
        amount = round(random.uniform(10, 500), 2)
        quantity = random.randint(1, 5)

        orders.append(
            (
                i,  # order_id
                random.randint(1, 200),  # customer_id
                random.randint(1, 100),  # product_id
                amount,
                quantity,
                status,
                random.choice(countries),
                random.choice(categories),
                random.choice(payment_methods),
                order_date,
            )
        )

    return orders


def generate_sessions(count: int) -> list[tuple]:
    """Generate session records."""
    traffic_sources = ["organic", "organic", "paid", "social", "direct", "email", "referral"]
    device_types = ["desktop", "desktop", "mobile", "mobile", "tablet"]

    start_date = date(2024, 1, 1)
    end_date = date(2024, 12, 31)
    date_range = (end_date - start_date).days

    sessions = []
    for i in range(1, count + 1):
        session_date = start_date + timedelta(days=random.randint(0, date_range))

        sessions.append(
            (
                i,  # session_id
                random.randint(1, 500),  # user_id
                random.randint(1, 20),  # page_view_count
                random.choice(traffic_sources),
                random.choice(device_types),
                session_date,
            )
        )

    return sessions


if __name__ == "__main__":
    import sys

    output_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).parent
    conn = generate_sample_data(output_dir)

    # Print summary
    result = conn.execute("SELECT COUNT(*) FROM orders").fetchone()
    print(f"Generated {result[0]} orders")

    result = conn.execute("SELECT COUNT(*) FROM sessions").fetchone()
    print(f"Generated {result[0]} sessions")

    result = conn.execute(
        "SELECT SUM(amount) as revenue FROM orders WHERE status = 'completed'"
    ).fetchone()
    print(f"Total revenue: ${result[0]:,.2f}")

    conn.close()
