"""Pytest fixtures for MetricForge tests."""

from collections.abc import Generator
from pathlib import Path

import pytest

from metricforge.executor.duckdb_executor import DuckDBExecutor
from metricforge.parser.loader import MetricRegistry
from metricforge.store import MetricStore


@pytest.fixture
def sample_metrics_yaml() -> str:
    """Sample metric YAML content for testing."""
    return """
semantic_models:
  - name: orders
    description: "Test order model"
    table: orders
    primary_entity: order_id

    entities:
      - name: order_id
        type: primary
      - name: customer_id
        type: foreign

    measures:
      - name: order_amount
        agg: sum
        expr: amount
        description: "Total order amount"

      - name: order_count
        agg: count
        expr: order_id
        description: "Count of orders"

      - name: unique_customers
        agg: count_distinct
        expr: customer_id
        description: "Unique customers"

    dimensions:
      - name: order_date
        type: time
        expr: order_date
        time_granularity: day

      - name: order_status
        type: categorical
        expr: status

      - name: country
        type: categorical
        expr: country

metrics:
  - name: revenue
    description: "Total revenue"
    type: simple
    type_params:
      measure: order_amount
    filter: "status = 'completed'"

  - name: total_orders
    description: "Total orders"
    type: simple
    type_params:
      measure: order_count

  - name: completed_orders
    description: "Completed orders"
    type: simple
    type_params:
      measure: order_count
    filter: "status = 'completed'"

  - name: customer_count
    description: "Unique customers"
    type: simple
    type_params:
      measure: unique_customers

  - name: average_order_value
    description: "Average order value"
    type: derived
    type_params:
      expr: "revenue / completed_orders"
      metrics:
        - revenue
        - completed_orders

  - name: order_completion_rate
    description: "Order completion rate"
    type: ratio
    type_params:
      numerator: completed_orders
      denominator: total_orders

  - name: cumulative_revenue
    description: "Running revenue total"
    type: cumulative
    type_params:
      measure: order_amount
    filter: "status = 'completed'"
"""


@pytest.fixture
def metrics_dir(tmp_path: Path, sample_metrics_yaml: str) -> Path:
    """Create a temporary metrics directory with sample YAML."""
    metrics_path = tmp_path / "metrics"
    metrics_path.mkdir()
    (metrics_path / "test.yaml").write_text(sample_metrics_yaml)
    return metrics_path


@pytest.fixture
def sample_orders_data() -> list[tuple]:
    """Sample orders data for testing."""
    return [
        (1, 101, 100.00, "completed", "US", "2024-01-15"),
        (2, 102, 150.00, "completed", "UK", "2024-01-16"),
        (3, 101, 200.00, "pending", "US", "2024-01-17"),
        (4, 103, 75.00, "completed", "US", "2024-01-18"),
        (5, 104, 300.00, "cancelled", "DE", "2024-01-19"),
        (6, 105, 125.00, "completed", "US", "2024-02-01"),
        (7, 102, 175.00, "completed", "UK", "2024-02-15"),
        (8, 106, 250.00, "completed", "US", "2024-02-20"),
        (9, 107, 50.00, "pending", "FR", "2024-03-01"),
        (10, 108, 400.00, "completed", "US", "2024-03-15"),
    ]


@pytest.fixture
def db_with_data(sample_orders_data: list[tuple]) -> Generator[DuckDBExecutor, None, None]:
    """Create a DuckDB executor with sample data."""
    executor = DuckDBExecutor()

    executor.conn.execute("""
        CREATE TABLE orders (
            order_id INTEGER,
            customer_id INTEGER,
            amount DECIMAL(10, 2),
            status VARCHAR,
            country VARCHAR,
            order_date DATE
        )
    """)

    executor.conn.executemany(
        "INSERT INTO orders VALUES (?, ?, ?, ?, ?, ?)",
        sample_orders_data,
    )

    yield executor
    executor.close()


@pytest.fixture
def registry(metrics_dir: Path) -> MetricRegistry:
    """Create a loaded MetricRegistry."""
    reg = MetricRegistry()
    reg.load_directory(metrics_dir)
    return reg


@pytest.fixture
def store_with_data(
    metrics_dir: Path, sample_orders_data: list[tuple]
) -> Generator[MetricStore, None, None]:
    """Create a MetricStore with loaded data."""
    store = MetricStore(metrics_dir)

    store.executor.conn.execute("""
        CREATE TABLE orders (
            order_id INTEGER,
            customer_id INTEGER,
            amount DECIMAL(10, 2),
            status VARCHAR,
            country VARCHAR,
            order_date DATE
        )
    """)

    store.executor.conn.executemany(
        "INSERT INTO orders VALUES (?, ?, ?, ?, ?, ?)",
        sample_orders_data,
    )

    yield store
    store.close()
