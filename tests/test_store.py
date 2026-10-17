"""Integration tests for MetricStore."""

from collections.abc import Generator
from pathlib import Path

import pytest

from metricforge.compiler.sql_builder import MultiModelQueryError
from metricforge.models.query import QueryResult
from metricforge.store import MetricStore


class TestMetricStore:
    def test_create_store(self, metrics_dir: Path):
        """Can create a MetricStore."""
        store = MetricStore(metrics_dir)
        assert len(store.registry.metrics) > 0
        store.close()

    def test_list_metrics(self, metrics_dir: Path):
        """Can list all metrics."""
        store = MetricStore(metrics_dir)
        metrics = store.list_metrics()

        assert len(metrics) > 0
        assert all("name" in m for m in metrics)
        assert all("type" in m for m in metrics)
        store.close()

    def test_list_dimensions(self, metrics_dir: Path):
        """Can list all dimensions."""
        store = MetricStore(metrics_dir)
        dimensions = store.list_dimensions()

        assert len(dimensions) > 0
        assert all("name" in d for d in dimensions)
        assert all("type" in d for d in dimensions)
        store.close()

    def test_list_measures(self, metrics_dir: Path):
        """Can list all measures."""
        store = MetricStore(metrics_dir)
        measures = store.list_measures()

        assert len(measures) > 0
        assert all("name" in m for m in measures)
        assert all("agg" in m for m in measures)
        store.close()

    def test_get_sql(self, metrics_dir: Path):
        """Can generate SQL without executing."""
        store = MetricStore(metrics_dir)
        sql = store.get_sql(metrics=["revenue"])

        assert "SELECT" in sql.upper()
        assert "SUM" in sql.upper()
        store.close()

    def test_validate_success(self, metrics_dir: Path):
        """Validation passes for valid metrics."""
        store = MetricStore(metrics_dir)
        store.validate()
        # Note: validation may have errors if tables don't exist
        # but the metrics should at least compile
        store.close()

    def test_context_manager(self, metrics_dir: Path):
        """Store can be used as context manager."""
        with MetricStore(metrics_dir) as store:
            metrics = store.list_metrics()
            assert len(metrics) > 0


class TestMetricStoreQueries:
    def test_query_simple_metric(self, store_with_data: MetricStore):
        """Can query a simple metric."""
        result = store_with_data.query(metrics=["total_orders"])

        assert result.row_count == 1
        assert "total_orders" in result.columns
        assert result.data[0]["total_orders"] == 10

    def test_query_simple_metric_with_filter(self, store_with_data: MetricStore):
        """Can query a simple metric with built-in filter."""
        result = store_with_data.query(metrics=["completed_orders"])

        assert result.row_count == 1
        assert result.data[0]["completed_orders"] == 7

    def test_query_revenue(self, store_with_data: MetricStore):
        """Can query revenue metric."""
        result = store_with_data.query(metrics=["revenue"])

        assert result.row_count == 1
        assert result.data[0]["revenue"] == 1275  # Sum of completed orders

    def test_query_with_dimensions(self, store_with_data: MetricStore):
        """Can query metrics with dimensions."""
        result = store_with_data.query(
            metrics=["total_orders"],
            dimensions=["country"],
        )

        assert result.row_count > 1
        assert "country" in result.columns
        assert "total_orders" in result.columns

    def test_query_with_filters(self, store_with_data: MetricStore):
        """Can query metrics with additional filters."""
        result = store_with_data.query(
            metrics=["total_orders"],
            filters=["country = 'US'"],
        )

        assert result.row_count == 1
        assert result.data[0]["total_orders"] == 6  # US orders from fixture

    def test_query_derived_metric(self, store_with_data: MetricStore):
        """Can query a derived metric."""
        result = store_with_data.query(metrics=["average_order_value"])

        assert result.row_count == 1
        # AOV = 1275 / 7 = ~182.14
        aov = result.data[0]["average_order_value"]
        assert abs(aov - 182.14) < 0.1

    def test_query_ratio_metric(self, store_with_data: MetricStore):
        """Can query a ratio metric."""
        result = store_with_data.query(metrics=["order_completion_rate"])

        assert result.row_count == 1
        # Completion rate = 7 / 10 = 0.7
        rate = result.data[0]["order_completion_rate"]
        assert abs(rate - 0.7) < 0.01

    def test_query_multiple_metrics(self, store_with_data: MetricStore):
        """Can query multiple metrics at once."""
        result = store_with_data.query(
            metrics=["revenue", "completed_orders", "average_order_value"]
        )

        assert result.row_count == 1
        assert "revenue" in result.columns
        assert "completed_orders" in result.columns
        assert "average_order_value" in result.columns

    def test_query_with_time_grain(self, store_with_data: MetricStore):
        """Can query with time grain."""
        result = store_with_data.query(
            metrics=["revenue"],
            dimensions=["order_date"],
            time_grain="month",
        )

        assert result.row_count > 0
        assert "order_date" in result.columns

    def test_query_with_date_range(self, store_with_data: MetricStore):
        """Can query with date range."""
        result = store_with_data.query(
            metrics=["total_orders"],
            dimensions=["order_date"],
            start_date="2024-01-01",
            end_date="2024-01-31",
        )

        # Only January orders
        assert result.row_count > 0

    def test_query_with_limit(self, store_with_data: MetricStore):
        """Can query with limit."""
        result = store_with_data.query(
            metrics=["total_orders"],
            dimensions=["country"],
            limit=2,
        )

        assert result.row_count <= 2

    def test_query_returns_sql(self, store_with_data: MetricStore):
        """Query result includes generated SQL."""
        result = store_with_data.query(metrics=["revenue"])

        assert "SELECT" in result.sql.upper()

    def test_query_tracks_execution_time(self, store_with_data: MetricStore):
        """Query result includes execution time."""
        result = store_with_data.query(metrics=["revenue"])

        assert result.execution_time_ms >= 0


class TestMetricStoreCumulative:
    def test_cumulative_is_a_running_total_by_month(self, store_with_data: MetricStore):
        """Each month carries the sum of every month up to and including it."""
        result = store_with_data.query(
            metrics=["cumulative_revenue"],
            dimensions=["order_date"],
            time_grain="month",
        )

        # Completed revenue per month: Jan 325, Feb 550, Mar 400
        totals = [row["cumulative_revenue"] for row in result.data]
        assert totals == [325, 875, 1275]

    def test_cumulative_restarts_per_partition(self, store_with_data: MetricStore):
        """Running totals are kept separately for each non-time dimension value."""
        result = store_with_data.query(
            metrics=["cumulative_revenue"],
            dimensions=["country", "order_date"],
            time_grain="month",
        )

        by_country: dict[str, list[float]] = {}
        for row in result.data:
            by_country.setdefault(row["country"], []).append(row["cumulative_revenue"])

        # US completed: Jan 100+75, Feb 125+250, Mar 400. UK completed: Jan 150, Feb 175.
        assert by_country["US"] == [175, 550, 950]
        assert by_country["UK"] == [150, 325]

    def test_cumulative_without_time_dimension_is_total(self, store_with_data: MetricStore):
        result = store_with_data.query(metrics=["cumulative_revenue"])

        assert result.data[0]["cumulative_revenue"] == 1275

    def test_grain_to_date_restarts_each_month(self, store_with_data: MetricStore):
        """A grain_to_date cumulative metric resets at every grain boundary."""
        result = store_with_data.query(
            metrics=["mtd_revenue"],
            dimensions=["order_date"],
            time_grain="day",
        )

        totals = [(str(row["order_date"])[:10], row["mtd_revenue"]) for row in result.data]
        # Completed orders only; pending/cancelled days still appear with the carried total
        assert totals == [
            ("2024-01-15", 100),
            ("2024-01-16", 250),
            ("2024-01-17", 250),
            ("2024-01-18", 325),
            ("2024-01-19", 325),
            ("2024-02-01", 125),
            ("2024-02-15", 300),
            ("2024-02-20", 550),
            ("2024-03-01", None),
            ("2024-03-15", 400),
        ]

    def test_trailing_window_by_month(self, store_with_data: MetricStore):
        """Each month carries its own revenue plus the month before it."""
        result = store_with_data.query(
            metrics=["revenue", "trailing_revenue"],
            dimensions=["order_date"],
            time_grain="month",
        )

        # Completed revenue per month: Jan 325, Feb 550, Mar 400
        rows = [(row["revenue"], row["trailing_revenue"]) for row in result.data]
        assert rows == [(325, 325), (550, 875), (400, 950)]

    def test_trailing_window_by_day_counts_calendar_time_not_rows(
        self, store_with_data: MetricStore
    ):
        """The window is two calendar months back from each day, whatever rows exist between."""
        result = store_with_data.query(metrics=["trailing_revenue"], dimensions=["order_date"])

        totals = {str(row["order_date"])[:10]: row["trailing_revenue"] for row in result.data}
        # 2024-03-15 reaches back to 2024-01-16, so the 100 on 01-15 has dropped out
        assert totals["2024-03-15"] == 1175
        # 2024-03-01 reaches back to 2024-01-02 and still holds every completed order so far
        assert totals["2024-03-01"] == 875


JOINED_YAML = """
semantic_models:
  - name: orders
    table: orders
    primary_entity: order_id
    measures:
      - {name: order_amount, agg: sum, expr: amount}
      - {name: order_count, agg: count, expr: order_id}
    dimensions:
      - {name: order_date, type: time}
      - {name: country, type: categorical}
      - {name: order_status, type: categorical, expr: status}
  - name: sessions
    table: sessions
    primary_entity: session_id
    measures:
      - {name: session_count, agg: count, expr: session_id}
    dimensions:
      - {name: session_date, type: time}
      - {name: country, type: categorical}
      - {name: device_type, type: categorical}
metrics:
  - name: revenue
    type: simple
    type_params: {measure: order_amount}
    filter: "status = 'completed'"
  - {name: total_orders, type: simple, type_params: {measure: order_count}}
  - {name: total_sessions, type: simple, type_params: {measure: session_count}}
  - name: revenue_per_session
    type: ratio
    type_params: {numerator: revenue, denominator: total_sessions}
  - name: us_revenue_per_session
    type: ratio
    filter: "country = 'US'"
    type_params: {numerator: revenue, denominator: total_sessions}
  - name: orders_per_session
    type: derived
    type_params: {expr: "total_orders / total_sessions", metrics: [total_orders, total_sessions]}
  - {name: running_revenue, type: cumulative, type_params: {measure: order_amount}}
"""


@pytest.fixture
def joined_store(tmp_path: Path) -> Generator[MetricStore, None, None]:
    """Orders and sessions on separate tables, sharing a country dimension."""
    store = _store_from(tmp_path, JOINED_YAML)
    conn = store.executor.conn
    conn.execute(
        "CREATE TABLE orders (order_id INT, amount DECIMAL(10, 2), status VARCHAR,"
        " country VARCHAR, order_date DATE)"
    )
    conn.executemany(
        "INSERT INTO orders VALUES (?, ?, ?, ?, ?)",
        [
            (1, 100, "completed", "US", "2024-01-15"),
            (2, 50, "pending", "US", "2024-01-20"),
            (3, 200, "completed", "UK", "2024-02-10"),
            (4, 300, "completed", "DE", "2024-02-11"),
        ],
    )
    conn.execute(
        "CREATE TABLE sessions (session_id INT, country VARCHAR, device_type VARCHAR,"
        " session_date DATE)"
    )
    conn.executemany(
        "INSERT INTO sessions VALUES (?, ?, ?, ?)",
        [
            (1, "US", "mobile", "2024-01-01"),
            (2, "US", "desktop", "2024-01-02"),
            (3, "UK", "mobile", "2024-02-01"),
            (4, "FR", "mobile", "2024-02-02"),
            (5, "US", "mobile", "2024-02-03"),
        ],
    )
    yield store
    store.close()


def _rows(result: QueryResult) -> list[tuple[object, ...]]:
    return [tuple(None if v is None else str(v) for v in row.values()) for row in result.data]


class TestMetricStoreMultiModel:
    def test_metrics_from_two_tables_by_shared_dimension(self, joined_store: MetricStore):
        """Each table is aggregated by country on its own; countries on one side only survive."""
        result = joined_store.query(
            metrics=["revenue", "total_sessions", "revenue_per_session"], dimensions=["country"]
        )

        assert _rows(result) == [
            ("DE", "300.00", None, None),
            ("FR", None, "1", None),
            ("UK", "200.00", "1", "200.0"),
            ("US", "100.00", "3", "33.333333333333336"),
        ]

    def test_derived_metric_across_tables_without_dimensions(self, joined_store: MetricStore):
        result = joined_store.query(metrics=["orders_per_session"])

        assert _rows(result) == [("0.8",)]  # 4 orders / 5 sessions

    def test_time_axis_aligns_differently_named_time_dimensions(self, joined_store: MetricStore):
        """order_date names the axis; sessions contributes session_date under that name."""
        result = joined_store.query(
            metrics=["revenue", "total_sessions"], dimensions=["order_date"], time_grain="month"
        )

        assert _rows(result) == [("2024-01-01", "100.00", "2"), ("2024-02-01", "500.00", "3")]

    def test_date_range_applies_to_every_table(self, joined_store: MetricStore):
        result = joined_store.query(
            metrics=["revenue", "total_sessions"], start_date="2024-02-01", end_date="2024-02-29"
        )

        assert _rows(result) == [("500.00", "3")]

    def test_shared_filter_applies_to_every_table(self, joined_store: MetricStore):
        result = joined_store.query(
            metrics=["revenue", "total_sessions"],
            dimensions=["country"],
            filters=["country IN ('US', 'UK')"],
        )

        assert _rows(result) == [("UK", "200.00", "1"), ("US", "100.00", "3")]

    def test_filter_on_a_column_one_table_lacks_is_refused(self, joined_store: MetricStore):
        with pytest.raises(MultiModelQueryError, match="'sessions' does not declare"):
            joined_store.get_sql(
                metrics=["revenue", "total_sessions"], filters=["status = 'completed'"]
            )

    def test_metric_filter_reaches_both_sides_of_a_ratio(self, joined_store: MetricStore):
        """A filtered ratio keeps its own aggregate columns next to the unfiltered ones."""
        result = joined_store.query(metrics=["revenue", "us_revenue_per_session"])

        assert _rows(result) == [("600.00", "33.333333333333336")]
        assert "revenue_2" in result.sql  # filtered revenue, distinct from plain revenue

    def test_cumulative_metric_inside_a_joined_query(self, joined_store: MetricStore):
        result = joined_store.query(
            metrics=["running_revenue", "total_sessions"],
            dimensions=["order_date"],
            time_grain="month",
        )

        assert _rows(result) == [("2024-01-01", "150.00", "2"), ("2024-02-01", "650.00", "3")]

    def test_order_by_and_limit_apply_to_the_joined_result(self, joined_store: MetricStore):
        result = joined_store.query(
            metrics=["revenue", "total_sessions"],
            dimensions=["country"],
            order_by=["revenue DESC NULLS LAST"],
            limit=2,
        )

        assert _rows(result) == [("DE", "300.00", None), ("UK", "200.00", "1")]

    def test_dimension_only_one_table_declares_is_refused(self, joined_store: MetricStore):
        with pytest.raises(MultiModelQueryError, match="'device_type' is not declared by 'orders'"):
            joined_store.get_sql(metrics=["revenue", "total_sessions"], dimensions=["device_type"])


EVENTS_YAML = """
semantic_models:
  - name: events
    table: events
    primary_entity: event_id
    entities:
      - name: event_id
        type: primary
    measures:
      - name: event_count
        agg: count
        expr: event_id
    dimensions:
      - name: event_at
        type: time
        expr: event_at

metrics:
  - name: events
    type: simple
    type_params:
      measure: event_count
"""

US_AOV_YAML = """
  - name: us_aov
    type: derived
    filter: "country = 'US'"
    type_params:
      expr: "revenue / completed_orders"
      metrics: [revenue, completed_orders]
"""

ORDERS_DDL = """
    CREATE TABLE orders (
        order_id INTEGER,
        customer_id INTEGER,
        amount DECIMAL(10, 2),
        status VARCHAR,
        country VARCHAR,
        order_date DATE
    )
"""


def _store_from(tmp_path: Path, yaml_text: str) -> MetricStore:
    metrics = tmp_path / "metrics"
    metrics.mkdir()
    (metrics / "m.yaml").write_text(yaml_text)
    return MetricStore(metrics)


def test_date_range_without_dimension_filters_rows(store_with_data: MetricStore):
    result = store_with_data.query(["revenue"], start_date="2024-01-01", end_date="2024-01-31")
    assert float(result.data[0]["revenue"]) == 325.0


def test_end_date_is_inclusive_on_timestamp_column(tmp_path: Path):
    store = _store_from(tmp_path, EVENTS_YAML)
    try:
        store.executor.conn.execute("CREATE TABLE events (event_id INTEGER, event_at TIMESTAMP)")
        store.executor.conn.execute(
            "INSERT INTO events VALUES (1, '2024-01-10 09:30:00'), (2, '2024-01-11 00:00:00')"
        )
        result = store.query(["events"], start_date="2024-01-10", end_date="2024-01-10")
        assert result.data[0]["events"] == 1
    finally:
        store.close()


def test_derived_metric_filter_is_applied(
    tmp_path: Path, sample_metrics_yaml: str, sample_orders_data: list[tuple]
):
    store = _store_from(tmp_path, sample_metrics_yaml + US_AOV_YAML)
    try:
        store.executor.conn.execute(ORDERS_DDL)
        store.executor.conn.executemany(
            "INSERT INTO orders VALUES (?, ?, ?, ?, ?, ?)", sample_orders_data
        )
        result = store.query(["us_aov"])
        assert float(result.data[0]["us_aov"]) == 190.0
    finally:
        store.close()


def test_validate_gives_windowed_metrics_a_time_axis(store_with_data: MetricStore) -> None:
    """A trailing window needs a time dimension, which validate supplies from its model."""
    assert store_with_data.validate() == []
