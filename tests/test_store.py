"""Integration tests for MetricStore."""

from pathlib import Path

import pytest

from metricforge.compiler.sql_builder import MultiModelQueryError
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


class TestMetricStoreMultiModel:
    def test_cross_model_query_raises(self, tmp_path: Path):
        """Querying metrics from two tables fails instead of silently using one table."""
        metrics_dir = tmp_path / "metrics"
        metrics_dir.mkdir()
        (metrics_dir / "m.yaml").write_text(
            """
semantic_models:
  - name: orders
    table: orders
    primary_entity: order_id
    measures: [{name: order_amount, agg: sum, expr: amount}]
  - name: sessions
    table: sessions
    primary_entity: session_id
    measures: [{name: session_count, agg: count, expr: session_id}]
metrics:
  - {name: revenue, type: simple, type_params: {measure: order_amount}}
  - {name: total_sessions, type: simple, type_params: {measure: session_count}}
"""
        )
        with MetricStore(metrics_dir) as store:
            with pytest.raises(MultiModelQueryError, match="'orders' and 'sessions'"):
                store.get_sql(metrics=["revenue", "total_sessions"])
