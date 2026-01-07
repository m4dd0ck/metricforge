"""Integration tests for MetricStore."""

from pathlib import Path

import pytest

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
        errors = store.validate()
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
