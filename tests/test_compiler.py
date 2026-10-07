"""Tests for SQL compiler."""

from pathlib import Path

import pytest

from metricforge.compiler.sql_builder import MultiModelQueryError, SQLCompiler
from metricforge.models.query import MetricQuery
from metricforge.parser.loader import MetricRegistry


class TestSQLCompiler:
    def test_compile_simple_metric(self, registry: MetricRegistry):
        """Compiles simple metric to SQL with aggregation."""
        compiler = SQLCompiler(registry)
        query = MetricQuery(metrics=["total_orders"])
        sql = compiler.compile(query)

        assert "COUNT" in sql.upper()
        assert "orders" in sql.lower()

    def test_compile_simple_metric_with_filter(self, registry: MetricRegistry):
        """Compiles simple metric with filter to SQL."""
        compiler = SQLCompiler(registry)
        query = MetricQuery(metrics=["revenue"])
        sql = compiler.compile(query)

        assert "SUM" in sql.upper()
        assert "CASE WHEN" in sql.upper() or "status" in sql.lower()

    def test_compile_derived_metric(self, registry: MetricRegistry):
        """Compiles derived metric to SQL."""
        compiler = SQLCompiler(registry)
        query = MetricQuery(metrics=["average_order_value"])
        sql = compiler.compile(query)

        # Should contain both component metrics' aggregations
        assert "SUM" in sql.upper()
        assert "COUNT" in sql.upper()
        # Should have division for derived calculation
        assert "/" in sql

    def test_compile_ratio_metric(self, registry: MetricRegistry):
        """Compiles ratio metric to SQL."""
        compiler = SQLCompiler(registry)
        query = MetricQuery(metrics=["order_completion_rate"])
        sql = compiler.compile(query)

        # Should have NULLIF to prevent division by zero
        assert "NULLIF" in sql.upper()
        assert "/" in sql

    def test_compile_cumulative_metric(self, registry: MetricRegistry):
        """Compiles cumulative metric to SQL."""
        compiler = SQLCompiler(registry)
        query = MetricQuery(metrics=["cumulative_revenue"])
        sql = compiler.compile(query)

        # Should use window function for cumulative
        assert "OVER" in sql.upper() or "SUM" in sql.upper()

    def test_compile_with_dimensions(self, registry: MetricRegistry):
        """Compiles query with dimensions to SQL."""
        compiler = SQLCompiler(registry)
        query = MetricQuery(
            metrics=["revenue"],
            dimensions=["country"],
        )
        sql = compiler.compile(query)

        assert "country" in sql.lower()
        assert "GROUP BY" in sql.upper()

    def test_compile_with_time_grain(self, registry: MetricRegistry):
        """Compiles query with time grain to SQL."""
        compiler = SQLCompiler(registry)
        query = MetricQuery(
            metrics=["revenue"],
            dimensions=["order_date"],
            time_grain="month",
        )
        sql = compiler.compile(query)

        assert "DATE_TRUNC" in sql.upper()
        assert "month" in sql.lower()

    def test_compile_with_filters(self, registry: MetricRegistry):
        """Compiles query with filters to SQL."""
        compiler = SQLCompiler(registry)
        query = MetricQuery(
            metrics=["total_orders"],
            filters=["country = 'US'"],
        )
        sql = compiler.compile(query)

        assert "WHERE" in sql.upper()
        assert "country" in sql.lower()

    def test_compile_with_date_range(self, registry: MetricRegistry):
        """Compiles query with date range to SQL."""
        compiler = SQLCompiler(registry)
        query = MetricQuery(
            metrics=["revenue"],
            dimensions=["order_date"],
            start_date="2024-01-01",
            end_date="2024-12-31",
        )
        sql = compiler.compile(query)

        assert "2024-01-01" in sql
        assert "2024-12-31" in sql

    def test_compile_with_limit(self, registry: MetricRegistry):
        """Compiles query with limit to SQL."""
        compiler = SQLCompiler(registry)
        query = MetricQuery(
            metrics=["revenue"],
            dimensions=["country"],
            limit=10,
        )
        sql = compiler.compile(query)

        assert "LIMIT" in sql.upper()
        assert "10" in sql

    def test_compile_multiple_metrics(self, registry: MetricRegistry):
        """Compiles query with multiple metrics to SQL."""
        compiler = SQLCompiler(registry)
        query = MetricQuery(metrics=["revenue", "total_orders"])
        sql = compiler.compile(query)

        # Should have both metrics
        assert "revenue" in sql.lower()
        assert "total_orders" in sql.lower()

    def test_compile_multiple_dimensions(self, registry: MetricRegistry):
        """Compiles query with multiple dimensions to SQL."""
        compiler = SQLCompiler(registry)
        query = MetricQuery(
            metrics=["revenue"],
            dimensions=["country", "order_status"],
        )
        sql = compiler.compile(query)

        assert "country" in sql.lower()
        assert "status" in sql.lower()  # order_status expr is 'status'
        assert "GROUP BY" in sql.upper()


class TestSQLCompilerFormat:
    def test_sql_is_formatted(self, registry: MetricRegistry):
        """Generated SQL is formatted nicely."""
        compiler = SQLCompiler(registry)
        query = MetricQuery(metrics=["revenue"])
        sql = compiler.compile(query)

        # Should have newlines (formatted)
        assert "\n" in sql

    def test_sql_uses_aliases(self, registry: MetricRegistry):
        """Generated SQL uses column aliases."""
        compiler = SQLCompiler(registry)
        query = MetricQuery(metrics=["revenue"])
        sql = compiler.compile(query)

        assert "AS revenue" in sql.lower() or "as revenue" in sql.lower()


TWO_MODEL_YAML = """
semantic_models:
  - name: orders
    table: orders
    primary_entity: order_id
    measures:
      - {name: order_amount, agg: sum, expr: amount}
    dimensions:
      - {name: order_date, type: time}
      - {name: country, type: categorical}
  - name: sessions
    table: sessions
    primary_entity: session_id
    measures:
      - {name: session_count, agg: count, expr: session_id}
    dimensions:
      - {name: session_date, type: time}
      - {name: traffic_source, type: categorical}
  - name: completed_orders_view
    table: orders
    primary_entity: order_id
    measures:
      - {name: completed_amount, agg: sum, expr: amount}
metrics:
  - {name: revenue, type: simple, type_params: {measure: order_amount}}
  - {name: total_sessions, type: simple, type_params: {measure: session_count}}
  - {name: completed_revenue, type: simple, type_params: {measure: completed_amount}}
  - name: revenue_per_session
    type: ratio
    type_params: {numerator: revenue, denominator: total_sessions}
"""


@pytest.fixture
def two_model_registry(tmp_path: Path) -> MetricRegistry:
    metrics_dir = tmp_path / "metrics"
    metrics_dir.mkdir()
    (metrics_dir / "two_models.yaml").write_text(TWO_MODEL_YAML)
    registry = MetricRegistry()
    registry.load_directory(metrics_dir)
    return registry


class TestSQLCompilerMultiModel:
    def test_metrics_on_two_models_raise(self, two_model_registry: MetricRegistry):
        """Metrics from different tables cannot be compiled into one query."""
        compiler = SQLCompiler(two_model_registry)
        query = MetricQuery(metrics=["revenue", "total_sessions"])

        with pytest.raises(MultiModelQueryError, match="'orders'.*'sessions'"):
            compiler.compile(query)

    def test_dimension_from_other_model_raises(self, two_model_registry: MetricRegistry):
        """A dimension that lives on a different table than the metric is refused."""
        compiler = SQLCompiler(two_model_registry)
        query = MetricQuery(metrics=["revenue"], dimensions=["traffic_source"])

        with pytest.raises(MultiModelQueryError, match="'orders'.*'sessions'"):
            compiler.compile(query)

    def test_ratio_spanning_two_models_raises(self, two_model_registry: MetricRegistry):
        """Ratio and derived metrics whose inputs span tables are refused."""
        compiler = SQLCompiler(two_model_registry)
        query = MetricQuery(metrics=["revenue_per_session"])

        with pytest.raises(MultiModelQueryError, match="revenue_per_session"):
            compiler.compile(query)

    def test_models_sharing_a_table_compile(self, two_model_registry: MetricRegistry):
        """Two semantic models over the same table are still one FROM clause."""
        compiler = SQLCompiler(two_model_registry)
        query = MetricQuery(metrics=["revenue", "completed_revenue"])
        sql = compiler.compile(query)

        assert sql.upper().count("FROM") == 1
        assert "orders" in sql.lower()

    def test_error_is_a_value_error(self, two_model_registry: MetricRegistry):
        """Existing callers that catch ValueError keep working."""
        compiler = SQLCompiler(two_model_registry)
        with pytest.raises(ValueError):
            compiler.compile(MetricQuery(metrics=["revenue", "total_sessions"]))


class TestSQLCompilerCumulative:
    def test_cumulative_orders_by_time_dimension(self, registry: MetricRegistry):
        """Cumulative metrics use a running-total frame ordered by the time dimension."""
        compiler = SQLCompiler(registry)
        query = MetricQuery(
            metrics=["cumulative_revenue"],
            dimensions=["order_date"],
            time_grain="month",
        )
        sql = compiler.compile(query)

        assert "ORDER BY 1" not in sql
        assert "ORDER BY DATE_TRUNC('MONTH', order_date)" in sql
        assert "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW" in sql

    def test_cumulative_partitions_by_other_dimensions(self, registry: MetricRegistry):
        """Non-time group-by dimensions partition the running total."""
        compiler = SQLCompiler(registry)
        query = MetricQuery(
            metrics=["cumulative_revenue"],
            dimensions=["country", "order_date"],
        )
        sql = compiler.compile(query)

        assert "PARTITION BY country" in sql
        assert "ORDER BY order_date" in sql

    def test_cumulative_without_time_dimension_is_plain_total(self, registry: MetricRegistry):
        """With no time axis the running total collapses to the plain aggregate."""
        compiler = SQLCompiler(registry)
        sql = compiler.compile(MetricQuery(metrics=["cumulative_revenue"]))

        assert "OVER" not in sql.upper()
        assert "SUM" in sql.upper()
