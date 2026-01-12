"""Tests for SQL compiler."""


from metricforge.compiler.sql_builder import SQLCompiler
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
