"""Tests for SQL compiler."""

from pathlib import Path

import pytest

from metricforge.compiler.sql_builder import (
    MultiModelQueryError,
    NoTimeDimensionError,
    SQLCompiler,
    WindowGrainError,
)
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
    def test_metrics_on_two_models_are_joined(self, two_model_registry: MetricRegistry):
        """Metrics from different tables become one aggregate per model, joined afterwards."""
        compiler = SQLCompiler(two_model_registry)
        sql = compiler.compile(MetricQuery(metrics=["revenue", "total_sessions"]))

        assert "WITH orders_q AS" in sql
        assert "sessions_q AS" in sql
        assert "CROSS JOIN sessions_q" in sql  # no dimensions: one row each side
        assert "orders_q.revenue AS revenue" in sql
        assert "sessions_q.total_sessions AS total_sessions" in sql

    def test_dimensions_key_a_full_outer_join(self, two_model_registry: MetricRegistry):
        """Each model groups by the dimension and the outer query joins on it."""
        compiler = SQLCompiler(two_model_registry)
        query = MetricQuery(
            metrics=["revenue", "total_sessions"], dimensions=["order_date"], time_grain="month"
        )
        sql = " ".join(compiler.compile(query).split())

        assert "FULL OUTER JOIN sessions_q ON orders_q.order_date IS NOT DISTINCT FROM" in sql
        assert "COALESCE(orders_q.order_date, sessions_q.order_date) AS order_date" in sql

    def test_time_axis_falls_back_to_each_models_own_time_dimension(
        self, two_model_registry: MetricRegistry
    ):
        """sessions has no order_date, so its CTE buckets session_date under that name."""
        compiler = SQLCompiler(two_model_registry)
        query = MetricQuery(
            metrics=["revenue", "total_sessions"], dimensions=["order_date"], time_grain="month"
        )
        sql = compiler.compile(query)

        assert "DATE_TRUNC('MONTH', order_date) AS order_date" in sql
        assert "DATE_TRUNC('MONTH', session_date) AS order_date" in sql

    def test_ratio_spanning_two_models_divides_the_joined_columns(
        self, two_model_registry: MetricRegistry
    ):
        compiler = SQLCompiler(two_model_registry)
        sql = " ".join(compiler.compile(MetricQuery(metrics=["revenue_per_session"])).split())

        assert "( orders_q.revenue ) * 1.0 / NULLIF(( sessions_q.total_sessions ), 0)" in sql

    def test_dimension_from_other_model_raises(self, two_model_registry: MetricRegistry):
        """A dimension that lives on a different table than the metric is refused."""
        compiler = SQLCompiler(two_model_registry)
        query = MetricQuery(metrics=["revenue"], dimensions=["traffic_source"])

        with pytest.raises(MultiModelQueryError, match="'orders'.*'sessions'"):
            compiler.compile(query)

    def test_categorical_dimension_must_be_on_every_joined_model(
        self, two_model_registry: MetricRegistry
    ):
        compiler = SQLCompiler(two_model_registry)
        query = MetricQuery(metrics=["revenue", "total_sessions"], dimensions=["traffic_source"])

        with pytest.raises(MultiModelQueryError, match="not declared by 'orders'"):
            compiler.compile(query)

    def test_unknown_dimension_cannot_key_a_join(self, two_model_registry: MetricRegistry):
        compiler = SQLCompiler(two_model_registry)
        query = MetricQuery(metrics=["revenue", "total_sessions"], dimensions=["nope"])

        with pytest.raises(MultiModelQueryError, match="not declared by any semantic model"):
            compiler.compile(query)

    def test_joined_model_without_time_dimension_cannot_align(
        self, two_model_registry: MetricRegistry
    ):
        compiler = SQLCompiler(two_model_registry)
        query = MetricQuery(
            metrics=["completed_revenue", "total_sessions"], dimensions=["session_date"]
        )

        with pytest.raises(NoTimeDimensionError, match="'completed_orders_view' has no time"):
            compiler.compile(query)

    def test_filter_must_apply_to_every_joined_model(self, two_model_registry: MetricRegistry):
        compiler = SQLCompiler(two_model_registry)
        query = MetricQuery(metrics=["revenue", "total_sessions"], filters=["country = 'US'"])

        with pytest.raises(MultiModelQueryError, match="'country'.*'sessions' does not declare"):
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
            compiler.compile(MetricQuery(metrics=["revenue"], dimensions=["traffic_source"]))


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


class TestSQLCompilerTrailingWindow:
    def test_window_is_a_range_frame_ending_at_the_current_bucket(self, registry: MetricRegistry):
        """A 2-month window at month grain reaches one month back from the current bucket."""
        compiler = SQLCompiler(registry)
        query = MetricQuery(
            metrics=["trailing_revenue"], dimensions=["order_date"], time_grain="month"
        )
        sql = " ".join(compiler.compile(query).split())

        assert "ORDER BY DATE_TRUNC('MONTH', order_date) RANGE BETWEEN" in sql
        assert "INTERVAL '2' MONTH - INTERVAL '1' MONTH ) PRECEDING AND CURRENT ROW" in sql
        assert "UNBOUNDED" not in sql

    def test_window_at_finer_grain_subtracts_one_bucket(self, registry: MetricRegistry):
        """Without a query grain the dimension's own day granularity is the bucket."""
        compiler = SQLCompiler(registry)
        sql = " ".join(
            compiler.compile(
                MetricQuery(metrics=["trailing_revenue"], dimensions=["order_date"])
            ).split()
        )

        assert "INTERVAL '2' MONTH - INTERVAL '1' DAY" in sql

    def test_window_finer_than_grain_is_refused(self, registry: MetricRegistry):
        """A 2-month window cannot be measured along quarterly buckets."""
        compiler = SQLCompiler(registry)
        query = MetricQuery(
            metrics=["trailing_revenue"], dimensions=["order_date"], time_grain="quarter"
        )

        with pytest.raises(WindowGrainError, match="finer than the query's 'quarter' grain"):
            compiler.compile(query)

    def test_window_without_time_dimension_is_refused(self, registry: MetricRegistry):
        """Unlike a running total, a trailing window has no meaning without a time axis."""
        compiler = SQLCompiler(registry)

        with pytest.raises(NoTimeDimensionError, match="trailing window"):
            compiler.compile(MetricQuery(metrics=["trailing_revenue"]))

    def test_window_keeps_partitions(self, registry: MetricRegistry):
        compiler = SQLCompiler(registry)
        query = MetricQuery(metrics=["trailing_revenue"], dimensions=["country", "order_date"])
        sql = compiler.compile(query)

        assert "PARTITION BY country" in sql
        assert "RANGE BETWEEN" in sql


OVERLAP_YAML = """
semantic_models:
  - name: orders
    table: orders
    primary_entity: order_id
    entities:
      - name: order_id
        type: primary
    measures:
      - name: order_count
        agg: count
        expr: order_id
      - name: order_amount
        agg: sum
        expr: amount
    dimensions:
      - name: order_date
        type: time
        expr: order_date
      - name: country
        type: categorical

metrics:
  - name: orders
    type: simple
    type_params:
      measure: order_count
  - name: completed_orders
    type: simple
    type_params:
      measure: order_count
    filter: "status = 'completed'"
  - name: revenue
    type: simple
    type_params:
      measure: order_amount
  - name: amount
    type: simple
    type_params:
      measure: order_amount
  - name: completion_share
    type: derived
    type_params:
      expr: "completed_orders / orders"
      metrics: [orders, completed_orders]
  - name: rev_per_amount
    type: derived
    type_params:
      expr: "revenue / amount"
      metrics: [revenue, amount]
"""

NO_TIME_YAML = """
semantic_models:
  - name: products
    table: products
    primary_entity: product_id
    entities:
      - name: product_id
        type: primary
    measures:
      - name: product_count
        agg: count
        expr: product_id
    dimensions:
      - name: category
        type: categorical

metrics:
  - name: products
    type: simple
    type_params:
      measure: product_count
"""


def _registry_from(tmp_path: Path, yaml_text: str) -> MetricRegistry:
    metrics = tmp_path / "metrics"
    metrics.mkdir()
    (metrics / "m.yaml").write_text(yaml_text)
    reg = MetricRegistry()
    reg.load_directory(metrics)
    return reg


class TestCorrectness:
    def test_derived_substitution_respects_word_boundaries(self, tmp_path: Path):
        compiler = SQLCompiler(_registry_from(tmp_path, OVERLAP_YAML))
        sql = compiler.compile(MetricQuery(metrics=["completion_share"])).lower()
        assert "completed_(" not in sql
        assert sql.count("count(") == 2

    def test_derived_substitution_does_not_recurse_into_substituted_sql(self, tmp_path: Path):
        compiler = SQLCompiler(_registry_from(tmp_path, OVERLAP_YAML))
        sql = compiler.compile(MetricQuery(metrics=["rev_per_amount"])).upper().replace(" ", "")
        assert "SUM(SUM" not in sql
        assert sql.count("SUM(") == 2

    def test_date_range_uses_model_time_dimension_when_query_has_none(
        self, registry: MetricRegistry
    ):
        compiler = SQLCompiler(registry)
        query = MetricQuery(metrics=["revenue"], start_date="2024-01-01", end_date="2024-01-31")
        sql = compiler.compile(query)
        assert "WHERE" in sql.upper()
        assert "order_date" in sql and "2024-01-01" in sql and "2024-01-31" in sql

    def test_date_range_without_any_time_dimension_raises(self, tmp_path: Path):
        compiler = SQLCompiler(_registry_from(tmp_path, NO_TIME_YAML))
        query = MetricQuery(metrics=["products"], start_date="2024-01-01")
        with pytest.raises(NoTimeDimensionError, match="no time dimension"):
            compiler.compile(query)

    def test_limit_zero_is_emitted(self, registry: MetricRegistry):
        sql = SQLCompiler(registry).compile(MetricQuery(metrics=["revenue"], limit=0))
        assert "LIMIT 0" in sql.upper()
