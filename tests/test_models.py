"""Tests for Pydantic models."""

import pytest
from pydantic import ValidationError

from metricforge.models.metric import (
    CumulativeMetricParams,
    DerivedMetricParams,
    Metric,
    RatioMetricParams,
    SimpleMetricParams,
    parse_window,
)
from metricforge.models.query import MetricQuery, QueryResult
from metricforge.models.semantic_model import (
    AggregationType,
    Dimension,
    DimensionType,
    Entity,
    EntityType,
    Measure,
    SemanticModel,
    TimeGranularity,
)


class TestSemanticModel:
    def test_create_semantic_model(self):
        """Can create a valid semantic model."""
        model = SemanticModel(
            name="orders",
            table="orders",
            primary_entity="order_id",
            entities=[Entity(name="order_id", type=EntityType.PRIMARY)],
            measures=[Measure(name="amount", agg=AggregationType.SUM, expr="amount")],
            dimensions=[Dimension(name="country", type=DimensionType.CATEGORICAL)],
        )
        assert model.name == "orders"
        assert model.get_table_name() == "orders"

    def test_get_table_name_with_ref(self):
        """Extract table name from ref() syntax."""
        model = SemanticModel(
            name="orders",
            table="ref('stg_orders')",
            primary_entity="order_id",
        )
        assert model.get_table_name() == "stg_orders"

    def test_get_measure(self):
        """Can retrieve measure by name."""
        model = SemanticModel(
            name="orders",
            table="orders",
            primary_entity="order_id",
            measures=[
                Measure(name="amount", agg=AggregationType.SUM, expr="amount"),
                Measure(name="count", agg=AggregationType.COUNT, expr="order_id"),
            ],
        )
        measure = model.get_measure("amount")
        assert measure is not None
        assert measure.agg == AggregationType.SUM

    def test_get_measure_not_found(self):
        """Returns None for unknown measure."""
        model = SemanticModel(name="orders", table="orders", primary_entity="order_id")
        assert model.get_measure("unknown") is None


class TestMeasure:
    def test_create_measure(self):
        """Can create a valid measure."""
        measure = Measure(name="revenue", agg=AggregationType.SUM, expr="amount")
        assert measure.name == "revenue"
        assert measure.agg == AggregationType.SUM

    def test_all_aggregation_types(self):
        """All aggregation types are valid."""
        for agg_type in AggregationType:
            measure = Measure(name="test", agg=agg_type, expr="col")
            assert measure.agg == agg_type


class TestDimension:
    def test_create_categorical_dimension(self):
        """Can create a categorical dimension."""
        dim = Dimension(name="country", type=DimensionType.CATEGORICAL)
        assert dim.type == DimensionType.CATEGORICAL
        assert dim.time_granularity is None

    def test_create_time_dimension(self):
        """Can create a time dimension."""
        dim = Dimension(
            name="order_date",
            type=DimensionType.TIME,
            time_granularity=TimeGranularity.DAY,
        )
        assert dim.type == DimensionType.TIME
        assert dim.time_granularity == TimeGranularity.DAY


class TestMetric:
    def test_create_simple_metric(self):
        """Can create a simple metric."""
        metric = Metric(
            name="revenue",
            type="simple",
            type_params=SimpleMetricParams(measure="order_amount"),
        )
        assert metric.type == "simple"
        assert isinstance(metric.type_params, SimpleMetricParams)

    def test_create_derived_metric(self):
        """Can create a derived metric."""
        metric = Metric(
            name="aov",
            type="derived",
            type_params=DerivedMetricParams(
                expr="revenue / order_count",
                metrics=["revenue", "order_count"],
            ),
        )
        assert metric.type == "derived"
        assert isinstance(metric.type_params, DerivedMetricParams)

    def test_create_ratio_metric(self):
        """Can create a ratio metric."""
        metric = Metric(
            name="completion_rate",
            type="ratio",
            type_params=RatioMetricParams(
                numerator="completed_orders",
                denominator="total_orders",
            ),
        )
        assert metric.type == "ratio"
        assert isinstance(metric.type_params, RatioMetricParams)

    def test_create_cumulative_metric(self):
        """Can create a cumulative metric."""
        metric = Metric(
            name="cumulative_revenue",
            type="cumulative",
            type_params=CumulativeMetricParams(measure="order_amount"),
        )
        assert metric.type == "cumulative"
        assert isinstance(metric.type_params, CumulativeMetricParams)

    def test_invalid_type_params_raises_error(self):
        """Wrong type_params for metric type raises validation error."""
        with pytest.raises(ValidationError):
            Metric(
                name="revenue",
                type="simple",
                type_params=DerivedMetricParams(expr="x / y", metrics=["x", "y"]),
            )


class TestMetricQuery:
    def test_create_basic_query(self):
        """Can create a basic metric query."""
        query = MetricQuery(metrics=["revenue"])
        assert query.metrics == ["revenue"]
        assert query.dimensions == []
        assert query.filters == []

    def test_create_full_query(self):
        """Can create a query with all options."""
        query = MetricQuery(
            metrics=["revenue", "orders"],
            dimensions=["country", "order_date"],
            filters=["status = 'completed'"],
            time_grain="month",
            start_date="2024-01-01",
            end_date="2024-12-31",
            limit=100,
        )
        assert len(query.metrics) == 2
        assert len(query.dimensions) == 2
        assert query.time_grain == "month"


class TestQueryResult:
    def test_create_query_result(self):
        """Can create a query result."""
        result = QueryResult(
            sql="SELECT * FROM test",
            columns=["revenue"],
            data=[{"revenue": 1000}],
            row_count=1,
            execution_time_ms=10.5,
        )
        assert result.row_count == 1
        assert result.columns == ["revenue"]


class TestCumulativeWindow:
    def _metric(self, **params: str) -> Metric:
        return Metric(
            name="revenue_window",
            type="cumulative",
            type_params=CumulativeMetricParams(measure="order_amount", **params),
        )

    @pytest.mark.parametrize(
        ("text", "expected"),
        [("7 days", (7, "day")), ("1 month", (1, "month")), ("  2 Quarters ", (2, "quarter"))],
    )
    def test_parse_window(self, text: str, expected: tuple[int, str]):
        assert parse_window(text) == expected

    @pytest.mark.parametrize("text", ["days", "7", "7 fortnights", "0 days", "-1 day", "7d"])
    def test_malformed_window_is_rejected(self, text: str):
        with pytest.raises(ValueError, match="window"):
            self._metric(window=text)

    def test_window_and_grain_to_date_are_exclusive(self):
        with pytest.raises(ValueError, match="not both"):
            self._metric(window="7 days", grain_to_date="month")

    def test_unknown_grain_to_date_is_rejected(self):
        with pytest.raises(ValueError, match="grain_to_date"):
            self._metric(grain_to_date="fortnight")

    def test_valid_window_is_kept(self):
        assert self._metric(window="7 days").type_params.window == "7 days"
