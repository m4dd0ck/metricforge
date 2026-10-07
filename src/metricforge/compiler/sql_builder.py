"""SQL compiler for metric queries."""

from dataclasses import dataclass, field

import sqlglot

from metricforge.models.metric import (
    CumulativeMetricParams,
    DerivedMetricParams,
    Metric,
    RatioMetricParams,
    SimpleMetricParams,
)
from metricforge.models.query import MetricQuery
from metricforge.models.semantic_model import (
    AggregationType,
    DimensionType,
    Measure,
    SemanticModel,
)
from metricforge.parser.loader import MetricRegistry


class MultiModelQueryError(ValueError):
    """Raised when one query needs metrics or dimensions from more than one table."""


@dataclass
class ResolvedMetric:
    """A metric resolved to its SQL components."""

    name: str
    expr: str  # the sql expression for this metric
    model: SemanticModel | None  # which model this comes from (for table lookup)
    filter: str | None = None
    is_aggregate: bool = True  # false for derived/ratio which wrap aggregates
    sub_metrics: dict[str, "ResolvedMetric"] = field(default_factory=dict)


class SQLCompiler:
    """Compiles metric queries into SQL."""

    AGG_MAP = {
        AggregationType.SUM: "SUM",
        AggregationType.COUNT: "COUNT",
        AggregationType.COUNT_DISTINCT: "COUNT",  # will add DISTINCT keyword separately
        AggregationType.AVG: "AVG",
        AggregationType.MIN: "MIN",
        AggregationType.MAX: "MAX",
    }

    def __init__(self, registry: MetricRegistry, dialect: str = "duckdb") -> None:
        self.registry = registry
        self.dialect = dialect  # passed to sqlglot for formatting

    def compile(self, query: MetricQuery) -> str:
        """Convert a MetricQuery into SQL."""
        resolved_metrics = self._resolve_metrics(query)
        models = self._get_required_models(resolved_metrics, query.dimensions)

        select_exprs = self._build_select_exprs(resolved_metrics, query)
        from_clause = self._build_from_clause(models, query)
        where_conditions = self._build_where_conditions(resolved_metrics, query)
        group_by_exprs = self._build_group_by_exprs(query)
        order_by_exprs = self._build_order_by_exprs(query)

        sql = self._assemble_query(
            select_exprs=select_exprs,
            from_clause=from_clause,
            where_conditions=where_conditions,
            group_by_exprs=group_by_exprs,
            order_by_exprs=order_by_exprs,
            limit=query.limit,
        )

        return self._format_sql(sql)

    def _resolve_metrics(self, query: MetricQuery) -> dict[str, ResolvedMetric]:
        resolved = {}
        for name in query.metrics:
            metric = self.registry.get_metric(name)
            resolved[name] = self._resolve_single_metric(metric, query)
        return resolved

    def _resolve_single_metric(self, metric: Metric, query: MetricQuery) -> ResolvedMetric:
        if metric.type == "simple":
            return self._resolve_simple_metric(metric)
        elif metric.type == "derived":
            return self._resolve_derived_metric(metric, query)
        elif metric.type == "ratio":
            return self._resolve_ratio_metric(metric, query)
        elif metric.type == "cumulative":
            return self._resolve_cumulative_metric(metric, query)
        else:
            raise ValueError(f"Unknown metric type: {metric.type}")

    def _resolve_simple_metric(self, metric: Metric) -> ResolvedMetric:
        params: SimpleMetricParams = metric.type_params  # type: ignore
        model = self.registry.get_model_for_measure(params.measure)
        measure = self.registry.get_measure(params.measure)

        expr = self._build_measure_expr(measure, metric.filter)

        return ResolvedMetric(
            name=metric.name,
            expr=expr,
            model=model,
            filter=metric.filter,
        )

    def _resolve_derived_metric(self, metric: Metric, query: MetricQuery) -> ResolvedMetric:
        """Resolve a derived metric to SQL."""
        params: DerivedMetricParams = metric.type_params  # type: ignore

        sub_metrics: dict[str, ResolvedMetric] = {}
        for ref_name in params.metrics:
            ref_metric = self.registry.get_metric(ref_name)
            sub_metrics[ref_name] = self._resolve_single_metric(ref_metric, query)

        # TODO: use proper expression parsing for robustness
        expr = params.expr
        for ref_name, resolved in sub_metrics.items():
            sub_expr = resolved.expr
            expr = expr.replace(ref_name, f"({sub_expr})")

        model = next(iter(sub_metrics.values())).model if sub_metrics else None

        return ResolvedMetric(
            name=metric.name,
            expr=expr,
            model=model,
            filter=metric.filter,
            is_aggregate=False,  # derived metrics wrap aggregates, not aggregates themselves
            sub_metrics=sub_metrics,
        )

    def _resolve_ratio_metric(self, metric: Metric, query: MetricQuery) -> ResolvedMetric:
        """Resolve a ratio metric to SQL."""
        params: RatioMetricParams = metric.type_params  # type: ignore

        numerator_metric = self.registry.get_metric(params.numerator)
        denominator_metric = self.registry.get_metric(params.denominator)

        num_resolved = self._resolve_single_metric(numerator_metric, query)
        denom_resolved = self._resolve_single_metric(denominator_metric, query)

        # NULLIF for division-by-zero, 1.0 for float division
        expr = f"({num_resolved.expr}) * 1.0 / NULLIF({denom_resolved.expr}, 0)"

        return ResolvedMetric(
            name=metric.name,
            expr=expr,
            model=num_resolved.model,
            filter=metric.filter,
            is_aggregate=False,
            sub_metrics={params.numerator: num_resolved, params.denominator: denom_resolved},
        )

    def _resolve_cumulative_metric(self, metric: Metric, query: MetricQuery) -> ResolvedMetric:
        """Resolve a cumulative metric to a running total over the query's time dimension."""
        params: CumulativeMetricParams = metric.type_params  # type: ignore
        model = self.registry.get_model_for_measure(params.measure)
        measure = self.registry.get_measure(params.measure)

        base_expr = self._build_measure_expr(measure, metric.filter)
        window = self._build_cumulative_window(query, params.grain_to_date)
        # Reason: without a time axis there is one bucket, so the running total is the total
        expr = f"SUM({base_expr}) {window}" if window else base_expr

        return ResolvedMetric(
            name=metric.name,
            expr=expr,
            model=model,
            filter=metric.filter,
            is_aggregate=False,  # window function, handled differently than aggregates
        )

    def _build_cumulative_window(self, query: MetricQuery, grain_to_date: str | None) -> str | None:
        """Build the OVER clause for a running total, or None if the query has no time axis."""
        time_dim = self._find_time_dimension(query.dimensions)
        if time_dim is None:
            return None

        order_expr = self._build_dimension_expr(time_dim, query.time_grain)
        partitions = [
            self._build_dimension_expr(dim, query.time_grain)
            for dim in query.dimensions
            if dim != time_dim
        ]
        if grain_to_date:
            # Reason: restarting at each grain boundary turns the running total into
            # a to-date total (e.g. month-to-date); truncating the group key keeps it valid SQL
            partitions.append(f"DATE_TRUNC('{grain_to_date}', {order_expr})")

        clauses = []
        if partitions:
            clauses.append(f"PARTITION BY {', '.join(partitions)}")
        clauses.append(f"ORDER BY {order_expr} ROWS UNBOUNDED PRECEDING")
        return f"OVER ({' '.join(clauses)})"

    def _build_measure_expr(self, measure: Measure, filter_expr: str | None = None) -> str:
        """Build SQL expression for a measure with optional filter."""
        agg_func = self.AGG_MAP[measure.agg]
        col_expr = measure.expr

        if measure.agg == AggregationType.COUNT_DISTINCT:
            if filter_expr:
                # filter applied via CASE WHEN - nulls don't get counted
                return f"{agg_func}(DISTINCT CASE WHEN {filter_expr} THEN {col_expr} END)"
            return f"{agg_func}(DISTINCT {col_expr})"

        if filter_expr:
            return f"{agg_func}(CASE WHEN {filter_expr} THEN {col_expr} END)"

        return f"{agg_func}({col_expr})"

    def _get_required_models(
        self, resolved_metrics: dict[str, ResolvedMetric], dimensions: list[str]
    ) -> list[SemanticModel]:
        """Get the semantic models the query touches, in first-seen order."""
        models: dict[str, SemanticModel] = {}

        def add(model: SemanticModel | None) -> None:
            if model is not None:
                models.setdefault(model.name, model)

        for resolved in resolved_metrics.values():
            add(resolved.model)
            # also check sub-metrics for derived/ratio
            for sub in resolved.sub_metrics.values():
                add(sub.model)

        for dim_name in dimensions:
            try:
                add(self.registry.get_model_for_dimension(dim_name))
            except KeyError:
                pass  # dimension might be in a filter, not a semantic model

        return list(models.values())

    def _build_select_exprs(
        self, resolved_metrics: dict[str, ResolvedMetric], query: MetricQuery
    ) -> list[str]:
        exprs = []

        for dim_name in query.dimensions:
            dim_expr = self._build_dimension_expr(dim_name, query.time_grain)
            exprs.append(f"{dim_expr} AS {dim_name}")

        for metric_name, resolved in resolved_metrics.items():
            exprs.append(f"{resolved.expr} AS {metric_name}")

        return exprs

    def _build_dimension_expr(self, dim_name: str, time_grain: str | None) -> str:
        """Build SQL expression for a dimension."""
        try:
            dimension = self.registry.get_dimension(dim_name)
            col_expr = dimension.expr or dim_name

            # time dimensions can be truncated to different grains
            if dimension.type == DimensionType.TIME and time_grain:
                return f"DATE_TRUNC('{time_grain}', {col_expr})"

            return col_expr
        except KeyError:
            return dim_name

    def _build_from_clause(self, models: list[SemanticModel], query: MetricQuery) -> str:
        """Build the FROM clause; every model in the query must map to the same table.

        Raises:
            MultiModelQueryError: If the metrics or dimensions span more than one table.
        """
        tables = list(dict.fromkeys(model.get_table_name() for model in models))
        if not tables:
            raise ValueError("No tables found for query")
        if len(tables) > 1:
            model_names = [f"'{model.name}'" for model in models]
            requested = ", ".join(f"'{name}'" for name in query.metrics)
            if query.dimensions:
                requested += " by " + ", ".join(f"'{name}'" for name in query.dimensions)
            raise MultiModelQueryError(
                f"Query for {requested} spans semantic models "
                f"{', '.join(model_names[:-1])} and {model_names[-1]}. "
                "MetricForge compiles single-table queries, so query one model at a time."
            )

        return tables[0]

    def _build_where_conditions(
        self, resolved_metrics: dict[str, ResolvedMetric], query: MetricQuery
    ) -> list[str]:
        """Build WHERE conditions."""
        conditions = list(query.filters)

        if query.start_date or query.end_date:
            time_dim = self._find_time_dimension(query.dimensions)
            if time_dim:
                dim = self.registry.get_dimension(time_dim)
                col_expr = dim.expr or time_dim

                if query.start_date:
                    conditions.append(f"{col_expr} >= '{query.start_date}'")
                if query.end_date:
                    conditions.append(f"{col_expr} <= '{query.end_date}'")

        return conditions

    def _find_time_dimension(self, dimensions: list[str]) -> str | None:
        for dim_name in dimensions:
            try:
                dim = self.registry.get_dimension(dim_name)
                if dim.type == DimensionType.TIME:
                    return dim_name
            except KeyError:
                pass
        return None

    def _build_group_by_exprs(self, query: MetricQuery) -> list[str]:
        if not query.dimensions:
            return []

        exprs = []
        for dim_name in query.dimensions:
            dim_expr = self._build_dimension_expr(dim_name, query.time_grain)
            exprs.append(dim_expr)

        return exprs

    def _build_order_by_exprs(self, query: MetricQuery) -> list[str]:
        if query.order_by:
            return query.order_by

        if query.dimensions:
            return [self._build_dimension_expr(d, query.time_grain) for d in query.dimensions]

        return []

    def _assemble_query(
        self,
        select_exprs: list[str],
        from_clause: str,
        where_conditions: list[str],
        group_by_exprs: list[str],
        order_by_exprs: list[str],
        limit: int | None,
    ) -> str:
        """Assemble the final SQL query."""
        parts = [f"SELECT\n  {',\n  '.join(select_exprs)}"]
        parts.append(f"FROM {from_clause}")

        if where_conditions:
            parts.append(f"WHERE {' AND '.join(where_conditions)}")

        if group_by_exprs:
            parts.append(f"GROUP BY {', '.join(group_by_exprs)}")

        if order_by_exprs:
            parts.append(f"ORDER BY {', '.join(order_by_exprs)}")

        if limit:
            parts.append(f"LIMIT {limit}")

        return "\n".join(parts)

    def _format_sql(self, sql: str) -> str:
        """Format SQL using SQLGlot."""
        try:
            parsed = sqlglot.parse_one(sql, dialect=self.dialect)
            return parsed.sql(dialect=self.dialect, pretty=True)
        except Exception:
            return sql
