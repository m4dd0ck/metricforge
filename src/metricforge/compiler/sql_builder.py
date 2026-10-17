"""SQL compiler for metric queries."""

import sqlglot

from metricforge.compiler.resolved import (
    MultiModelQueryError,
    NoTimeDimensionError,
    ResolvedMetric,
    WindowGrainError,
    _and,
)
from metricforge.models.metric import (
    TIME_UNITS,
    CumulativeMetricParams,
    DerivedMetricParams,
    Metric,
    RatioMetricParams,
    SimpleMetricParams,
    parse_window,
)
from metricforge.models.query import MetricQuery
from metricforge.models.semantic_model import (
    AggregationType,
    Dimension,
    DimensionType,
    Measure,
    SemanticModel,
)

__all__ = [
    "MultiModelQueryError",
    "NoTimeDimensionError",
    "ResolvedMetric",
    "SQLCompiler",
    "WindowGrainError",
]
from metricforge.parser.loader import MetricRegistry


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
        """Convert a MetricQuery into SQL.

        Metrics on one table compile to a single SELECT. Metrics spanning tables compile to
        one aggregate per model, joined on the query's dimensions (see ``joined.py``).
        """
        models = self._models_for_query(query)
        is_joined = len({model.get_table_name() for model in models}) > 1
        self._check_dimensions(query, models, is_joined)
        resolved_metrics = self._resolve_metrics(query)

        if is_joined:
            from metricforge.compiler.joined import JoinedQueryBuilder

            return self._format_sql(JoinedQueryBuilder(self).build(query, resolved_metrics, models))

        select_exprs = self._build_select_exprs(resolved_metrics, query, models)
        where_conditions = self._build_where_conditions(query, models)
        group_by_exprs = self._build_group_by_exprs(query, models)
        order_by_exprs = self._build_order_by_exprs(query, models)

        sql = self._assemble_query(
            select_exprs=select_exprs,
            from_clause=models[0].get_table_name(),
            where_conditions=where_conditions,
            group_by_exprs=group_by_exprs,
            order_by_exprs=order_by_exprs,
            limit=query.limit,
        )

        return self._format_sql(sql)

    # -- which models a query touches ---------------------------------------------------------

    def _models_for_query(self, query: MetricQuery) -> list[SemanticModel]:
        """The semantic models the query's metrics read from, in first-seen order."""
        models: dict[str, SemanticModel] = {}
        for name in query.metrics:
            for model in self._models_of_metric(self.registry.get_metric(name)):
                models.setdefault(model.name, model)
        if not models:
            raise ValueError("No tables found for query")
        return list(models.values())

    def _models_of_metric(self, metric: Metric) -> list[SemanticModel]:
        params = metric.type_params
        if isinstance(params, SimpleMetricParams | CumulativeMetricParams):
            return [self.registry.get_model_for_measure(params.measure)]
        if isinstance(params, RatioMetricParams):
            referenced = [params.numerator, params.denominator]
        else:
            referenced = params.metrics
        models: list[SemanticModel] = []
        for name in referenced:
            models.extend(self._models_of_metric(self.registry.get_metric(name)))
        return models

    def _check_dimensions(
        self, query: MetricQuery, models: list[SemanticModel], is_joined: bool
    ) -> None:
        """Every dimension must be declared by the query's models.

        On one table an unknown name passes through as a raw column. Across tables every
        name must be declared: categorical dimensions by every model (they key the join),
        time dimensions by at least one (the others fall back to their own time dimension).

        Raises:
            MultiModelQueryError: If a dimension is missing from the models that need it.
            NoTimeDimensionError: If a joined model has no time dimension to align on.
        """
        model_names = ", ".join(f"'{m.name}'" for m in models)
        for dim_name in query.dimensions:
            try:
                dimension = self.registry.get_dimension(dim_name)
            except KeyError:
                if is_joined:
                    raise MultiModelQueryError(
                        f"Dimension '{dim_name}' is not declared by any semantic model, so it "
                        f"cannot key the join across {model_names}."
                    ) from None
                continue

            missing = [m for m in models if m.get_dimension(dim_name) is None]
            if len(missing) == len(models):
                owner = self.registry.get_model_for_dimension(dim_name).name
                raise MultiModelQueryError(
                    f"Dimension '{dim_name}' is not declared by {model_names}; it is on '{owner}'."
                )
            if not is_joined or not missing:
                continue
            if dimension.type == DimensionType.CATEGORICAL:
                raise MultiModelQueryError(
                    f"Dimension '{dim_name}' is not declared by "
                    f"{', '.join(repr(m.name) for m in missing)}. A query across models can "
                    "only group by dimensions every model declares (time dimensions fall back "
                    "to each model's own)."
                )
            for model in missing:
                if self._first_time_dimension(model) is None:
                    raise NoTimeDimensionError(
                        f"Model '{model.name}' has no time dimension to align with '{dim_name}'."
                    )

    # -- metric resolution ----------------------------------------------------------------------

    def _resolve_metrics(self, query: MetricQuery) -> dict[str, ResolvedMetric]:
        resolved = {}
        for name in query.metrics:
            metric = self.registry.get_metric(name)
            resolved[name] = self._resolve_single_metric(metric, query)
        return resolved

    def _resolve_single_metric(
        self, metric: Metric, query: MetricQuery, extra_filter: str | None = None
    ) -> ResolvedMetric:
        """Resolve one metric; ``extra_filter`` is a parent derived/ratio metric's filter."""
        if metric.type == "simple":
            return self._resolve_simple_metric(metric, extra_filter)
        elif metric.type == "derived":
            return self._resolve_derived_metric(metric, query, extra_filter)
        elif metric.type == "ratio":
            return self._resolve_ratio_metric(metric, query, extra_filter)
        elif metric.type == "cumulative":
            return self._resolve_cumulative_metric(metric, query, extra_filter)
        else:
            raise ValueError(f"Unknown metric type: {metric.type}")

    def _resolve_simple_metric(
        self, metric: Metric, extra_filter: str | None = None
    ) -> ResolvedMetric:
        params: SimpleMetricParams = metric.type_params  # type: ignore
        model = self.registry.get_model_for_measure(params.measure)
        measure = self.registry.get_measure(params.measure)

        expr = self._build_measure_expr(measure, _and(metric.filter, extra_filter))

        return ResolvedMetric(
            name=metric.name,
            expr=expr,
            model=model,
            filter=metric.filter,
        )

    def _resolve_derived_metric(
        self, metric: Metric, query: MetricQuery, extra_filter: str | None = None
    ) -> ResolvedMetric:
        """Resolve a derived metric to SQL."""
        params: DerivedMetricParams = metric.type_params  # type: ignore
        combined = _and(metric.filter, extra_filter)

        sub_metrics: dict[str, ResolvedMetric] = {}
        for ref_name in params.metrics:
            ref_metric = self.registry.get_metric(ref_name)
            sub_metrics[ref_name] = self._resolve_single_metric(ref_metric, query, combined)

        resolved = ResolvedMetric(
            name=metric.name,
            expr="",
            model=next(iter(sub_metrics.values())).model if sub_metrics else None,
            filter=metric.filter,
            is_aggregate=False,  # derived metrics wrap aggregates, not aggregates themselves
            sub_metrics=sub_metrics,
            template=params.expr,
        )
        resolved.expr = resolved.render(lambda leaf: leaf.expr)
        return resolved

    def _resolve_ratio_metric(
        self, metric: Metric, query: MetricQuery, extra_filter: str | None = None
    ) -> ResolvedMetric:
        """Resolve a ratio metric to SQL."""
        params: RatioMetricParams = metric.type_params  # type: ignore
        combined = _and(metric.filter, extra_filter)

        numerator_metric = self.registry.get_metric(params.numerator)
        denominator_metric = self.registry.get_metric(params.denominator)

        num_resolved = self._resolve_single_metric(numerator_metric, query, combined)
        denom_resolved = self._resolve_single_metric(denominator_metric, query, combined)

        resolved = ResolvedMetric(
            name=metric.name,
            expr="",
            model=num_resolved.model,
            filter=metric.filter,
            is_aggregate=False,
            sub_metrics={params.numerator: num_resolved, params.denominator: denom_resolved},
            # NULLIF for division-by-zero, 1.0 for float division
            template=f"{params.numerator} * 1.0 / NULLIF({params.denominator}, 0)",
        )
        resolved.expr = resolved.render(lambda leaf: leaf.expr)
        return resolved

    def _resolve_cumulative_metric(
        self, metric: Metric, query: MetricQuery, extra_filter: str | None = None
    ) -> ResolvedMetric:
        """Resolve a cumulative metric to a running total over the query's time dimension."""
        params: CumulativeMetricParams = metric.type_params  # type: ignore
        model = self.registry.get_model_for_measure(params.measure)
        measure = self.registry.get_measure(params.measure)

        base_expr = self._build_measure_expr(measure, _and(metric.filter, extra_filter))
        window = self._build_cumulative_window(query, params, metric.name, model)
        # Reason: without a time axis there is one bucket, so the running total is the total
        expr = f"SUM({base_expr}) {window}" if window else base_expr

        return ResolvedMetric(
            name=metric.name,
            expr=expr,
            model=model,
            filter=metric.filter,
            is_aggregate=False,  # window function, handled differently than aggregates
        )

    def _build_cumulative_window(
        self,
        query: MetricQuery,
        params: CumulativeMetricParams,
        metric_name: str,
        model: SemanticModel,
    ) -> str | None:
        """Build the OVER clause for a cumulative metric, or None if the query has no time axis.

        Raises:
            NoTimeDimensionError: If the metric has a trailing window but the query no time
                dimension to measure it along.
            WindowGrainError: If the window unit is finer than the query's grain.
        """
        time_dim = self._find_time_dimension(query.dimensions)
        if time_dim is None:
            if params.window:
                raise NoTimeDimensionError(
                    f"Cumulative metric '{metric_name}' has a trailing window of "
                    f"'{params.window}' but the query has no time dimension to apply it along."
                )
            return None

        order_expr = self._dimension_expr(time_dim, query.time_grain, [model])
        partitions = [
            self._dimension_expr(dim, query.time_grain, [model])
            for dim in query.dimensions
            if dim != time_dim
        ]
        if params.grain_to_date:
            # Reason: restarting at each grain boundary turns the running total into
            # a to-date total (e.g. month-to-date); truncating the group key keeps it valid SQL
            partitions.append(f"DATE_TRUNC('{params.grain_to_date}', {order_expr})")

        if params.window:
            grain = self._effective_grain(time_dim, query.time_grain)
            frame = self._trailing_frame(params.window, grain, metric_name)
        else:
            frame = "ROWS UNBOUNDED PRECEDING"

        clauses = []
        if partitions:
            clauses.append(f"PARTITION BY {', '.join(partitions)}")
        clauses.append(f"ORDER BY {order_expr} {frame}")
        return f"OVER ({' '.join(clauses)})"

    def _effective_grain(self, time_dim: str, time_grain: str | None) -> str:
        """The bucket size of the time axis: the query grain, else the dimension's own."""
        if time_grain:
            return time_grain
        declared = self.registry.get_dimension(time_dim).time_granularity
        return declared.value if declared else "day"

    @staticmethod
    def _trailing_frame(window: str, grain: str, metric_name: str) -> str:
        """RANGE frame covering the last ``window`` ending at (and including) the current bucket."""
        count, unit = parse_window(window)
        if grain in TIME_UNITS and TIME_UNITS.index(unit) < TIME_UNITS.index(grain):
            raise WindowGrainError(
                f"Cumulative metric '{metric_name}' has a window of '{window}', finer than "
                f"the query's '{grain}' grain; group by {unit} or coarser, or widen the window."
            )
        # Reason: RANGE PRECEDING includes its bound, so a window of N units that ends at the
        # current bucket starts N units minus one bucket back (7 days by day = 6 days preceding)
        return (
            f"RANGE BETWEEN (INTERVAL {count} {unit.upper()} - INTERVAL 1 {grain.upper()}) "
            "PRECEDING AND CURRENT ROW"
        )

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

    # -- dimensions -----------------------------------------------------------------------------

    def _dimension_expr(
        self, dim_name: str, time_grain: str | None, models: list[SemanticModel]
    ) -> str:
        """SQL for a dimension as these models see it.

        The first model declaring the name wins. A time dimension none of them declares falls
        back to the first model's own time dimension (the models were checked for one). A name
        no model knows passes through as a raw column.
        """
        dimension = self._dimension_on(models, dim_name)
        if dimension is None:
            return dim_name

        col_expr = dimension.expr or dimension.name
        # time dimensions can be truncated to different grains
        if dimension.type == DimensionType.TIME and time_grain:
            return f"DATE_TRUNC('{time_grain}', {col_expr})"
        return col_expr

    def _dimension_on(self, models: list[SemanticModel], dim_name: str) -> Dimension | None:
        for model in models:
            dimension = model.get_dimension(dim_name)
            if dimension is not None:
                return dimension
        if self._is_time_dimension(dim_name):
            for model in models:
                fallback = self._first_time_dimension(model)
                if fallback is not None:
                    return fallback
        return None

    @staticmethod
    def _first_time_dimension(model: SemanticModel) -> Dimension | None:
        return next((d for d in model.dimensions if d.type == DimensionType.TIME), None)

    def _is_time_dimension(self, dim_name: str) -> bool:
        try:
            return self.registry.get_dimension(dim_name).type == DimensionType.TIME
        except KeyError:
            return False

    def _find_time_dimension(self, dimensions: list[str]) -> str | None:
        for dim_name in dimensions:
            if self._is_time_dimension(dim_name):
                return dim_name
        return None

    # -- clauses --------------------------------------------------------------------------------

    def _build_select_exprs(
        self,
        resolved_metrics: dict[str, ResolvedMetric],
        query: MetricQuery,
        models: list[SemanticModel],
    ) -> list[str]:
        exprs = []

        for dim_name in query.dimensions:
            dim_expr = self._dimension_expr(dim_name, query.time_grain, models)
            exprs.append(f"{dim_expr} AS {dim_name}")

        for metric_name, resolved in resolved_metrics.items():
            exprs.append(f"{resolved.expr} AS {metric_name}")

        return exprs

    def _build_where_conditions(self, query: MetricQuery, models: list[SemanticModel]) -> list[str]:
        """Build WHERE conditions.

        Raises:
            NoTimeDimensionError: If the query has a date range but neither the query nor the
                model names a time dimension.
        """
        conditions = list(query.filters)

        if query.start_date or query.end_date:
            time_dim = self._find_time_dimension(query.dimensions) or self._model_time_dimension(
                models, query
            )
            col_expr = self._dimension_expr(time_dim, None, models)

            if query.start_date:
                conditions.append(f"{col_expr} >= DATE '{query.start_date}'")
            if query.end_date:
                # Reason: "<= end" is midnight on a TIMESTAMP column, which drops the last day
                conditions.append(f"{col_expr} < DATE '{query.end_date}' + INTERVAL 1 DAY")

        return conditions

    def _model_time_dimension(self, models: list[SemanticModel], query: MetricQuery) -> str:
        """The first time dimension declared by the query's model, for date-range fallback."""
        for model in models:
            first = self._first_time_dimension(model)
            if first is not None:
                return first.name
        names = ", ".join(f"'{m.name}'" for m in models) or "the query"
        requested = ", ".join(f"'{name}'" for name in query.metrics)
        raise NoTimeDimensionError(
            f"Query for {requested} has a date range but {names} declares no time dimension; "
            "add one or drop the date range."
        )

    def _build_group_by_exprs(self, query: MetricQuery, models: list[SemanticModel]) -> list[str]:
        return [self._dimension_expr(d, query.time_grain, models) for d in query.dimensions]

    def _build_order_by_exprs(self, query: MetricQuery, models: list[SemanticModel]) -> list[str]:
        if query.order_by:
            return query.order_by
        return self._build_group_by_exprs(query, models)

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

        if limit is not None:
            parts.append(f"LIMIT {limit}")

        return "\n".join(parts)

    def _format_sql(self, sql: str) -> str:
        """Format SQL using SQLGlot."""
        try:
            parsed = sqlglot.parse_one(sql, dialect=self.dialect)
            return parsed.sql(dialect=self.dialect, pretty=True)
        except Exception:
            return sql
