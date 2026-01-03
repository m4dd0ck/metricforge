"""SQL compiler for metric queries.

this is where the magic happens - translating semantic layer queries into
executable sql. heavily inspired by metricflow's approach but simplified.

the basic flow:
  1. resolve metrics to sql expressions (handling derived/ratio recursively)
  2. figure out which tables we need
  3. build select/from/where/group by/order by clauses
  4. format with sqlglot

sqlglot does the heavy lifting for sql formatting and dialect translation.
"""

from dataclasses import dataclass, field

import sqlglot
from sqlglot import exp  # not currently using exp but keeping for future ast work

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
    Dimension,
    DimensionType,
    Measure,
    SemanticModel,
)
from metricforge.parser.loader import MetricRegistry


@dataclass
class ResolvedMetric:
    """A metric resolved to its SQL components.

    intermediate representation used during compilation. captures the sql
    expression along with metadata needed for query building.
    """

    name: str
    expr: str  # the sql expression for this metric
    model: SemanticModel | None  # which model this comes from (for table lookup)
    filter: str | None = None
    is_aggregate: bool = True  # false for derived/ratio which wrap aggregates
    sub_metrics: dict[str, "ResolvedMetric"] = field(default_factory=dict)


class SQLCompiler:
    """Compiles metric queries into SQL.

    stateless compiler - takes a registry reference but doesn't modify it.
    all the logic for turning semantic queries into sql lives here.
    """

    # mapping from our aggregation types to sql function names
    # count_distinct is special-cased in _build_measure_expr
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

        the compilation pipeline is fairly linear - resolve metrics first,
        then build each sql clause in order. keeping the steps explicit
        makes it easier to debug when something goes wrong.
        """
        # step 1: resolve all metrics to their base sql expressions
        # this handles derived/ratio metrics recursively
        resolved_metrics = self._resolve_metrics(query.metrics)

        # step 2: figure out which tables we need
        # currently only supports single-table queries
        tables = self._get_required_tables(resolved_metrics, query.dimensions)

        # step 3-7: build each clause
        select_exprs = self._build_select_exprs(resolved_metrics, query)
        from_clause = self._build_from_clause(tables)
        where_conditions = self._build_where_conditions(resolved_metrics, query)
        group_by_exprs = self._build_group_by_exprs(query)
        order_by_exprs = self._build_order_by_exprs(query)

        # step 8: put it all together
        sql = self._assemble_query(
            select_exprs=select_exprs,
            from_clause=from_clause,
            where_conditions=where_conditions,
            group_by_exprs=group_by_exprs,
            order_by_exprs=order_by_exprs,
            limit=query.limit,
        )

        return self._format_sql(sql)

    def _resolve_metrics(self, metric_names: list[str]) -> dict[str, ResolvedMetric]:
        """Resolve metrics to their SQL expressions."""
        resolved = {}
        for name in metric_names:
            metric = self.registry.get_metric(name)
            resolved[name] = self._resolve_single_metric(metric)
        return resolved

    def _resolve_single_metric(self, metric: Metric) -> ResolvedMetric:
        """Convert a metric definition to SQL expression components."""
        if metric.type == "simple":
            return self._resolve_simple_metric(metric)
        elif metric.type == "derived":
            return self._resolve_derived_metric(metric)
        elif metric.type == "ratio":
            return self._resolve_ratio_metric(metric)
        elif metric.type == "cumulative":
            return self._resolve_cumulative_metric(metric)
        else:
            raise ValueError(f"Unknown metric type: {metric.type}")

    def _resolve_simple_metric(self, metric: Metric) -> ResolvedMetric:
        """Resolve a simple metric to SQL."""
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

    def _resolve_derived_metric(self, metric: Metric) -> ResolvedMetric:
        """Resolve a derived metric to SQL.

        derived metrics are expressions over other metrics.
        we recursively resolve the referenced metrics and substitute their
        sql expressions into the derived expression.

        example: avg_order_value with expr "revenue / order_count" becomes
        "(SUM(amount)) / (COUNT(order_id))"
        """
        params: DerivedMetricParams = metric.type_params  # type: ignore

        # recursively resolve referenced metrics
        sub_metrics: dict[str, ResolvedMetric] = {}
        for ref_name in params.metrics:
            ref_metric = self.registry.get_metric(ref_name)
            sub_metrics[ref_name] = self._resolve_single_metric(ref_metric)

        # substitute metric names with their sql expressions
        # simple string replacement - assumes metric names don't appear as substrings
        # TODO: use proper expression parsing for robustness
        expr = params.expr
        for ref_name, resolved in sub_metrics.items():
            sub_expr = resolved.expr
            expr = expr.replace(ref_name, f"({sub_expr})")

        # model comes from first sub-metric (they should all use same model for now)
        model = next(iter(sub_metrics.values())).model if sub_metrics else None

        return ResolvedMetric(
            name=metric.name,
            expr=expr,
            model=model,
            filter=metric.filter,
            is_aggregate=False,  # derived metrics wrap aggregates, not aggregates themselves
            sub_metrics=sub_metrics,
        )

    def _resolve_ratio_metric(self, metric: Metric) -> ResolvedMetric:
        """Resolve a ratio metric to SQL.

        ratio is a special case of derived - just numerator/denominator.
        could have users write derived metrics for ratios but this is
        more explicit and we can handle division-by-zero automatically.
        """
        params: RatioMetricParams = metric.type_params  # type: ignore

        numerator_metric = self.registry.get_metric(params.numerator)
        denominator_metric = self.registry.get_metric(params.denominator)

        num_resolved = self._resolve_single_metric(numerator_metric)
        denom_resolved = self._resolve_single_metric(denominator_metric)

        # NULLIF handles division by zero - returns null instead of error
        # multiply by 1.0 to force float division in case both are integers
        expr = f"({num_resolved.expr}) * 1.0 / NULLIF({denom_resolved.expr}, 0)"

        return ResolvedMetric(
            name=metric.name,
            expr=expr,
            model=num_resolved.model,
            filter=metric.filter,
            is_aggregate=False,
            sub_metrics={params.numerator: num_resolved, params.denominator: denom_resolved},
        )

    def _resolve_cumulative_metric(self, metric: Metric) -> ResolvedMetric:
        """Resolve a cumulative metric to SQL.

        cumulative metrics are running totals using window functions.
        this implementation is pretty basic - ideally we'd use the actual
        time dimension in the ORDER BY clause.
        """
        params: CumulativeMetricParams = metric.type_params  # type: ignore
        model = self.registry.get_model_for_measure(params.measure)
        measure = self.registry.get_measure(params.measure)

        # build the base measure expression
        base_expr = self._build_measure_expr(measure, metric.filter)

        # window function for running total
        # ORDER BY 1 is a hack - should really use the time dimension
        # TODO: pass time dimension through and use it here
        expr = f"SUM({base_expr}) OVER (ORDER BY 1)"

        return ResolvedMetric(
            name=metric.name,
            expr=expr,
            model=model,
            filter=metric.filter,
            is_aggregate=False,  # window function, handled differently than aggregates
        )

    def _build_measure_expr(self, measure: Measure, filter_expr: str | None = None) -> str:
        """Build SQL expression for a measure.

        handles aggregation function wrapping and optional filtering.
        filters use CASE WHEN so we can apply metric-specific filters
        within a single query (e.g., count completed orders vs all orders).
        """
        agg_func = self.AGG_MAP[measure.agg]
        col_expr = measure.expr

        # count_distinct needs special handling - DISTINCT goes inside the parens
        if measure.agg == AggregationType.COUNT_DISTINCT:
            if filter_expr:
                # filter applied via CASE WHEN - nulls don't get counted
                return f"{agg_func}(DISTINCT CASE WHEN {filter_expr} THEN {col_expr} END)"
            return f"{agg_func}(DISTINCT {col_expr})"

        # standard aggregates with optional filter
        if filter_expr:
            return f"{agg_func}(CASE WHEN {filter_expr} THEN {col_expr} END)"

        return f"{agg_func}({col_expr})"

    def _get_required_tables(
        self, resolved_metrics: dict[str, ResolvedMetric], dimensions: list[str]
    ) -> set[str]:
        """Get the set of required tables for the query.

        walks through all metrics (including sub-metrics for derived/ratio)
        and dimensions to find which tables we need.
        """
        tables = set()

        # tables from metrics
        for resolved in resolved_metrics.values():
            if resolved.model:
                tables.add(resolved.model.get_table_name())
            # also check sub-metrics for derived/ratio
            for sub in resolved.sub_metrics.values():
                if sub.model:
                    tables.add(sub.model.get_table_name())

        # tables from dimensions
        for dim_name in dimensions:
            try:
                model = self.registry.get_model_for_dimension(dim_name)
                tables.add(model.get_table_name())
            except KeyError:
                pass  # dimension might be in a filter, not a semantic model

        return tables

    def _build_select_exprs(
        self, resolved_metrics: dict[str, ResolvedMetric], query: MetricQuery
    ) -> list[str]:
        """Build SELECT expressions."""
        exprs = []

        # Add dimensions first
        for dim_name in query.dimensions:
            dim_expr = self._build_dimension_expr(dim_name, query.time_grain)
            exprs.append(f"{dim_expr} AS {dim_name}")

        # Add metrics
        for metric_name, resolved in resolved_metrics.items():
            exprs.append(f"{resolved.expr} AS {metric_name}")

        return exprs

    def _build_dimension_expr(self, dim_name: str, time_grain: str | None) -> str:
        """Build SQL expression for a dimension.

        time dimensions get wrapped in DATE_TRUNC when a grain is specified.
        duckdb's DATE_TRUNC is nice because it accepts string grain names.
        """
        try:
            dimension = self.registry.get_dimension(dim_name)
            col_expr = dimension.expr or dim_name

            # time dimensions can be truncated to different grains
            if dimension.type == DimensionType.TIME and time_grain:
                return f"DATE_TRUNC('{time_grain}', {col_expr})"

            return col_expr
        except KeyError:
            # if dimension isn't in the registry, just use the name as-is
            # this lets users reference columns that aren't defined as dimensions
            return dim_name

    def _build_from_clause(self, tables: set[str]) -> str:
        """Build FROM clause.

        currently only supports single-table queries. multi-table would need
        join logic based on entity relationships defined in semantic models.
        that's a significant chunk of work I haven't gotten to yet.
        """
        if not tables:
            raise ValueError("No tables found for query")

        if len(tables) > 1:
            # TODO: implement join logic using entity relationships
            # for now, just warn and use first table
            pass

        return next(iter(tables))

    def _build_where_conditions(
        self, resolved_metrics: dict[str, ResolvedMetric], query: MetricQuery
    ) -> list[str]:
        """Build WHERE conditions.

        combines user-provided filters with automatic date filtering.
        date filters only work if there's a time dimension in the query.
        """
        conditions = list(query.filters)

        # add date filters if start/end date provided
        if query.start_date or query.end_date:
            time_dim = self._find_time_dimension(query.dimensions)
            if time_dim:
                dim = self.registry.get_dimension(time_dim)
                col_expr = dim.expr or time_dim

                # inclusive date range - might want to make this configurable
                if query.start_date:
                    conditions.append(f"{col_expr} >= '{query.start_date}'")
                if query.end_date:
                    conditions.append(f"{col_expr} <= '{query.end_date}'")

        return conditions

    def _find_time_dimension(self, dimensions: list[str]) -> str | None:
        """Find a time dimension in the list."""
        for dim_name in dimensions:
            try:
                dim = self.registry.get_dimension(dim_name)
                if dim.type == DimensionType.TIME:
                    return dim_name
            except KeyError:
                pass
        return None

    def _build_group_by_exprs(self, query: MetricQuery) -> list[str]:
        """Build GROUP BY expressions."""
        if not query.dimensions:
            return []

        exprs = []
        for dim_name in query.dimensions:
            dim_expr = self._build_dimension_expr(dim_name, query.time_grain)
            exprs.append(dim_expr)

        return exprs

    def _build_order_by_exprs(self, query: MetricQuery) -> list[str]:
        """Build ORDER BY expressions."""
        if query.order_by:
            return query.order_by

        # Default: order by dimensions if present
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
        """Assemble the final SQL query.

        just string concatenation at this point - sqlglot handles formatting.
        could probably build an ast directly but this is simpler to debug.
        """
        parts = [f"SELECT\n  {',\n  '.join(select_exprs)}"]
        parts.append(f"FROM {from_clause}")

        if where_conditions:
            # all conditions ANDed together
            parts.append(f"WHERE {' AND '.join(where_conditions)}")

        if group_by_exprs:
            parts.append(f"GROUP BY {', '.join(group_by_exprs)}")

        if order_by_exprs:
            parts.append(f"ORDER BY {', '.join(order_by_exprs)}")

        if limit:
            parts.append(f"LIMIT {limit}")

        return "\n".join(parts)

    def _format_sql(self, sql: str) -> str:
        """Format SQL using SQLGlot.

        sqlglot's pretty printer is great for readability. the try/except
        is defensive - if our generated sql has syntax issues we at least
        return something the user can debug.
        """
        try:
            parsed = sqlglot.parse_one(sql, dialect=self.dialect)
            return parsed.sql(dialect=self.dialect, pretty=True)
        except Exception:
            # fall back to unformatted if parsing fails
            # this shouldn't happen but better than crashing
            return sql
