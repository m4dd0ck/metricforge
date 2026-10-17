"""Compile a query whose metrics span several tables.

Each model is aggregated on its own, grouped by the query's dimensions, and the aggregates
are joined on those dimensions afterwards. Joining first and aggregating second would
multiply rows (a fan-out), so it never happens here.
"""

import re
from typing import TYPE_CHECKING

import sqlglot
from sqlglot import expressions as exp
from sqlglot.errors import SqlglotError

from metricforge.compiler.resolved import MultiModelQueryError, ResolvedMetric
from metricforge.models.query import MetricQuery
from metricforge.models.semantic_model import SemanticModel

if TYPE_CHECKING:
    from metricforge.compiler.sql_builder import SQLCompiler

_IDENTIFIER = re.compile(r"^\w+$")


def _coalesce(parts: list[str]) -> str:
    return parts[0] if len(parts) == 1 else f"COALESCE({', '.join(parts)})"


class JoinedQueryBuilder:
    """One aggregate CTE per model, FULL OUTER JOINed on the query's dimensions."""

    def __init__(self, compiler: "SQLCompiler") -> None:
        self.compiler = compiler
        # leaf aggregates per model: alias -> SQL, and the alias each resolved leaf got
        self._leaf_exprs: dict[str, dict[str, str]] = {}
        self._alias_of_leaf: dict[int, str] = {}

    def build(
        self,
        query: MetricQuery,
        resolved_metrics: dict[str, ResolvedMetric],
        models: list[SemanticModel],
    ) -> str:
        """SQL for metrics whose models live on different tables.

        Raises:
            MultiModelQueryError: If a raw filter references a column some model lacks.
        """
        self._check_filters(query.filters, models)
        for resolved in resolved_metrics.values():
            for leaf in resolved.leaves():
                self._register_leaf(leaf)

        ctes = [f"{self._alias(m)} AS (\n{self._model_query(m, query)}\n)" for m in models]
        aliases = [self._alias(m) for m in models]

        select_exprs = [
            f"{_coalesce([f'{a}.{d}' for a in aliases])} AS {d}" for d in query.dimensions
        ]
        for name, resolved in resolved_metrics.items():
            select_exprs.append(f"{self._render(resolved)} AS {name}")

        parts = [
            f"WITH {',\n'.join(ctes)}",
            f"SELECT\n  {',\n  '.join(select_exprs)}",
            self._from_clause(aliases, query.dimensions),
        ]
        order_by = query.order_by or list(query.dimensions)
        if order_by:
            parts.append(f"ORDER BY {', '.join(order_by)}")
        if query.limit is not None:
            parts.append(f"LIMIT {query.limit}")
        return "\n".join(parts)

    # -- per-model aggregates -------------------------------------------------------------------

    @staticmethod
    def _alias(model: SemanticModel) -> str:
        # Reason: a CTE named like the table it reads from would shadow that table
        return f"{model.name}_q"

    def _register_leaf(self, leaf: ResolvedMetric) -> None:
        """Give a leaf aggregate a column in its model's CTE; identical SQL shares one."""
        if leaf.model is None:
            raise ValueError(f"Metric '{leaf.name}' resolves to no semantic model")
        exprs = self._leaf_exprs.setdefault(leaf.model.name, {})
        for alias, expr in exprs.items():
            if expr == leaf.expr:
                self._alias_of_leaf[id(leaf)] = alias
                return
        alias, n = leaf.name, 1
        while alias in exprs:
            # Reason: the same metric name can carry different SQL (a parent's filter applied)
            n += 1
            alias = f"{leaf.name}_{n}"
        exprs[alias] = leaf.expr
        self._alias_of_leaf[id(leaf)] = alias

    def _model_query(self, model: SemanticModel, query: MetricQuery) -> str:
        compiler = self.compiler
        dim_exprs = [
            compiler._dimension_expr(d, query.time_grain, [model]) for d in query.dimensions
        ]
        select_exprs = [f"{expr} AS {name}" for expr, name in zip(dim_exprs, query.dimensions)]
        select_exprs += [
            f"{expr} AS {alias}" for alias, expr in self._leaf_exprs[model.name].items()
        ]
        where_conditions = compiler._build_where_conditions(query, [model])

        parts = [
            f"  SELECT\n    {',\n    '.join(select_exprs)}",
            f"  FROM {model.get_table_name()}",
        ]
        if where_conditions:
            parts.append(f"  WHERE {' AND '.join(where_conditions)}")
        if dim_exprs:
            parts.append(f"  GROUP BY {', '.join(dim_exprs)}")
        return "\n".join(parts)

    def _render(self, resolved: ResolvedMetric) -> str:
        def leaf_column(leaf: ResolvedMetric) -> str:
            assert leaf.model is not None  # registered leaves always have one
            return f"{self._alias(leaf.model)}.{self._alias_of_leaf[id(leaf)]}"

        return resolved.render(leaf_column)

    # -- join -----------------------------------------------------------------------------------

    @staticmethod
    def _from_clause(aliases: list[str], dimensions: list[str]) -> str:
        clause = f"FROM {aliases[0]}"
        for i in range(1, len(aliases)):
            if not dimensions:
                clause += f"\nCROSS JOIN {aliases[i]}"
                continue
            # Reason: IS NOT DISTINCT FROM keeps rows whose key is NULL on both sides together
            conditions = [
                f"{_coalesce([f'{a}.{d}' for a in aliases[:i]])} IS NOT DISTINCT FROM "
                f"{aliases[i]}.{d}"
                for d in dimensions
            ]
            clause += f"\nFULL OUTER JOIN {aliases[i]} ON {' AND '.join(conditions)}"
        return clause

    # -- filters --------------------------------------------------------------------------------

    def _check_filters(self, filters: list[str], models: list[SemanticModel]) -> None:
        """A raw filter goes into every model's aggregate, so every model must know its columns.

        Raises:
            MultiModelQueryError: Naming the column and the model that lacks it.
        """
        for filter_sql in filters:
            try:
                columns = {
                    c.name
                    for c in sqlglot.parse_one(filter_sql, dialect=self.compiler.dialect).find_all(
                        exp.Column
                    )
                }
            except SqlglotError:
                continue  # Reason: cannot tell which columns it reads; DuckDB reports a bad one
            for model in models:
                missing = sorted(columns - self._known_columns(model))
                if missing:
                    raise MultiModelQueryError(
                        f'Filter "{filter_sql}" references {", ".join(repr(c) for c in missing)}, '
                        f"which model '{model.name}' does not declare. A filter on a query "
                        "across models must apply to every model; put it on the metric instead."
                    )

    @staticmethod
    def _known_columns(model: SemanticModel) -> set[str]:
        """Column names a model's filters may use: its dimensions, measures and entities."""
        candidates: list[str | None] = [d.name for d in model.dimensions]
        candidates += [d.expr for d in model.dimensions]
        candidates += [e.name for e in model.entities] + [e.expr for e in model.entities]
        candidates += [m.expr for m in model.measures]
        return {c for c in candidates if c is not None and _IDENTIFIER.match(c)}
