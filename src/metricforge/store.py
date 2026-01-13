"""Main MetricStore interface for MetricForge."""

from datetime import date
from pathlib import Path

from metricforge.compiler.sql_builder import SQLCompiler
from metricforge.executor.duckdb_executor import DuckDBExecutor
from metricforge.models.query import MetricQuery, QueryResult
from metricforge.parser.loader import MetricRegistry


class MetricStore:
    """Main interface for MetricForge."""

    def __init__(
        self,
        metrics_path: str | Path,
        database_path: str | None = None,
    ) -> None:
        """Initialize the metric store.

        Args:
            metrics_path: Directory containing metric YAML files.
            database_path: Path to DuckDB file, or None for in-memory.
        """
        self.metrics_path = Path(metrics_path)
        self.registry = MetricRegistry()
        self.compiler = SQLCompiler(self.registry)
        self.executor = DuckDBExecutor(database_path)

        # load and validate metrics upfront - fail fast if there are problems
        self.registry.load_directory(self.metrics_path)

    def query(
        self,
        metrics: list[str],
        dimensions: list[str] | None = None,
        filters: list[str] | None = None,
        time_grain: str | None = None,
        start_date: str | date | None = None,
        end_date: str | date | None = None,
        limit: int | None = None,
        order_by: list[str] | None = None,
    ) -> QueryResult:
        """Query metrics with optional dimensions and filters.

        Args:
            metrics: List of metric names to compute.
            dimensions: List of dimensions to group by.
            filters: List of SQL filter expressions.
            time_grain: Time granularity (day, week, month, etc.).
            start_date: Start date filter (inclusive).
            end_date: End date filter (inclusive).
            limit: Maximum rows to return.
            order_by: Columns to order by.

        Returns:
            QueryResult with data and metadata.
        """
        # accept both string and date for convenience
        start = self._parse_date(start_date) if start_date else None
        end = self._parse_date(end_date) if end_date else None

        query = MetricQuery(
            metrics=metrics,
            dimensions=dimensions or [],
            filters=filters or [],
            time_grain=time_grain,
            start_date=start,
            end_date=end,
            limit=limit,
            order_by=order_by or [],
        )

        # two-step: compile to sql, then execute
        sql = self.compiler.compile(query)
        return self.executor.execute(sql)

    def get_sql(
        self,
        metrics: list[str],
        dimensions: list[str] | None = None,
        filters: list[str] | None = None,
        time_grain: str | None = None,
        start_date: str | date | None = None,
        end_date: str | date | None = None,
        limit: int | None = None,
        order_by: list[str] | None = None,
    ) -> str:
        """Get the SQL without executing it."""
        start = self._parse_date(start_date) if start_date else None
        end = self._parse_date(end_date) if end_date else None

        query = MetricQuery(
            metrics=metrics,
            dimensions=dimensions or [],
            filters=filters or [],
            time_grain=time_grain,
            start_date=start,
            end_date=end,
            limit=limit,
            order_by=order_by or [],
        )
        return self.compiler.compile(query)

    def list_metrics(self) -> list[dict]:
        """List all available metrics."""
        return [
            {
                "name": m.name,
                "type": m.type,
                "description": m.description,
            }
            for m in self.registry.metrics.values()
        ]

    def list_dimensions(self) -> list[dict]:
        """List all available dimensions across all models."""
        dims = []
        for model in self.registry.semantic_models.values():
            for dim in model.dimensions:
                dims.append(
                    {
                        "name": dim.name,
                        "type": dim.type.value,
                        "model": model.name,
                        "description": dim.description,
                    }
                )
        return dims

    def list_measures(self) -> list[dict]:
        """List all available measures across all models."""
        measures = []
        for model in self.registry.semantic_models.values():
            for measure in model.measures:
                measures.append(
                    {
                        "name": measure.name,
                        "agg": measure.agg.value,
                        "model": model.name,
                        "description": measure.description,
                    }
                )
        return measures

    def validate(self) -> list[str]:
        """Validate all metric definitions. Returns list of errors."""
        errors = []

        for metric in self.registry.metrics.values():
            try:
                # compilation will fail if references are broken
                self.compiler.compile(MetricQuery(metrics=[metric.name]))
            except Exception as e:
                errors.append(f"Metric '{metric.name}': {e}")

        return errors

    def _parse_date(self, value: str | date) -> date:
        """Parse ISO date string or return date object as-is."""
        if isinstance(value, date):
            return value
        return date.fromisoformat(value)

    def close(self) -> None:
        """Close database connection."""
        self.executor.close()

    def __enter__(self) -> "MetricStore":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
