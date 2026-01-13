"""CLI for MetricForge."""

import json
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console
from rich.syntax import Syntax
from rich.table import Table

from metricforge.store import MetricStore

app = typer.Typer(
    name="mf",
    help="MetricForge - Semantic Layer CLI",
    no_args_is_help=True,
)
console = Console()


def get_store(metrics_dir: Path, db_path: str | None = None) -> MetricStore:
    return MetricStore(metrics_dir, db_path)


@app.command("list")
def list_items(
    item_type: Annotated[str, typer.Argument(help="Type: metrics, dimensions, or measures")],
    metrics_dir: Annotated[Path, typer.Option("--dir", "-d", help="Metrics directory")] = Path(
        "./metrics"
    ),
) -> None:
    """List metrics, dimensions, or measures."""
    try:
        store = get_store(metrics_dir)
    except Exception as e:
        console.print(f"[red]Error loading metrics: {e}[/red]")
        raise typer.Exit(1)

    if item_type == "metrics":
        _list_metrics(store)
    elif item_type == "dimensions":
        _list_dimensions(store)
    elif item_type == "measures":
        _list_measures(store)
    else:
        console.print(f"[red]Unknown type: {item_type}. Use: metrics, dimensions, measures[/red]")
        raise typer.Exit(1)


def _list_metrics(store: MetricStore) -> None:
    metrics = store.list_metrics()

    if not metrics:
        console.print("[yellow]No metrics defined[/yellow]")
        return

    table = Table(title="Metrics")
    table.add_column("Name", style="cyan")
    table.add_column("Type", style="green")
    table.add_column("Description")

    for metric in metrics:
        table.add_row(
            metric["name"],
            metric["type"],
            metric["description"] or "-",
        )

    console.print(table)


def _list_dimensions(store: MetricStore) -> None:
    dims = store.list_dimensions()

    if not dims:
        console.print("[yellow]No dimensions defined[/yellow]")
        return

    table = Table(title="Dimensions")
    table.add_column("Name", style="cyan")
    table.add_column("Type", style="green")
    table.add_column("Model", style="yellow")
    table.add_column("Description")

    for dim in dims:
        table.add_row(
            dim["name"],
            dim["type"],
            dim["model"],
            dim["description"] or "-",
        )

    console.print(table)


def _list_measures(store: MetricStore) -> None:
    measures = store.list_measures()

    if not measures:
        console.print("[yellow]No measures defined[/yellow]")
        return

    table = Table(title="Measures")
    table.add_column("Name", style="cyan")
    table.add_column("Aggregation", style="green")
    table.add_column("Model", style="yellow")
    table.add_column("Description")

    for measure in measures:
        table.add_row(
            measure["name"],
            measure["agg"],
            measure["model"],
            measure["description"] or "-",
        )

    console.print(table)


@app.command()
def query(
    metrics: Annotated[str, typer.Argument(help="Comma-separated metric names")],
    metrics_dir: Annotated[Path, typer.Option("--dir", "-d", help="Metrics directory")] = Path(
        "./metrics"
    ),
    db_path: Annotated[str | None, typer.Option("--db", help="DuckDB database path")] = None,
    dimensions: Annotated[
        str | None, typer.Option("--dimensions", "-g", help="Comma-separated dimensions")
    ] = None,
    time_grain: Annotated[
        str | None, typer.Option("--grain", "-t", help="Time grain: day, week, month, etc.")
    ] = None,
    filters: Annotated[
        str | None, typer.Option("--filter", "-f", help="Semicolon-separated filters")
    ] = None,
    start_date: Annotated[
        str | None, typer.Option("--start", help="Start date (YYYY-MM-DD)")
    ] = None,
    end_date: Annotated[str | None, typer.Option("--end", help="End date (YYYY-MM-DD)")] = None,
    show_sql: Annotated[bool, typer.Option("--sql", "-s", help="Show generated SQL")] = False,
    output: Annotated[
        str, typer.Option("--output", "-o", help="Output format: table, json, csv")
    ] = "table",
    limit: Annotated[int | None, typer.Option("--limit", "-l", help="Maximum rows")] = None,
) -> None:
    """Query metrics with optional dimensions and filters."""
    try:
        store = get_store(metrics_dir, db_path)
    except Exception as e:
        console.print(f"[red]Error loading metrics: {e}[/red]")
        raise typer.Exit(1)

    metric_list = [m.strip() for m in metrics.split(",")]
    dim_list = [d.strip() for d in dimensions.split(",")] if dimensions else []
    filter_list = [f.strip() for f in filters.split(";")] if filters else []

    try:
        if show_sql:
            sql = store.get_sql(
                metrics=metric_list,
                dimensions=dim_list,
                time_grain=time_grain,
                filters=filter_list,
                start_date=start_date,
                end_date=end_date,
                limit=limit,
            )
            syntax = Syntax(sql, "sql", theme="monokai", line_numbers=True)
            console.print(syntax)
            console.print()

        result = store.query(
            metrics=metric_list,
            dimensions=dim_list,
            time_grain=time_grain,
            filters=filter_list,
            start_date=start_date,
            end_date=end_date,
            limit=limit,
        )
    except Exception as e:
        console.print(f"[red]Query error: {e}[/red]")
        raise typer.Exit(1)

    _output_result(result, output)


def _output_result(result, output_format: str) -> None:
    """Output query result in the specified format."""
    if output_format == "json":
        console.print(json.dumps(result.data, indent=2, default=str))
    elif output_format == "csv":
        # TODO: use csv module for proper escaping
        if result.data:
            console.print(",".join(result.columns))
            for row in result.data:
                values = [str(row.get(c, "")) for c in result.columns]
                console.print(",".join(values))
    else:
        table = Table(
            title=f"Query Results ({result.row_count} rows, {result.execution_time_ms}ms)"
        )
        for col in result.columns:
            table.add_column(col)

        for row in result.data:
            values = [str(row.get(c, "")) for c in result.columns]
            table.add_row(*values)

        console.print(table)


@app.command()
def validate(
    metrics_dir: Annotated[Path, typer.Option("--dir", "-d", help="Metrics directory")] = Path(
        "./metrics"
    ),
) -> None:
    """Validate all metric definitions."""
    try:
        store = get_store(metrics_dir)
    except Exception as e:
        console.print(f"[red]Error loading metrics: {e}[/red]")
        raise typer.Exit(1)

    errors = store.validate()

    if errors:
        console.print("[red]Validation failed:[/red]")
        for error in errors:
            console.print(f"  - {error}")
        raise typer.Exit(1)
    else:
        metric_count = len(store.registry.metrics)
        model_count = len(store.registry.semantic_models)
        console.print(
            f"[green]Validated {metric_count} metrics and "
            f"{model_count} semantic models successfully![/green]"
        )


@app.command("show-sql")
def show_sql(
    metrics: Annotated[str, typer.Argument(help="Comma-separated metric names")],
    metrics_dir: Annotated[Path, typer.Option("--dir", "-d", help="Metrics directory")] = Path(
        "./metrics"
    ),
    dimensions: Annotated[
        str | None, typer.Option("--dimensions", "-g", help="Comma-separated dimensions")
    ] = None,
    time_grain: Annotated[
        str | None, typer.Option("--grain", "-t", help="Time grain: day, week, month, etc.")
    ] = None,
    filters: Annotated[
        str | None, typer.Option("--filter", "-f", help="Semicolon-separated filters")
    ] = None,
    start_date: Annotated[
        str | None, typer.Option("--start", help="Start date (YYYY-MM-DD)")
    ] = None,
    end_date: Annotated[str | None, typer.Option("--end", help="End date (YYYY-MM-DD)")] = None,
    limit: Annotated[int | None, typer.Option("--limit", "-l", help="Maximum rows")] = None,
) -> None:
    """Show generated SQL without executing."""
    try:
        store = get_store(metrics_dir)
    except Exception as e:
        console.print(f"[red]Error loading metrics: {e}[/red]")
        raise typer.Exit(1)

    metric_list = [m.strip() for m in metrics.split(",")]
    dim_list = [d.strip() for d in dimensions.split(",")] if dimensions else []
    filter_list = [f.strip() for f in filters.split(";")] if filters else []

    try:
        sql = store.get_sql(
            metrics=metric_list,
            dimensions=dim_list,
            time_grain=time_grain,
            filters=filter_list,
            start_date=start_date,
            end_date=end_date,
            limit=limit,
        )
    except Exception as e:
        console.print(f"[red]Error generating SQL: {e}[/red]")
        raise typer.Exit(1)

    syntax = Syntax(sql, "sql", theme="monokai", line_numbers=True)
    console.print(syntax)


if __name__ == "__main__":
    app()
