"""Pydantic models for metric queries and results.

query model is intentionally simple - it captures what the user wants to compute,
not how to compute it. the compiler handles the translation to sql.
"""

from datetime import date

from pydantic import BaseModel, Field


class MetricQuery(BaseModel):
    """A request to compute one or more metrics.

    designed to mirror the kind of questions analysts actually ask:
    "give me revenue by region for last month, daily grain"
    """

    metrics: list[str]
    dimensions: list[str] = Field(default_factory=list)
    filters: list[str] = Field(default_factory=list)  # raw sql WHERE fragments
    time_grain: str | None = None  # day, week, month, etc.
    start_date: date | None = None
    end_date: date | None = None
    limit: int | None = None
    order_by: list[str] = Field(default_factory=list)
    # TODO: could add having clause for filtering on aggregated values


class QueryResult(BaseModel):
    """Result of a metric query.

    returning the sql alongside data is useful for debugging and auditing -
    analysts at my previous job always wanted to see what query was actually run.
    """

    sql: str  # the generated sql - always include this for transparency
    columns: list[str]
    data: list[dict]  # list of dicts is easy to work with even if not the most efficient
    row_count: int
    execution_time_ms: float
