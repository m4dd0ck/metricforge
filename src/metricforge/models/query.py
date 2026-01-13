"""Pydantic models for metric queries and results."""

from datetime import date

from pydantic import BaseModel, Field


class MetricQuery(BaseModel):
    """A request to compute one or more metrics."""

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
    """Result of a metric query."""

    sql: str
    columns: list[str]
    data: list[dict]
    row_count: int
    execution_time_ms: float
