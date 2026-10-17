"""Pydantic models for metric definitions."""

import re
from typing import Annotated, Literal, Self

from pydantic import BaseModel, Field, model_validator

# Reason: finest to coarsest, so a window can be checked against the query's grain
TIME_UNITS = ("day", "week", "month", "quarter", "year")

_WINDOW_PATTERN = re.compile(r"^\s*(\d+)\s+(day|week|month|quarter|year)s?\s*$", re.IGNORECASE)


def parse_window(window: str) -> tuple[int, str]:
    """Split a cumulative window such as ``"7 days"`` into ``(7, "day")``.

    Raises:
        ValueError: If the text is not ``<count> <day|week|month|quarter|year>``.
    """
    match = _WINDOW_PATTERN.match(window)
    if match is None or int(match.group(1)) < 1:
        raise ValueError(
            f"window '{window}' must be a positive count and a unit, e.g. '7 days' or '1 month'"
        )
    return int(match.group(1)), match.group(2).lower()


class SimpleMetricParams(BaseModel):
    """Parameters for simple (single measure) metrics."""

    measure: str  # reference to a measure in a semantic model


class DerivedMetricParams(BaseModel):
    """Parameters for derived (calculated) metrics."""

    expr: str  # evaluated after aggregation
    metrics: list[str]  # metrics referenced in expr


class RatioMetricParams(BaseModel):
    """Parameters for ratio metrics."""

    numerator: str  # Metric name
    denominator: str  # Metric name


class CumulativeMetricParams(BaseModel):
    """Parameters for cumulative (running total) metrics."""

    measure: str
    window: str | None = None  # trailing window, e.g. "7 days"; None means running total
    grain_to_date: str | None = None  # e.g., "month" for MTD


MetricTypeParams = Annotated[
    SimpleMetricParams | DerivedMetricParams | RatioMetricParams | CumulativeMetricParams,
    Field(discriminator=None),
]


class Metric(BaseModel):
    """A business metric definition."""

    name: str
    description: str | None = None
    type: Literal["simple", "derived", "ratio", "cumulative"]
    type_params: MetricTypeParams
    filter: str | None = None  # sql where clause fragment - gets injected into CASE WHEN

    @model_validator(mode="after")
    def validate_type_params(self) -> Self:
        """Ensure type_params matches the metric type."""
        type_map = {
            "simple": SimpleMetricParams,
            "derived": DerivedMetricParams,
            "ratio": RatioMetricParams,
            "cumulative": CumulativeMetricParams,
        }
        expected = type_map[self.type]
        if not isinstance(self.type_params, expected):
            raise ValueError(f"Metric type '{self.type}' requires {expected.__name__}")
        if isinstance(self.type_params, CumulativeMetricParams):
            self._validate_cumulative(self.type_params)
        return self

    def _validate_cumulative(self, params: CumulativeMetricParams) -> None:
        if params.window and params.grain_to_date:
            raise ValueError(
                f"Cumulative metric '{self.name}': use either 'window' or 'grain_to_date', not both"
            )
        if params.grain_to_date and params.grain_to_date not in TIME_UNITS:
            raise ValueError(
                f"Cumulative metric '{self.name}': grain_to_date '{params.grain_to_date}' "
                f"is not one of {', '.join(TIME_UNITS)}"
            )
        if params.window:
            try:
                parse_window(params.window)
            except ValueError as e:
                raise ValueError(f"Cumulative metric '{self.name}': {e}") from e
