"""Pydantic models for metric definitions."""

from typing import Annotated, Literal, Self

from pydantic import BaseModel, Field, model_validator


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
    window: str | None = None  # declared so a YAML "window:" key is rejected, not ignored
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
        if isinstance(self.type_params, CumulativeMetricParams) and self.type_params.window:
            # Reason: an ignored window compiled to an unbounded running total, silently wrong
            raise ValueError(
                f"Cumulative metric '{self.name}': 'window' is not implemented yet; "
                "use grain_to_date or remove it"
            )
        return self
