"""Pydantic models for metric definitions.

heavily inspired by dbt's semantic layer design - they got the mental model right
even if their implementation is tied to their ecosystem.
"""

from typing import Annotated, Literal, Self

from pydantic import BaseModel, Field, model_validator

# metric type params - I went back and forth on whether to use inheritance
# or composition here. ended up with separate classes since the params are
# genuinely different for each type. probably the right call.

class SimpleMetricParams(BaseModel):
    """Parameters for simple (single measure) metrics."""

    measure: str  # reference to a measure in a semantic model


class DerivedMetricParams(BaseModel):
    """Parameters for derived (calculated) metrics."""

    # the expr is evaluated after aggregation - learned this the hard way when
    # I initially tried to do it in the wrong order and got nonsense results
    expr: str  # e.g., "revenue / order_count"
    metrics: list[str]  # metrics referenced in expr - must list them explicitly


class RatioMetricParams(BaseModel):
    """Parameters for ratio metrics."""

    numerator: str  # Metric name
    denominator: str  # Metric name


class CumulativeMetricParams(BaseModel):
    """Parameters for cumulative (running total) metrics."""

    measure: str
    window: str | None = None  # e.g., "7 days", "1 month"
    grain_to_date: str | None = None  # e.g., "month" for MTD
    # TODO: window parsing needs work - currently only supports simple cases


# tried using a proper discriminated union here but pydantic's discriminator
# doesn't work well when the discriminator field is on the parent model.
# ended up doing manual dispatch in the loader instead - a bit ugly but works
MetricTypeParams = Annotated[
    SimpleMetricParams | DerivedMetricParams | RatioMetricParams | CumulativeMetricParams,
    Field(discriminator=None),
]


class Metric(BaseModel):
    """A business metric definition.

    this is the core abstraction - a metric is a named, typed calculation that
    references measures and/or other metrics. keeping it simple for now but
    might want to add labels/tags for organization later.
    """

    name: str
    description: str | None = None
    type: Literal["simple", "derived", "ratio", "cumulative"]
    type_params: MetricTypeParams
    filter: str | None = None  # sql where clause fragment - gets injected into CASE WHEN

    @model_validator(mode="after")
    def validate_type_params(self) -> Self:
        """Ensure type_params matches the metric type.

        this validation happens after pydantic parses type_params, so if the
        yaml has the wrong structure we catch it here with a clear error.
        spent too long debugging a case where someone had "measure" in a derived
        metric and got a confusing AttributeError deep in the compiler.
        """
        type_map = {
            "simple": SimpleMetricParams,
            "derived": DerivedMetricParams,
            "ratio": RatioMetricParams,
            "cumulative": CumulativeMetricParams,
        }
        expected = type_map[self.type]
        if not isinstance(self.type_params, expected):
            raise ValueError(f"Metric type '{self.type}' requires {expected.__name__}")
        return self
