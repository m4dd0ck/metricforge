"""Resolved metrics and the errors the compiler raises."""

import re
from collections.abc import Callable
from dataclasses import dataclass, field

from metricforge.models.semantic_model import SemanticModel


class MultiModelQueryError(ValueError):
    """Raised when a query's dimensions or filters cannot be applied to every model it spans."""


class NoTimeDimensionError(ValueError):
    """Raised when a query needs a time axis (date range, trailing window) and has none."""


class WindowGrainError(ValueError):
    """Raised when a cumulative window is finer than the grain the query groups by."""


def _and(*parts: str | None) -> str | None:
    """Combine SQL boolean fragments with AND, skipping empties."""
    present = [p for p in parts if p]
    if not present:
        return None
    return present[0] if len(present) == 1 else " AND ".join(f"({p})" for p in present)


def _names_pattern(names: list[str]) -> re.Pattern[str]:
    # Reason: whole words, longest first, so a name that is a substring of another (or that
    # appears inside already-substituted SQL) is never rewritten by mistake
    ordered = sorted(names, key=len, reverse=True)
    return re.compile(r"\b(?:" + "|".join(re.escape(n) for n in ordered) + r")\b")


@dataclass
class ResolvedMetric:
    """A metric resolved to its SQL components."""

    name: str
    expr: str  # the sql expression for this metric
    model: SemanticModel | None  # which model this comes from (for table lookup)
    filter: str | None = None
    is_aggregate: bool = True  # false for derived/ratio which wrap aggregates
    sub_metrics: dict[str, "ResolvedMetric"] = field(default_factory=dict)
    template: str | None = None  # derived/ratio: expression written over sub-metric names

    def render(self, leaf: Callable[["ResolvedMetric"], str]) -> str:
        """SQL for this metric with every leaf aggregate rendered by ``leaf``."""
        if self.template is None:
            return leaf(self)
        pattern = _names_pattern(list(self.sub_metrics))
        return pattern.sub(
            lambda m: f"({self.sub_metrics[m.group(0)].render(leaf)})", self.template
        )

    def leaves(self) -> list["ResolvedMetric"]:
        """The aggregate metrics this one is built from (itself, if it is one)."""
        if self.template is None:
            return [self]
        return [leaf for sub in self.sub_metrics.values() for leaf in sub.leaves()]
