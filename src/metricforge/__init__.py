"""MetricForge: Local-first semantic layer for e-commerce analytics."""

from metricforge.compiler.sql_builder import MultiModelQueryError, NoTimeDimensionError
from metricforge.store import MetricStore

__all__ = ["MetricStore", "MultiModelQueryError", "NoTimeDimensionError"]
__version__ = "0.2.0"
