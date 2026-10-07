"""MetricForge: Local-first semantic layer for e-commerce analytics."""

from metricforge.compiler.sql_builder import MultiModelQueryError
from metricforge.store import MetricStore

__all__ = ["MetricStore", "MultiModelQueryError"]
__version__ = "0.2.0"
