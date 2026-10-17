"""YAML parser and metric registry for MetricForge."""

from pathlib import Path
from typing import Any

import yaml

from metricforge.models.metric import (
    CumulativeMetricParams,
    DerivedMetricParams,
    Metric,
    RatioMetricParams,
    SimpleMetricParams,
)
from metricforge.models.semantic_model import Dimension, Measure, SemanticModel


class MetricRegistry:
    """Central registry of all semantic models and metrics."""

    def __init__(self) -> None:
        self.semantic_models: dict[str, SemanticModel] = {}
        self.metrics: dict[str, Metric] = {}
        # reverse indexes for looking up which model contains a measure/dimension
        # built after loading to avoid circular dependency issues
        self._measure_to_model: dict[str, str] = {}  # measure_name -> model_name
        # Reason: the same dimension name on several models is the join key between them,
        # so this keeps every model that declares it, in load order
        self._dimension_to_models: dict[str, list[str]] = {}

    def load_directory(self, path: Path) -> None:
        """Load all YAML files from a directory recursively."""
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"Metrics directory not found: {path}")

        yaml_files = list(path.glob("**/*.yaml")) + list(path.glob("**/*.yml"))
        if not yaml_files:
            raise ValueError(f"No YAML files found in {path}")

        for yaml_file in sorted(yaml_files):
            self._load_file(yaml_file)

        # indexes must be built before validation since validation uses them
        self._build_indexes()
        self._validate_references()

    def _load_file(self, path: Path) -> None:
        """Parse a single YAML file."""
        with open(path) as f:
            data = yaml.safe_load(f)

        if data is None:
            return
        for sm_data in data.get("semantic_models", []):
            model = SemanticModel.model_validate(sm_data)
            if model.name in self.semantic_models:
                raise ValueError(f"Duplicate semantic model: {model.name}")
            self.semantic_models[model.name] = model

        for metric_data in data.get("metrics", []):
            metric = self._parse_metric(metric_data)
            if metric.name in self.metrics:
                raise ValueError(f"Duplicate metric: {metric.name}")
            self.metrics[metric.name] = metric

    def _parse_metric(self, data: dict[str, Any]) -> Metric:
        """Parse a metric definition with manual type_params dispatch."""
        metric_type = data.get("type")
        type_params_data = data.get("type_params", {})

        type_params: (
            SimpleMetricParams | DerivedMetricParams | RatioMetricParams | CumulativeMetricParams
        )
        if metric_type == "simple":
            type_params = SimpleMetricParams(**type_params_data)
        elif metric_type == "derived":
            type_params = DerivedMetricParams(**type_params_data)
        elif metric_type == "ratio":
            type_params = RatioMetricParams(**type_params_data)
        elif metric_type == "cumulative":
            type_params = CumulativeMetricParams(**type_params_data)
        else:
            raise ValueError(f"Unknown metric type: {metric_type}")

        return Metric(
            name=data["name"],
            description=data.get("description"),
            type=metric_type,
            type_params=type_params,
            filter=data.get("filter"),
        )

    def _build_indexes(self) -> None:
        """Build lookup indexes for measures and dimensions."""
        for model_name, model in self.semantic_models.items():
            for measure in model.measures:
                if measure.name in self._measure_to_model:
                    raise ValueError(
                        f"Duplicate measure name '{measure.name}' in models "
                        f"'{self._measure_to_model[measure.name]}' and '{model_name}'"
                    )
                self._measure_to_model[measure.name] = model_name

            for dimension in model.dimensions:
                self._index_dimension(dimension, model_name)

    def _index_dimension(self, dimension: Dimension, model_name: str) -> None:
        """Record which models declare a dimension; shared names must agree on the type."""
        for other_name in self._dimension_to_models.get(dimension.name, []):
            other = self.semantic_models[other_name].get_dimension(dimension.name)
            if other is not None and other.type != dimension.type:
                raise ValueError(
                    f"Dimension '{dimension.name}' is {other.type.value} in model "
                    f"'{other_name}' but {dimension.type.value} in '{model_name}'"
                )
        self._dimension_to_models.setdefault(dimension.name, []).append(model_name)

    def _validate_references(self) -> None:
        """Ensure all metric references are valid."""
        for metric_name, metric in self.metrics.items():
            if metric.type == "simple":
                params = metric.type_params
                if not isinstance(params, SimpleMetricParams):
                    continue
                if params.measure not in self._measure_to_model:
                    raise ValueError(
                        f"Metric '{metric_name}' references unknown measure '{params.measure}'"
                    )

            elif metric.type == "derived":
                params = metric.type_params
                if not isinstance(params, DerivedMetricParams):
                    continue
                for ref_metric in params.metrics:
                    if ref_metric not in self.metrics:
                        raise ValueError(
                            f"Derived metric '{metric_name}' references unknown "
                            f"metric '{ref_metric}'"
                        )

            elif metric.type == "ratio":
                params = metric.type_params
                if not isinstance(params, RatioMetricParams):
                    continue
                if params.numerator not in self.metrics:
                    raise ValueError(
                        f"Ratio metric '{metric_name}' references unknown "
                        f"numerator metric '{params.numerator}'"
                    )
                if params.denominator not in self.metrics:
                    raise ValueError(
                        f"Ratio metric '{metric_name}' references unknown "
                        f"denominator metric '{params.denominator}'"
                    )

            elif metric.type == "cumulative":
                params = metric.type_params
                if not isinstance(params, CumulativeMetricParams):
                    continue
                if params.measure not in self._measure_to_model:
                    raise ValueError(
                        f"Cumulative metric '{metric_name}' references unknown "
                        f"measure '{params.measure}'"
                    )

        self._reject_circular_references()

    def _referenced_metrics(self, metric: Metric) -> list[str]:
        """Names of the metrics a derived or ratio metric is computed from."""
        params = metric.type_params
        if isinstance(params, DerivedMetricParams):
            return list(params.metrics)
        if isinstance(params, RatioMetricParams):
            return [params.numerator, params.denominator]
        return []

    def _reject_circular_references(self) -> None:
        """Raise if derived or ratio metrics reference each other in a cycle.

        Reason: the compiler resolves sub-metrics recursively, so a cycle would
        only surface as a RecursionError at query time.
        """
        done: set[str] = set()

        def visit(name: str, path: list[str]) -> None:
            if name in path:
                cycle = " -> ".join([*path[path.index(name) :], name])
                raise ValueError(f"Circular metric reference: {cycle}")
            if name in done:
                return
            for ref in self._referenced_metrics(self.metrics[name]):
                visit(ref, [*path, name])
            done.add(name)

        for metric_name in self.metrics:
            visit(metric_name, [])

    def get_metric(self, name: str) -> Metric:
        if name not in self.metrics:
            raise KeyError(f"Unknown metric: {name}")
        return self.metrics[name]

    def get_semantic_model(self, name: str) -> SemanticModel:
        if name not in self.semantic_models:
            raise KeyError(f"Unknown semantic model: {name}")
        return self.semantic_models[name]

    def get_model_for_measure(self, measure_name: str) -> SemanticModel:
        model_name = self._measure_to_model.get(measure_name)
        if not model_name:
            raise KeyError(f"Unknown measure: {measure_name}")
        return self.semantic_models[model_name]

    def get_model_for_dimension(self, dimension_name: str) -> SemanticModel:
        """The first model that declares a dimension (see ``get_models_for_dimension``)."""
        return self.get_models_for_dimension(dimension_name)[0]

    def get_models_for_dimension(self, dimension_name: str) -> list[SemanticModel]:
        """Every model that declares a dimension, in load order."""
        model_names = self._dimension_to_models.get(dimension_name)
        if not model_names:
            raise KeyError(f"Unknown dimension: {dimension_name}")
        return [self.semantic_models[name] for name in model_names]

    def get_measure(self, measure_name: str) -> Measure:
        model = self.get_model_for_measure(measure_name)
        measure = model.get_measure(measure_name)
        if measure is None:
            raise KeyError(f"Unknown measure: {measure_name}")
        return measure

    def get_dimension(self, dimension_name: str) -> Dimension:
        model = self.get_model_for_dimension(dimension_name)
        dimension = model.get_dimension(dimension_name)
        if dimension is None:
            raise KeyError(f"Unknown dimension: {dimension_name}")
        return dimension
