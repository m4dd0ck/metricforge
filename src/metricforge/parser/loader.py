"""YAML parser and metric registry for MetricForge.

the registry is the central source of truth for all metric definitions.
yaml was an easy choice since it's human readable and plays nice with git.
considered json but yaml's support for comments is too useful for documentation.
"""

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
    """Central registry of all semantic models and metrics.

    loads yaml files, validates references, and provides lookup methods.
    I originally had separate registries for models and metrics but merged
    them since they're always used together anyway.
    """

    def __init__(self) -> None:
        self.semantic_models: dict[str, SemanticModel] = {}
        self.metrics: dict[str, Metric] = {}
        # reverse indexes for looking up which model contains a measure/dimension
        # built after loading to avoid circular dependency issues
        self._measure_to_model: dict[str, str] = {}  # measure_name -> model_name
        self._dimension_to_model: dict[str, str] = {}  # dimension_name -> model_name

    def load_directory(self, path: Path) -> None:
        """Load all YAML files from a directory.

        recursively finds all yaml/yml files. order doesn't matter since
        we build indexes and validate after loading everything.
        """
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"Metrics directory not found: {path}")

        # support both .yaml and .yml - people have opinions about this
        yaml_files = list(path.glob("**/*.yaml")) + list(path.glob("**/*.yml"))
        if not yaml_files:
            raise ValueError(f"No YAML files found in {path}")

        for yaml_file in yaml_files:
            self._load_file(yaml_file)

        # indexes must be built before validation since validation uses them
        self._build_indexes()
        self._validate_references()

    def _load_file(self, path: Path) -> None:
        """Parse a single YAML file.

        files can contain semantic_models, metrics, or both.
        empty files are silently ignored which is handy for templates.
        """
        with open(path) as f:
            data = yaml.safe_load(f)

        if data is None:
            return  # empty file, no big deal

        # load semantic models first (metrics depend on them)
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
        """Parse a metric definition, handling type_params discriminator.

        this is the manual discriminated union handling I mentioned in the model.
        pydantic v2 has discriminated unions but they expect the discriminator
        field to be inside the union variant, not on the parent.
        """
        metric_type = data.get("type")
        type_params_data = data.get("type_params", {})

        # manually dispatch to the right params class based on type
        # would love to use match/case here but want to support python 3.9+
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
        """Build lookup indexes for measures and dimensions.

        enforces global uniqueness for measure and dimension names across
        all models. could relax this with qualified names (model.measure)
        but simple names are nicer for the common case.
        """
        for model_name, model in self.semantic_models.items():
            for measure in model.measures:
                if measure.name in self._measure_to_model:
                    # this catches a real footgun - same measure name in two models
                    raise ValueError(
                        f"Duplicate measure name '{measure.name}' in models "
                        f"'{self._measure_to_model[measure.name]}' and '{model_name}'"
                    )
                self._measure_to_model[measure.name] = model_name

            for dimension in model.dimensions:
                if dimension.name in self._dimension_to_model:
                    raise ValueError(
                        f"Duplicate dimension name '{dimension.name}' in models "
                        f"'{self._dimension_to_model[dimension.name]}' and '{model_name}'"
                    )
                self._dimension_to_model[dimension.name] = model_name

    def _validate_references(self) -> None:
        """Ensure all metric references are valid.

        catches broken references early at load time rather than at query time.
        saves a lot of debugging headaches - ask me how I know.
        """
        for metric_name, metric in self.metrics.items():
            # simple metrics reference a measure directly
            if metric.type == "simple":
                params = metric.type_params
                if not isinstance(params, SimpleMetricParams):
                    continue
                if params.measure not in self._measure_to_model:
                    raise ValueError(
                        f"Metric '{metric_name}' references unknown measure '{params.measure}'"
                    )

            # derived metrics reference other metrics
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
                # TODO: check for circular dependencies

            # ratio metrics reference two other metrics
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

            # cumulative metrics reference a measure (like simple)
            elif metric.type == "cumulative":
                params = metric.type_params
                if not isinstance(params, CumulativeMetricParams):
                    continue
                if params.measure not in self._measure_to_model:
                    raise ValueError(
                        f"Cumulative metric '{metric_name}' references unknown "
                        f"measure '{params.measure}'"
                    )

    # --- lookup methods ---
    # all of these raise KeyError for unknown names which is standard dict behavior

    def get_metric(self, name: str) -> Metric:
        """Get a metric by name."""
        if name not in self.metrics:
            raise KeyError(f"Unknown metric: {name}")
        return self.metrics[name]

    def get_semantic_model(self, name: str) -> SemanticModel:
        """Get a semantic model by name."""
        if name not in self.semantic_models:
            raise KeyError(f"Unknown semantic model: {name}")
        return self.semantic_models[name]

    def get_model_for_measure(self, measure_name: str) -> SemanticModel:
        """Get the semantic model containing a measure.

        uses the reverse index built during load - O(1) lookup.
        """
        model_name = self._measure_to_model.get(measure_name)
        if not model_name:
            raise KeyError(f"Unknown measure: {measure_name}")
        return self.semantic_models[model_name]

    def get_model_for_dimension(self, dimension_name: str) -> SemanticModel:
        """Get the semantic model containing a dimension."""
        model_name = self._dimension_to_model.get(dimension_name)
        if not model_name:
            raise KeyError(f"Unknown dimension: {dimension_name}")
        return self.semantic_models[model_name]

    def get_measure(self, measure_name: str) -> Measure:
        """Get a measure by name.

        two-step lookup: find the model, then find the measure in the model.
        """
        model = self.get_model_for_measure(measure_name)
        measure = model.get_measure(measure_name)
        if measure is None:
            # shouldn't happen if indexes are consistent but defensive
            raise KeyError(f"Unknown measure: {measure_name}")
        return measure

    def get_dimension(self, dimension_name: str) -> Dimension:
        """Get a dimension by name."""
        model = self.get_model_for_dimension(dimension_name)
        dimension = model.get_dimension(dimension_name)
        if dimension is None:
            raise KeyError(f"Unknown dimension: {dimension_name}")
        return dimension
