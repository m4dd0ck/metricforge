"""Tests for YAML parser and MetricRegistry."""

from pathlib import Path

import pytest

from metricforge.parser.loader import MetricRegistry


class TestMetricRegistry:
    def test_load_directory(self, metrics_dir: Path):
        """Can load metrics from directory."""
        registry = MetricRegistry()
        registry.load_directory(metrics_dir)

        assert len(registry.semantic_models) > 0
        assert len(registry.metrics) > 0

    def test_load_nonexistent_directory(self, tmp_path: Path):
        """Raises error for nonexistent directory."""
        registry = MetricRegistry()
        with pytest.raises(FileNotFoundError):
            registry.load_directory(tmp_path / "nonexistent")

    def test_load_empty_directory(self, tmp_path: Path):
        """Raises error for empty directory."""
        empty_dir = tmp_path / "empty"
        empty_dir.mkdir()

        registry = MetricRegistry()
        with pytest.raises(ValueError, match="No YAML files"):
            registry.load_directory(empty_dir)

    def test_get_metric(self, registry: MetricRegistry):
        """Can retrieve metric by name."""
        metric = registry.get_metric("revenue")
        assert metric.name == "revenue"
        assert metric.type == "simple"

    def test_get_unknown_metric_raises(self, registry: MetricRegistry):
        """Raises KeyError for unknown metric."""
        with pytest.raises(KeyError, match="Unknown metric"):
            registry.get_metric("nonexistent")

    def test_get_semantic_model(self, registry: MetricRegistry):
        """Can retrieve semantic model by name."""
        model = registry.get_semantic_model("orders")
        assert model.name == "orders"
        assert model.table == "orders"

    def test_get_unknown_semantic_model_raises(self, registry: MetricRegistry):
        """Raises KeyError for unknown model."""
        with pytest.raises(KeyError, match="Unknown semantic model"):
            registry.get_semantic_model("nonexistent")

    def test_get_model_for_measure(self, registry: MetricRegistry):
        """Can find model containing a measure."""
        model = registry.get_model_for_measure("order_amount")
        assert model.name == "orders"

    def test_get_model_for_unknown_measure_raises(self, registry: MetricRegistry):
        """Raises KeyError for unknown measure."""
        with pytest.raises(KeyError, match="Unknown measure"):
            registry.get_model_for_measure("nonexistent")

    def test_get_model_for_dimension(self, registry: MetricRegistry):
        """Can find model containing a dimension."""
        model = registry.get_model_for_dimension("order_date")
        assert model.name == "orders"

    def test_get_measure(self, registry: MetricRegistry):
        """Can retrieve measure by name."""
        measure = registry.get_measure("order_amount")
        assert measure.name == "order_amount"

    def test_get_dimension(self, registry: MetricRegistry):
        """Can retrieve dimension by name."""
        dimension = registry.get_dimension("order_date")
        assert dimension.name == "order_date"


class TestMetricRegistryValidation:
    def test_duplicate_semantic_model_raises(self, tmp_path: Path):
        """Raises error for duplicate semantic model names."""
        metrics_path = tmp_path / "metrics"
        metrics_path.mkdir()

        yaml1 = """
semantic_models:
  - name: orders
    table: orders
    primary_entity: order_id
"""
        yaml2 = """
semantic_models:
  - name: orders
    table: orders2
    primary_entity: order_id
"""
        (metrics_path / "file1.yaml").write_text(yaml1)
        (metrics_path / "file2.yaml").write_text(yaml2)

        registry = MetricRegistry()
        with pytest.raises(ValueError, match="Duplicate semantic model"):
            registry.load_directory(metrics_path)

    def test_duplicate_metric_raises(self, tmp_path: Path):
        """Raises error for duplicate metric names."""
        metrics_path = tmp_path / "metrics"
        metrics_path.mkdir()

        yaml_content = """
semantic_models:
  - name: orders
    table: orders
    primary_entity: order_id
    measures:
      - name: order_amount
        agg: sum
        expr: amount

metrics:
  - name: revenue
    type: simple
    type_params:
      measure: order_amount

  - name: revenue
    type: simple
    type_params:
      measure: order_amount
"""
        (metrics_path / "test.yaml").write_text(yaml_content)

        registry = MetricRegistry()
        with pytest.raises(ValueError, match="Duplicate metric"):
            registry.load_directory(metrics_path)

    def test_unknown_measure_reference_raises(self, tmp_path: Path):
        """Raises error for metric referencing unknown measure."""
        metrics_path = tmp_path / "metrics"
        metrics_path.mkdir()

        yaml_content = """
semantic_models:
  - name: orders
    table: orders
    primary_entity: order_id

metrics:
  - name: revenue
    type: simple
    type_params:
      measure: nonexistent_measure
"""
        (metrics_path / "test.yaml").write_text(yaml_content)

        registry = MetricRegistry()
        with pytest.raises(ValueError, match="references unknown measure"):
            registry.load_directory(metrics_path)

    def test_unknown_metric_reference_raises(self, tmp_path: Path):
        """Raises error for derived metric referencing unknown metric."""
        metrics_path = tmp_path / "metrics"
        metrics_path.mkdir()

        yaml_content = """
semantic_models:
  - name: orders
    table: orders
    primary_entity: order_id
    measures:
      - name: order_amount
        agg: sum
        expr: amount

metrics:
  - name: revenue
    type: simple
    type_params:
      measure: order_amount

  - name: aov
    type: derived
    type_params:
      expr: "revenue / nonexistent"
      metrics:
        - revenue
        - nonexistent
"""
        (metrics_path / "test.yaml").write_text(yaml_content)

        registry = MetricRegistry()
        with pytest.raises(ValueError, match="references unknown metric"):
            registry.load_directory(metrics_path)
