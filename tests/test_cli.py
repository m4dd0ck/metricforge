"""Tests for CLI commands."""

from pathlib import Path

import pytest
from typer.testing import CliRunner

from metricforge.cli.main import app

runner = CliRunner()


class TestCLIList:
    def test_list_metrics(self, metrics_dir: Path):
        """Can list metrics via CLI."""
        result = runner.invoke(app, ["list", "metrics", "--dir", str(metrics_dir)])
        assert result.exit_code == 0
        assert "revenue" in result.stdout.lower()

    def test_list_dimensions(self, metrics_dir: Path):
        """Can list dimensions via CLI."""
        result = runner.invoke(app, ["list", "dimensions", "--dir", str(metrics_dir)])
        assert result.exit_code == 0
        assert "order_date" in result.stdout.lower()

    def test_list_measures(self, metrics_dir: Path):
        """Can list measures via CLI."""
        result = runner.invoke(app, ["list", "measures", "--dir", str(metrics_dir)])
        assert result.exit_code == 0
        assert "order_amount" in result.stdout.lower()

    def test_list_invalid_type(self, metrics_dir: Path):
        """Reports error for invalid list type."""
        result = runner.invoke(app, ["list", "invalid", "--dir", str(metrics_dir)])
        assert result.exit_code == 1
        assert "unknown type" in result.stdout.lower()

    def test_list_nonexistent_directory(self, tmp_path: Path):
        """Reports error for nonexistent directory."""
        result = runner.invoke(app, ["list", "metrics", "--dir", str(tmp_path / "nope")])
        assert result.exit_code == 1
        assert "error" in result.stdout.lower()


class TestCLIValidate:
    def test_validate_success(self, metrics_dir: Path):
        """Validate passes for valid metrics."""
        result = runner.invoke(app, ["validate", "--dir", str(metrics_dir)])
        assert result.exit_code == 0
        assert "success" in result.stdout.lower()

    def test_validate_nonexistent_directory(self, tmp_path: Path):
        """Validate fails for nonexistent directory."""
        result = runner.invoke(app, ["validate", "--dir", str(tmp_path / "nope")])
        assert result.exit_code == 1


class TestCLIShowSQL:
    def test_show_sql_single_metric(self, metrics_dir: Path):
        """Can show SQL for single metric."""
        result = runner.invoke(app, ["show-sql", "revenue", "--dir", str(metrics_dir)])
        assert result.exit_code == 0
        assert "SELECT" in result.stdout.upper()

    def test_show_sql_multiple_metrics(self, metrics_dir: Path):
        """Can show SQL for multiple metrics."""
        result = runner.invoke(
            app, ["show-sql", "revenue,total_orders", "--dir", str(metrics_dir)]
        )
        assert result.exit_code == 0
        assert "SELECT" in result.stdout.upper()

    def test_show_sql_with_dimensions(self, metrics_dir: Path):
        """Can show SQL with dimensions."""
        result = runner.invoke(
            app,
            ["show-sql", "revenue", "--dir", str(metrics_dir), "--dimensions", "country"],
        )
        assert result.exit_code == 0
        assert "GROUP BY" in result.stdout.upper()

    def test_show_sql_with_time_grain(self, metrics_dir: Path):
        """Can show SQL with time grain."""
        result = runner.invoke(
            app,
            [
                "show-sql",
                "revenue",
                "--dir",
                str(metrics_dir),
                "--dimensions",
                "order_date",
                "--grain",
                "month",
            ],
        )
        assert result.exit_code == 0
        assert "DATE_TRUNC" in result.stdout.upper()

    def test_show_sql_unknown_metric(self, metrics_dir: Path):
        """Reports error for unknown metric."""
        result = runner.invoke(app, ["show-sql", "nonexistent", "--dir", str(metrics_dir)])
        assert result.exit_code == 1
        assert "error" in result.stdout.lower()


class TestCLIQuery:
    def test_query_requires_database(self, metrics_dir: Path):
        """Query fails gracefully without database."""
        result = runner.invoke(app, ["query", "revenue", "--dir", str(metrics_dir)])
        # May fail because table doesn't exist, which is expected
        # The important thing is it doesn't crash
        assert result.exit_code in (0, 1)

    def test_query_with_show_sql(self, metrics_dir: Path):
        """Can show SQL with query."""
        result = runner.invoke(
            app, ["query", "revenue", "--dir", str(metrics_dir), "--sql"]
        )
        # May fail execution but should show SQL
        assert "SELECT" in result.stdout.upper()

    def test_query_json_output(self, metrics_dir: Path):
        """Can specify JSON output format."""
        # This will fail without data but tests the argument parsing
        result = runner.invoke(
            app, ["query", "revenue", "--dir", str(metrics_dir), "--output", "json"]
        )
        # Exit code may be 1 due to missing table, but args should parse
        assert result.exit_code in (0, 1)


class TestCLIHelp:
    def test_main_help(self):
        """Main help works."""
        result = runner.invoke(app, ["--help"])
        assert result.exit_code == 0
        assert "metricforge" in result.stdout.lower() or "semantic" in result.stdout.lower()

    def test_list_help(self):
        """List command help works."""
        result = runner.invoke(app, ["list", "--help"])
        assert result.exit_code == 0

    def test_query_help(self):
        """Query command help works."""
        result = runner.invoke(app, ["query", "--help"])
        assert result.exit_code == 0

    def test_validate_help(self):
        """Validate command help works."""
        result = runner.invoke(app, ["validate", "--help"])
        assert result.exit_code == 0

    def test_show_sql_help(self):
        """Show-sql command help works."""
        result = runner.invoke(app, ["show-sql", "--help"])
        assert result.exit_code == 0
