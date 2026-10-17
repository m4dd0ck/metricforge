"""Tests for CLI commands."""

import json
from pathlib import Path

import duckdb
import pytest
from typer.testing import CliRunner

from metricforge.cli.main import _output_result, app
from metricforge.models.query import QueryResult

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
        result = runner.invoke(app, ["show-sql", "revenue,total_orders", "--dir", str(metrics_dir)])
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
        result = runner.invoke(app, ["query", "revenue", "--dir", str(metrics_dir), "--sql"])
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


def _result(rows: list[dict[str, object]]) -> QueryResult:
    return QueryResult(
        sql="SELECT 1",
        columns=["country", "revenue"],
        data=rows,
        row_count=len(rows),
        execution_time_ms=0.1,
    )


class TestJSONOutput:
    def test_json_keeps_markup_like_values(self, capsys: pytest.CaptureFixture[str]):
        """A value that looks like rich markup comes back unchanged from json.loads."""
        _output_result(_result([{"country": "[red]x[/red]", "revenue": 1}]), "json")

        assert json.loads(capsys.readouterr().out) == [{"country": "[red]x[/red]", "revenue": 1}]

    def test_json_keeps_long_values_on_one_line(self, capsys: pytest.CaptureFixture[str]):
        """A value wider than the console is not wrapped, so the output stays valid JSON."""
        value = "x" * 120
        _output_result(_result([{"country": value, "revenue": 1}]), "json")

        assert json.loads(capsys.readouterr().out)[0]["country"] == value

    def test_json_ends_with_a_newline(self, capsys: pytest.CaptureFixture[str]):
        _output_result(_result([]), "json")

        assert capsys.readouterr().out == "[]\n"


class TestTableOutput:
    def test_table_escapes_markup_in_values(self, capsys: pytest.CaptureFixture[str]):
        """A value like "[red]x[/red]" is printed literally, not styled."""
        _output_result(_result([{"country": "[red]x[/red]", "revenue": 1}]), "table")

        assert "[red]x[/red]" in capsys.readouterr().out


class TestSQLGoesToStderrForDataOutput:
    @pytest.fixture
    def db_path(self, tmp_path: Path, sample_orders_data: list[tuple]) -> str:
        path = tmp_path / "orders.duckdb"
        conn = duckdb.connect(str(path))
        conn.execute(
            "CREATE TABLE orders (order_id INTEGER, customer_id INTEGER, amount DECIMAL(10, 2), "
            "status VARCHAR, country VARCHAR, order_date DATE)"
        )
        conn.executemany("INSERT INTO orders VALUES (?, ?, ?, ?, ?, ?)", sample_orders_data)
        conn.close()
        return str(path)

    def test_json_stdout_is_parseable_with_sql_shown(self, metrics_dir: Path, db_path: str):
        result = runner.invoke(
            app,
            ["query", "revenue", "--dir", str(metrics_dir), "--db", db_path, "--sql", "-o", "json"],
        )

        assert result.exit_code == 0, result.output
        assert json.loads(result.stdout)[0]["revenue"] is not None
        assert "SELECT" in result.stderr.upper()

    def test_csv_stdout_is_clean_with_sql_shown(self, metrics_dir: Path, db_path: str):
        result = runner.invoke(
            app,
            ["query", "revenue", "--dir", str(metrics_dir), "--db", db_path, "--sql", "-o", "csv"],
        )

        assert result.exit_code == 0, result.output
        assert result.stdout.splitlines()[0] == "revenue"
        assert "SELECT" in result.stderr.upper()

    def test_table_output_keeps_sql_on_stdout(self, metrics_dir: Path, db_path: str):
        result = runner.invoke(
            app, ["query", "revenue", "--dir", str(metrics_dir), "--db", db_path, "--sql"]
        )

        assert result.exit_code == 0, result.output
        assert "SELECT" in result.stdout.upper()


class TestCSVOutput:
    def _result(self, rows: list[dict[str, object]]) -> QueryResult:
        return _result(rows)

    def test_csv_escapes_commas_and_quotes(self, capsys: pytest.CaptureFixture[str]):
        """Values holding commas or quotes are quoted the way the csv module does it."""
        _output_result(self._result([{"country": 'Côte, d"Ivoire', "revenue": 10}]), "csv")

        assert capsys.readouterr().out == 'country,revenue\n"Côte, d""Ivoire",10\n'

    def test_csv_does_not_treat_values_as_markup(self, capsys: pytest.CaptureFixture[str]):
        """A value that looks like rich markup is printed as-is."""
        _output_result(self._result([{"country": "[red]US[/red]", "revenue": 1}]), "csv")

        assert "[red]US[/red]" in capsys.readouterr().out

    def test_csv_prints_header_for_empty_result(self, capsys: pytest.CaptureFixture[str]):
        _output_result(self._result([]), "csv")

        assert capsys.readouterr().out == "country,revenue\n"

    def test_csv_null_is_empty_field(self, capsys: pytest.CaptureFixture[str]):
        _output_result(self._result([{"country": None, "revenue": None}]), "csv")

        assert capsys.readouterr().out.splitlines()[1] == ","
