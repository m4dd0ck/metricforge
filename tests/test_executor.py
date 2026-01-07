"""Tests for DuckDB executor."""

from pathlib import Path

import pytest

from metricforge.executor.duckdb_executor import DuckDBExecutor


class TestDuckDBExecutor:
    def test_create_in_memory(self):
        """Can create in-memory executor."""
        executor = DuckDBExecutor()
        assert executor.database_path is None
        result = executor.execute("SELECT 1 AS value")
        assert result.data[0]["value"] == 1
        executor.close()

    def test_create_with_file(self, tmp_path: Path):
        """Can create file-based executor."""
        db_path = str(tmp_path / "test.duckdb")
        executor = DuckDBExecutor(db_path)
        executor.execute("CREATE TABLE test (id INTEGER)")
        executor.close()

        # Reopen and verify
        executor2 = DuckDBExecutor(db_path)
        assert executor2.table_exists("test")
        executor2.close()

    def test_execute_returns_query_result(self):
        """Execute returns QueryResult with correct fields."""
        executor = DuckDBExecutor()
        result = executor.execute("SELECT 1 AS a, 'hello' AS b")

        assert result.columns == ["a", "b"]
        assert result.data == [{"a": 1, "b": "hello"}]
        assert result.row_count == 1
        assert result.execution_time_ms >= 0
        assert "SELECT" in result.sql
        executor.close()

    def test_execute_raw(self):
        """Execute raw returns tuples."""
        executor = DuckDBExecutor()
        rows = executor.execute_raw("SELECT 1, 2, 3")
        assert rows == [(1, 2, 3)]
        executor.close()

    def test_load_csv(self, tmp_path: Path):
        """Can load CSV file into table."""
        csv_path = tmp_path / "test.csv"
        csv_path.write_text("id,name\n1,Alice\n2,Bob\n")

        executor = DuckDBExecutor()
        executor.load_csv("people", csv_path)

        result = executor.execute("SELECT COUNT(*) as count FROM people")
        assert result.data[0]["count"] == 2
        executor.close()

    def test_table_exists(self):
        """Can check if table exists."""
        executor = DuckDBExecutor()
        assert not executor.table_exists("nonexistent")

        executor.execute("CREATE TABLE test_table (id INTEGER)")
        assert executor.table_exists("test_table")
        executor.close()

    def test_get_table_schema(self):
        """Can get table schema."""
        executor = DuckDBExecutor()
        executor.execute(
            """
            CREATE TABLE test_table (
                id INTEGER,
                name VARCHAR,
                amount DECIMAL(10, 2)
            )
        """
        )

        schema = executor.get_table_schema("test_table")
        columns = [col[0] for col in schema]
        assert "id" in columns
        assert "name" in columns
        assert "amount" in columns
        executor.close()

    def test_context_manager(self):
        """Executor can be used as context manager."""
        with DuckDBExecutor() as executor:
            result = executor.execute("SELECT 1 AS value")
            assert result.data[0]["value"] == 1


class TestDuckDBExecutorWithData:
    def test_execute_aggregation(self, db_with_data):
        """Can execute aggregation queries."""
        result = db_with_data.execute("SELECT COUNT(*) as count FROM orders")
        assert result.data[0]["count"] == 10

    def test_execute_with_filter(self, db_with_data):
        """Can execute queries with filters."""
        result = db_with_data.execute(
            "SELECT COUNT(*) as count FROM orders WHERE status = 'completed'"
        )
        # From fixture: completed orders are 7
        assert result.data[0]["count"] == 7

    def test_execute_groupby(self, db_with_data):
        """Can execute queries with GROUP BY."""
        result = db_with_data.execute(
            """
            SELECT country, COUNT(*) as count
            FROM orders
            GROUP BY country
            ORDER BY count DESC
        """
        )
        assert result.row_count > 0
        assert "country" in result.columns
        assert "count" in result.columns

    def test_execute_aggregation_sum(self, db_with_data):
        """Can execute SUM aggregation."""
        result = db_with_data.execute(
            "SELECT SUM(amount) as total FROM orders WHERE status = 'completed'"
        )
        # Sum of completed: 100 + 150 + 75 + 125 + 175 + 250 + 400 = 1275
        assert result.data[0]["total"] == 1275
