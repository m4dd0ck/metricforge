"""DuckDB query executor for MetricForge.

duckdb is perfect for this use case - embedded, fast, and speaks sql.
no need for a separate database server for local analytics work.
the in-memory mode is great for testing and one-off queries.

I tried sqlite first but duckdb's analytical query performance is so much
better for aggregations and window functions.
"""

import time
from pathlib import Path
from typing import Any

import duckdb

from metricforge.models.query import QueryResult


class DuckDBExecutor:
    """Execute queries against DuckDB.

    thin wrapper around duckdb that handles connection management
    and result formatting. keeps the duckdb-specific bits isolated.
    """

    def __init__(self, database_path: str | None = None) -> None:
        """Initialize DuckDB connection.

        Args:
            database_path: Path to DuckDB file, or None for in-memory.
        """
        self.database_path = database_path
        self._conn: duckdb.DuckDBPyConnection | None = None  # lazy init

    @property
    def conn(self) -> duckdb.DuckDBPyConnection:
        """Get or create the DuckDB connection.

        lazy initialization so we don't open a db until we actually need it.
        ":memory:" is the duckdb convention for in-memory database.
        """
        if self._conn is None:
            self._conn = duckdb.connect(self.database_path or ":memory:")
        return self._conn

    def execute(self, sql: str) -> QueryResult:
        """Execute SQL and return structured results.

        times the execution for performance monitoring. the dict conversion
        isn't the most efficient for large result sets but makes the api
        much nicer to work with.
        """
        start = time.perf_counter()

        result = self.conn.execute(sql)
        # result.description gives us (name, type_code, ...) tuples
        columns = [desc[0] for desc in result.description]
        rows = result.fetchall()

        elapsed_ms = (time.perf_counter() - start) * 1000

        # convert rows to list of dicts - easier to work with in python
        # TODO: might want a flag to return raw tuples for large datasets
        data = [dict(zip(columns, row)) for row in rows]

        return QueryResult(
            sql=sql,
            columns=columns,
            data=data,
            row_count=len(data),
            execution_time_ms=round(elapsed_ms, 2),
        )

    def execute_raw(self, sql: str) -> list[tuple[Any, ...]]:
        """Execute SQL and return raw tuples."""
        result = self.conn.execute(sql)
        return result.fetchall()

    def load_parquet(self, table_name: str, path: str | Path) -> None:
        """Load a Parquet file as a table.

        duckdb can read parquet directly without loading into memory first.
        using CREATE OR REPLACE so reloading is idempotent.
        """
        path = Path(path)
        self.conn.execute(f"""
            CREATE OR REPLACE TABLE {table_name} AS
            SELECT * FROM read_parquet('{path}')
        """)

    def load_csv(self, table_name: str, path: str | Path) -> None:
        """Load a CSV file as a table.

        read_csv_auto is magic - duckdb figures out delimiters, types, etc.
        works surprisingly well in practice though sometimes you need hints.
        """
        path = Path(path)
        self.conn.execute(f"""
            CREATE OR REPLACE TABLE {table_name} AS
            SELECT * FROM read_csv_auto('{path}')
        """)

    def create_table_from_data(
        self, table_name: str, columns: list[str], data: list[tuple[Any, ...]]
    ) -> None:
        """Create a table from in-memory data.

        useful for testing and loading small datasets. for anything larger
        you probably want to use parquet files instead.
        """
        if not data:
            raise ValueError("Cannot create table from empty data")

        # duckdb infers types from the data - usually works fine
        placeholders = ", ".join(["?"] * len(columns))
        col_defs = ", ".join(columns)

        self.conn.execute(f"CREATE OR REPLACE TABLE {table_name} ({col_defs})")

        # executemany is more efficient than individual inserts
        self.conn.executemany(
            f"INSERT INTO {table_name} VALUES ({placeholders})",
            data,
        )

    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists.

        querying information_schema is the portable way to do this.
        duckdb also has a SHOW TABLES but this is more explicit.
        """
        result = self.conn.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
            [table_name],
        )
        return result.fetchone()[0] > 0

    def get_table_schema(self, table_name: str) -> list[tuple[str, str]]:
        """Get column names and types for a table."""
        result = self.conn.execute(f"DESCRIBE {table_name}")
        return [(row[0], row[1]) for row in result.fetchall()]

    def close(self) -> None:
        """Close the database connection."""
        if self._conn:
            self._conn.close()
            self._conn = None

    # context manager support for clean resource management
    def __enter__(self) -> "DuckDBExecutor":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()
