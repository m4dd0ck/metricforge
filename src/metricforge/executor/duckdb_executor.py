"""DuckDB query executor for MetricForge."""

import time
from pathlib import Path
from typing import Any

import duckdb

from metricforge.models.query import QueryResult


class DuckDBExecutor:
    """Execute queries against DuckDB."""

    def __init__(self, database_path: str | None = None) -> None:
        """Initialize DuckDB connection.

        Args:
            database_path: Path to DuckDB file, or None for in-memory.
        """
        self.database_path = database_path
        self._conn: duckdb.DuckDBPyConnection | None = None  # lazy init

    @property
    def conn(self) -> duckdb.DuckDBPyConnection:
        """Get or create the DuckDB connection (lazy init)."""
        if self._conn is None:
            self._conn = duckdb.connect(self.database_path or ":memory:")
        return self._conn

    def execute(self, sql: str) -> QueryResult:
        """Execute SQL and return structured results."""
        start = time.perf_counter()

        result = self.conn.execute(sql)
        columns = [desc[0] for desc in result.description]
        rows = result.fetchall()

        elapsed_ms = (time.perf_counter() - start) * 1000
        data = [dict(zip(columns, row)) for row in rows]

        return QueryResult(
            sql=sql,
            columns=columns,
            data=data,
            row_count=len(data),
            execution_time_ms=round(elapsed_ms, 2),
        )

    def execute_raw(self, sql: str) -> list[tuple[Any, ...]]:
        result = self.conn.execute(sql)
        return result.fetchall()

    def load_parquet(self, table_name: str, path: str | Path) -> None:
        """Load a Parquet file as a table."""
        path = Path(path)
        self.conn.execute(f"""
            CREATE OR REPLACE TABLE {table_name} AS
            SELECT * FROM read_parquet('{path}')
        """)

    def load_csv(self, table_name: str, path: str | Path) -> None:
        """Load a CSV file as a table."""
        path = Path(path)
        self.conn.execute(f"""
            CREATE OR REPLACE TABLE {table_name} AS
            SELECT * FROM read_csv_auto('{path}')
        """)

    def create_table_from_data(
        self, table_name: str, columns: list[str], data: list[tuple[Any, ...]]
    ) -> None:
        """Create a table from in-memory data."""
        if not data:
            raise ValueError("Cannot create table from empty data")

        placeholders = ", ".join(["?"] * len(columns))
        col_defs = ", ".join(columns)

        self.conn.execute(f"CREATE OR REPLACE TABLE {table_name} ({col_defs})")
        self.conn.executemany(
            f"INSERT INTO {table_name} VALUES ({placeholders})",
            data,
        )

    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists."""
        result = self.conn.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
            [table_name],
        )
        return result.fetchone()[0] > 0

    def get_table_schema(self, table_name: str) -> list[tuple[str, str]]:
        result = self.conn.execute(f"DESCRIBE {table_name}")
        return [(row[0], row[1]) for row in result.fetchall()]

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None

    def __enter__(self) -> "DuckDBExecutor":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()
