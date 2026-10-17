# MetricForge

Define metrics in YAML, query them via Python or CLI. DuckDB backend.

## Features

- **YAML-based metric definitions** - Version-controlled, human-readable metric DSL
- **4 metric types** - Simple, derived, ratio, and cumulative (running total, to-date, trailing window)
- **Queries across tables** - Metrics from different semantic models are aggregated separately and joined on shared dimensions
- **Time grain support** - Automatic DATE_TRUNC for day/week/month/quarter/year
- **SQL transparency** - Always see the generated SQL
- **DuckDB backend** - Fast, embedded OLAP database
- **Rich CLI** - List, query, validate, and show SQL for metrics

## Installation

```bash
# Clone the repository
git clone https://github.com/m4dd0ck/metricforge.git
cd metricforge

# Install with uv
uv sync
uv pip install -e .
```

## Quick Start

### 1. Generate sample data

```bash
uv run python data/generate_sample_data.py data/
uv run python data/init_db.py
```

### 2. List available metrics

```bash
uv run mf list metrics --dir metrics/
```

### 3. Query metrics

```bash
# Simple metric query
uv run mf query revenue --dir metrics/ --db data/metricforge.duckdb

# With dimensions
uv run mf query revenue,completed_orders --dir metrics/ --db data/metricforge.duckdb --dimensions country

# With time grain
uv run mf query revenue --dir metrics/ --db data/metricforge.duckdb --dimensions order_date --grain month

# Metrics from two tables (orders and sessions), joined by month
uv run mf query revenue,total_sessions,revenue_per_session --dir metrics/ --db data/metricforge.duckdb --dimensions order_date --grain month

# Show SQL without executing
uv run mf show-sql revenue,average_order_value --dir metrics/ --dimensions country
```

### 4. Validate definitions

```bash
uv run mf validate --dir metrics/
```

## Python SDK

```python
from metricforge import MetricStore

# Initialize
store = MetricStore("metrics", "data/metricforge.duckdb")

# Query metrics
result = store.query(
    metrics=["revenue", "average_order_value"],
    dimensions=["country"],
    time_grain="month",
)

for row in result.data:
    print(f"{row['country']}: ${row['revenue']:,.2f}")

# Get SQL without executing
sql = store.get_sql(metrics=["revenue"])
print(sql)
```

## Metric Definition DSL

```yaml
semantic_models:
  - name: orders
    table: orders
    primary_entity: order_id

    measures:
      - name: order_amount
        agg: sum
        expr: amount
      - name: order_count
        agg: count
        expr: order_id

    dimensions:
      - name: order_date
        type: time
        time_granularity: day
      - name: country
        type: categorical

metrics:
  # Simple metric
  - name: revenue
    type: simple
    type_params:
      measure: order_amount
    filter: "status = 'completed'"

  # Derived metric
  - name: average_order_value
    type: derived
    type_params:
      expr: "revenue / completed_orders"
      metrics:
        - revenue
        - completed_orders

  # Ratio metric
  - name: conversion_rate
    type: ratio
    type_params:
      numerator: purchases
      denominator: sessions

  # Cumulative metric (running total; add grain_to_date: month for month-to-date)
  - name: cumulative_revenue
    type: cumulative
    type_params:
      measure: order_amount

  # Trailing window: the last 7 days ending at each bucket
  - name: revenue_7d
    type: cumulative
    type_params:
      measure: order_amount
      window: 7 days
```

## CLI Commands

| Command | Description |
|---------|-------------|
| `mf list metrics` | List all defined metrics |
| `mf list dimensions` | List all dimensions |
| `mf list measures` | List all measures |
| `mf query <metrics>` | Query metrics with optional dimensions; `-o csv` writes RFC 4180 CSV |
| `mf show-sql <metrics>` | Show generated SQL |
| `mf validate` | Validate metric definitions |

### Query Options

```
--dir, -d       Metrics directory (default: ./metrics)
--db            DuckDB database path
--dimensions, -g Comma-separated dimensions
--grain, -t     Time grain: day, week, month, quarter, year
--filter, -f    Semicolon-separated filters
--start         Start date (YYYY-MM-DD)
--end           End date (YYYY-MM-DD)
--sql, -s       Show generated SQL
--output, -o    Output format: table, json, csv
--limit, -l     Maximum rows
```

`--start` and `--end` are inclusive. They apply to the time dimension in `--dimensions`, or,
when none is given, to the first time dimension the metric's model declares. A model without
a time dimension rejects a date range. A `filter` on a derived or ratio metric applies to
every metric it references.

## Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  YAML Metrics   │────▶│  MetricRegistry │────▶│   SQLCompiler   │
│  (definitions)  │     │  (in-memory)    │     │(SQLGlot formats)│
└─────────────────┘     └─────────────────┘     └────────┬────────┘
                                                         │
                                                         ▼
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  QueryResult    │◀────│   DuckDB        │◀────│  Generated SQL  │
│  (data + SQL)   │     │   Executor      │     │                 │
└─────────────────┘     └─────────────────┘     └─────────────────┘
```

`SQLCompiler` assembles the SELECT / FROM / WHERE / GROUP BY clauses as strings. SQLGlot
is used afterwards to parse and pretty-print the result (falling back to the raw string
if parsing fails), and to read the column names in raw filters; it does not build the query.

### Queries across tables

Metrics on one table compile to a single `SELECT ... FROM <table>`. When the metrics in a
query come from semantic models on different tables, each model is aggregated on its own
(a CTE grouped by the query's dimensions) and the aggregates are `FULL OUTER JOIN`ed on
those dimensions. Aggregating first means no fan-out: `revenue / total_sessions` by country
is each country's revenue over its sessions, and a country with sessions but no orders
still appears, with `NULL` revenue.

```sql
WITH orders_q AS (
  SELECT country AS country, SUM(CASE WHEN status = 'completed' THEN amount END) AS revenue
  FROM orders GROUP BY country
), sessions_q AS (
  SELECT country AS country, COUNT(session_id) AS total_sessions
  FROM sessions GROUP BY country
)
SELECT
  COALESCE(orders_q.country, sessions_q.country) AS country,
  orders_q.revenue AS revenue,
  (orders_q.revenue) * 1.0 / NULLIF((sessions_q.total_sessions), 0) AS revenue_per_session
FROM orders_q
FULL OUTER JOIN sessions_q ON orders_q.country IS NOT DISTINCT FROM sessions_q.country
ORDER BY country
```

The rules, each checked before any SQL runs (`MultiModelQueryError` / `NoTimeDimensionError`):

- A categorical dimension must be declared, under the same name, by every model in the
  query; it is the join key. Declaring `country` on both `orders` and `sessions` is how
  you say they mean the same thing.
- A time dimension names the axis. A model that does not declare it contributes its own
  first time dimension under that name, so `revenue, total_sessions` by `order_date` buckets
  sessions by `session_date`.
- A raw `--filter` is pushed into every model's aggregate, so it may only reference columns
  every model declares. A filter that belongs to one table goes on the metric definition.
- Joins through foreign keys to dimension tables (`customer.region`) are not supported.

## Project Structure

```
metricforge/
├── src/metricforge/
│   ├── models/          # Pydantic models
│   ├── parser/          # YAML loading
│   ├── compiler/        # SQL generation
│   ├── executor/        # DuckDB backend
│   └── cli/             # Typer CLI
├── metrics/             # Sample metric definitions
├── data/                # Sample data
├── tests/               # Test suite
└── examples/            # Usage examples
```

## Running Tests

```bash
uv run pytest tests/ -v
```

## Metric Types

| Type | Description | Use Case |
|------|-------------|----------|
| **simple** | Single measure aggregation | `SUM(amount)`, `COUNT(*)` |
| **derived** | Calculation from other metrics | `revenue / orders` |
| **ratio** | Two metrics as numerator/denominator | `completed / total` |
| **cumulative** | Running total over the queried time dimension; `grain_to_date: month` restarts it each month; `window: 7 days` makes it a trailing total | Month-to-date revenue, 7-day revenue |

Cumulative metrics compile to `SUM(<aggregate>) OVER (PARTITION BY <other dimensions>
ORDER BY <time dimension> ROWS UNBOUNDED PRECEDING)`. Without a time dimension in the query
a running total returns the plain total.

A `window` (`<count> <day|week|month|quarter|year>`) turns the frame into
`RANGE BETWEEN (INTERVAL <window> - INTERVAL 1 <grain>) PRECEDING AND CURRENT ROW`: the window
covers calendar time ending at the current bucket, so `7 days` by day is seven buckets even
when days are missing, and `1 month` by week is four weeks. A window finer than the query's
grain (`7 days` by month) is refused, and a windowed metric needs a time dimension in the
query. `window` and `grain_to_date` are mutually exclusive.

The date range (`--start`/`--end`) filters rows before aggregation, so a running total starts
at `--start` and the first buckets of a trailing window inside the range are partial. Drop
`--start` to see the all-time total.

## Aggregation Types

- `sum` - Sum of values
- `count` - Count of rows
- `count_distinct` - Count of unique values
- `avg` - Average
- `min` / `max` - Minimum / Maximum

## License

MIT
