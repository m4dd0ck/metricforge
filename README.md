# MetricForge

**Local-first semantic layer for e-commerce analytics.**

Define your metrics once in YAML, query them anywhere — via Python, CLI, or SQL.

## Features

- **YAML-based metric definitions** - Version-controlled, human-readable metric DSL
- **4 metric types** - Simple, derived, ratio, and cumulative metrics
- **Time grain support** - Automatic DATE_TRUNC for day/week/month/quarter/year
- **SQL transparency** - Always see the generated SQL
- **DuckDB backend** - Fast, embedded OLAP database
- **Rich CLI** - List, query, validate, and inspect metrics

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

  # Cumulative metric
  - name: cumulative_revenue
    type: cumulative
    type_params:
      measure: order_amount
```

## CLI Commands

| Command | Description |
|---------|-------------|
| `mf list metrics` | List all defined metrics |
| `mf list dimensions` | List all dimensions |
| `mf list measures` | List all measures |
| `mf query <metrics>` | Query metrics with optional dimensions |
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

## Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  YAML Metrics   │────▶│  MetricRegistry │────▶│   SQLCompiler   │
│  (definitions)  │     │  (in-memory)    │     │   (SQLGlot)     │
└─────────────────┘     └─────────────────┘     └────────┬────────┘
                                                         │
                                                         ▼
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  QueryResult    │◀────│   DuckDB        │◀────│  Generated SQL  │
│  (data + SQL)   │     │   Executor      │     │                 │
└─────────────────┘     └─────────────────┘     └─────────────────┘
```

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
| **cumulative** | Running total over time | Month-to-date revenue |

## Aggregation Types

- `sum` - Sum of values
- `count` - Count of rows
- `count_distinct` - Count of unique values
- `avg` - Average
- `min` / `max` - Minimum / Maximum

## License

MIT
