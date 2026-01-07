"""Basic usage example for MetricForge."""

import sys
from pathlib import Path

# Add parent to path for development
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from metricforge import MetricStore


def main():
    """Demonstrate MetricForge capabilities."""
    # Initialize store with sample data
    store = MetricStore("metrics")

    # Load sample data into DuckDB
    store.executor.load_parquet("orders", "data/orders.parquet")
    store.executor.load_parquet("sessions", "data/sessions.parquet")

    print("=" * 60)
    print("MetricForge E-Commerce Analytics Demo")
    print("=" * 60)

    # 1. Simple metric query
    print("\n1. Total Revenue:")
    result = store.query(metrics=["revenue"])
    print(f"   ${result.data[0]['revenue']:,.2f}")

    # 2. Multiple metrics
    print("\n2. Order Summary:")
    result = store.query(metrics=["revenue", "completed_orders", "average_order_value"])
    row = result.data[0]
    print(f"   Revenue: ${row['revenue']:,.2f}")
    print(f"   Completed Orders: {row['completed_orders']}")
    print(f"   Average Order Value: ${row['average_order_value']:.2f}")

    # 3. Metrics by dimension
    print("\n3. Revenue by Country:")
    result = store.query(
        metrics=["revenue", "completed_orders"],
        dimensions=["country"],
        limit=5,
    )
    for row in result.data:
        print(f"   {row['country']}: ${row['revenue']:,.2f} ({row['completed_orders']} orders)")

    # 4. Metrics by time
    print("\n4. Monthly Revenue (first 6 months):")
    result = store.query(
        metrics=["revenue"],
        dimensions=["order_date"],
        time_grain="month",
        start_date="2024-01-01",
        end_date="2024-06-30",
    )
    for row in result.data:
        month = row["order_date"].strftime("%Y-%m") if hasattr(row["order_date"], "strftime") else str(row["order_date"])[:7]
        print(f"   {month}: ${row['revenue']:,.2f}")

    # 5. Ratio metrics
    print("\n5. Order Rates:")
    result = store.query(metrics=["order_completion_rate", "cancellation_rate"])
    row = result.data[0]
    print(f"   Completion Rate: {row['order_completion_rate'] * 100:.1f}%")
    print(f"   Cancellation Rate: {row['cancellation_rate'] * 100:.1f}%")

    # 6. Show generated SQL
    print("\n6. Generated SQL for Average Order Value:")
    sql = store.get_sql(metrics=["average_order_value"])
    print(f"   {sql}")

    # 7. Session metrics
    print("\n7. Session Analytics:")
    result = store.query(metrics=["total_sessions", "total_page_views", "pages_per_session"])
    row = result.data[0]
    print(f"   Total Sessions: {row['total_sessions']:,}")
    print(f"   Total Page Views: {row['total_page_views']:,}")
    print(f"   Pages per Session: {row['pages_per_session']:.2f}")

    # 8. Traffic source breakdown
    print("\n8. Sessions by Traffic Source:")
    result = store.query(
        metrics=["total_sessions", "visitors"],
        dimensions=["traffic_source"],
    )
    for row in result.data:
        print(f"   {row['traffic_source']}: {row['total_sessions']} sessions, {row['visitors']} unique visitors")

    print("\n" + "=" * 60)
    print("Demo complete!")
    print("=" * 60)

    store.close()


if __name__ == "__main__":
    main()
