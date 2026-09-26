"""
Verification script for StreamForge Real-Time Pipeline Ingestion & Dashboard Queries.
"""
import time
import threading
from dashboard.pipeline_runner import BackgroundPipelineSupervisor
from dashboard.db_queries import get_executive_kpis, get_geographic_distribution, get_live_ingest_rate

def test_live_pipeline_flow():
    supervisor = BackgroundPipelineSupervisor(rate_per_sec=5.0, batch_duration_sec=1)
    t = threading.Thread(target=supervisor.run_loop, daemon=True)
    t.start()

    time.sleep(3.5)

    kpis = get_executive_kpis()
    geo = get_geographic_distribution()
    rate = get_live_ingest_rate()

    supervisor.stop()

    print(f"Total Orders: {kpis['total_orders']}")
    print(f"Total Revenue: ${kpis['total_revenue']:,.2f}")
    print(f"Active Cities: {len(geo)}")
    print(f"Computed Live Ingest Rate: {rate} eps")

    assert kpis['total_orders'] > 0, "Expected non-zero total orders"
    assert len(geo) > 0, "Expected active geographic locations"
    print("[SUCCESS] Real-time order flow and dashboard metrics verified successfully!")

if __name__ == "__main__":
    test_live_pipeline_flow()
