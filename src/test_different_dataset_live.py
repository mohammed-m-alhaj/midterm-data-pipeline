from __future__ import annotations

import json
import logging
import random
import sys
import time
from pathlib import Path
from pymongo import MongoClient

from bootstrap import ensure_project_root

ensure_project_root()

from config.settings import (
    MONGO_DATABASE,
    MONGO_TIMEOUT_MS,
    MONGO_URI,
    VALIDATED_COLLECTION,
)
from src.aggregations import AGGREGATION_REGISTRY
from src.api import app
from src.generate_phase2_dynamic_dataset import generate_dynamic_dataset
from src.jobs import run_job
from src.materialized_views import (
    MV_CUSTOMER_METRICS,
    MV_DAILY_SALES,
    refresh_all_materialized_views,
    refresh_daily_sales_summary,
)
from src.queries import QUERY_REGISTRY, create_phase2_indexes, run_explain_comparison
from starlette.testclient import TestClient


def run_different_data_validation():
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass

    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=MONGO_TIMEOUT_MS)
    db = client[MONGO_DATABASE]

    # Generate a brand new, random-sized dataset with randomized prefix
    row_count = random.randint(350, 480)
    print(f"[*] Generating new un-seen dataset with {row_count} rows...")
    new_csv = generate_dynamic_dataset(row_count)
    print(f"[*] Generated file: {new_csv.name} ({new_csv.stat().st_size} bytes)")

    # Ingest using Phase 1 File Router + batch_loader + ELT
    from src.file_router import route_file
    from src.batch_loader import load_csv_to_raw
    from src.elt_pipeline import process_run

    decision = route_file(new_csv)
    print(f"[*] Router decision: engine={decision['engine']}, size={decision['file_size_mb']} MB")
    load_stats = load_csv_to_raw(decision["file_path"], decision["run_id"])
    print(f"[*] Raw load stats: inserted={load_stats['raw_loaded']}")

    elt_stats = process_run(decision["run_id"], decision["file_path"])
    print(f"[*] ELT stats: raw_count={elt_stats['raw_count']}, valid_count={elt_stats['valid_count']}, corrected={elt_stats['corrected_count']}, quarantine={elt_stats['quarantine_count']}")
    assert (elt_stats['valid_count'] + elt_stats['corrected_count'] + elt_stats['quarantine_count']) == elt_stats['raw_count']

    # 1. Queries with dynamic random data
    print("\n[1] Verifying Queries on new dynamic data...")
    create_phase2_indexes()
    
    # Pick a random customer from this new batch
    sample_doc = db[VALIDATED_COLLECTION].find_one({"run_id": decision["run_id"]})
    test_cust = sample_doc["customer_id"]
    test_city = sample_doc["city"]

    q1_res = QUERY_REGISTRY["orders_by_customer"](customer_id=test_cust)
    print(f"  ✔ orders_by_customer for '{test_cust}': returned {len(q1_res['results'])} orders")
    assert len(q1_res["results"]) >= 1

    q2_res = QUERY_REGISTRY["orders_by_city_status"](city=test_city, status="تم الدفع")
    print(f"  ✔ orders_by_city_status for '{test_city}': returned {len(q2_res['results'])} orders")

    # 2. Aggregations on new dynamic data
    print("\n[2] Verifying Aggregations on new dynamic data...")
    for name, fn in AGGREGATION_REGISTRY.items():
        res = fn()
        assert len(res) > 0, f"Aggregation {name} returned empty results!"
        print(f"  ✔ Aggregation '{name}': returned {len(res)} aggregated records")

    # 3. Materialized Views Incremental Refresh
    print("\n[3] Verifying Materialized Views Incremental Refresh on new dynamic data...")
    target_date = sample_doc["order_date"][:10]
    print(f"  -> Refreshing daily sales targeting date '{target_date}'...")
    mv_res = refresh_daily_sales_summary(incremental=True, target_dates=[target_date])
    assert mv_res["status"] in ("SUCCESS", "UP_TO_DATE")
    print(f"  ✔ MV Incremental Refresh: mode={mv_res['mode']}, affected_keys={mv_res['affected_keys']}, duration={mv_res['duration_ms']}ms")

    # 4. Jobs Execution
    print("\n[4] Verifying Scheduled Jobs Execution...")
    j1 = run_job("refresh_materialized_views", incremental=True)
    assert j1["status"] == "SUCCESS"
    j2 = run_job("generate_analytics_snapshot")
    assert j2["status"] == "SUCCESS"
    print(f"  ✔ Job 1 and Job 2 executed with status SUCCESS")

    # 5. FastAPI Endpoints Live Call
    print("\n[5] Verifying API Live Calls on new data...")
    test_client = TestClient(app)
    r_health = test_client.get("/health")
    assert r_health.status_code == 200
    r_agg = test_client.get("/aggregations/sales_by_city")
    assert r_agg.status_code == 200
    assert r_agg.json()["records_returned"] > 0
    print(f"  ✔ API test passed with live data: records_returned={r_agg.json()['records_returned']}")

    print("\n\033[92m✔ ALL DIFFERENT-DATA LIVE TESTS PASSED 100% WITHOUT HARDCODED ASSUMPTIONS!\033[0m")


if __name__ == "__main__":
    run_different_data_validation()
