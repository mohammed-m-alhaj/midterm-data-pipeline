from __future__ import annotations

import json
import logging
import sys
import time
from pathlib import Path
from pymongo import MongoClient
from starlette.testclient import TestClient

from bootstrap import PROJECT_ROOT, ensure_project_root

ensure_project_root()

from config.settings import (
    MONGO_DATABASE,
    MONGO_TIMEOUT_MS,
    MONGO_URI,
    REPORTS_DIR,
    VALIDATED_COLLECTION,
    ensure_directories,
)
from src.aggregations import AGGREGATION_REGISTRY
from src.api import app
from src.generate_phase2_dynamic_dataset import generate_dynamic_dataset
from src.jobs import get_job_logs, list_registered_jobs, run_job
from src.materialized_views import (
    MV_CUSTOMER_METRICS,
    MV_DAILY_SALES,
    list_materialized_views_status,
    refresh_all_materialized_views,
    refresh_daily_sales_summary,
)
from src.mongo_setup import setup_mongodb
from src.queries import (
    QUERY_REGISTRY,
    create_phase2_indexes,
    drop_phase2_indexes,
    list_current_indexes,
    run_explain_comparison,
)

EVIDENCE_DIR = REPORTS_DIR / "phase2_evidence"


def ensure_evidence_dir():
    ensure_directories()
    EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)


def run_all_benchmarks():
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass

    ensure_evidence_dir()
    setup_mongodb()

    print("\033[96m" + "=" * 75 + "\033[0m")
    print("\033[1m\033[92mSTARTING PHASE 2 RIGOROUS VERIFICATION & EVIDENCE GENERATION\033[0m")
    print("\033[96m" + "=" * 75 + "\033[0m\n")

    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=MONGO_TIMEOUT_MS)
    db = client[MONGO_DATABASE]

    # Step 0: Ensure we have adequate dynamic test data in orders_validated
    curr_validated = db[VALIDATED_COLLECTION].count_documents({})
    print(f"[*] Current records in orders_validated: {curr_validated}")
    if curr_validated < 100:
        print("[*] Generating and ingesting dynamic dataset via Phase 1 File Router...")
        dyn_file = generate_dynamic_dataset(300)
        from src.file_router import route_file
        from src.batch_loader import load_csv_to_raw
        from src.elt_pipeline import process_run

        decision = route_file(dyn_file)
        load_csv_to_raw(decision["file_path"], decision["run_id"])
        process_run(decision["run_id"], decision["file_path"])
        curr_validated = db[VALIDATED_COLLECTION].count_documents({})
        print(f"[*] New records count in orders_validated: {curr_validated}")

    # -------------------------------------------------------------------------
    # TEST 1: Queries, Indexes & Explain (Before / After)
    # -------------------------------------------------------------------------
    print("\n\033[1m\033[93m[1/5] Testing 5 Queries & Explain Stats Before/After Indexes...\033[0m")
    
    # We will run explain on 3 queries before and after
    sample_doc = db[VALIDATED_COLLECTION].find_one({"city": {"$ne": None}, "customer_id": {"$ne": None}})
    test_city = sample_doc.get("city", "صنعاء") if sample_doc else "صنعاء"
    test_cust = sample_doc.get("customer_id", "CUST-0001") if sample_doc else "CUST-0001"

    explain_targets = [
        ("orders_by_city_status", {"city": test_city, "status": "تم الدفع", "limit": 50}),
        ("orders_by_customer", {"customer_id": test_cust, "limit": 50}),
        ("high_value_orders_by_date", {"min_amount": 3000.0, "start_date": "2026-01-01", "end_date": "2026-12-31", "limit": 50}),
    ]

    explain_results = []
    for q_name, q_kwargs in explain_targets:
        print(f"  -> Comparing explain for '{q_name}'...")
        comp = run_explain_comparison(q_name, q_kwargs)
        explain_results.append(comp)
        before_stage = comp["before_index"]["stage"]
        after_stage = comp["after_index"]["stage"]
        time_before = comp["before_index"]["executionTimeMillis"]
        time_after = comp["after_index"]["executionTimeMillis"]
        docs_before = comp["before_index"]["totalDocsExamined"]
        docs_after = comp["after_index"]["totalDocsExamined"]
        print(f"     [Before] Stage: {before_stage}, Examined: {docs_before} docs, Time: {time_before}ms")
        print(f"     [After ] Stage: {after_stage}, Examined: {docs_after} docs, Time: {time_after}ms")
        print(f"     [Gain  ] Transition: {comp['performance_gain']['stage_transition']}, Docs Reduction: {comp['performance_gain']['docs_examined_reduction']}")

    # Save explain evidence JSON
    with open(EVIDENCE_DIR / "explain_before_after.json", "w", encoding="utf-8") as f:
        json.dump(explain_results, f, ensure_ascii=False, indent=2, default=str)

    # Save explain comparison Markdown
    md_content = ["# 📊 MongoDB Explain (executionStats) Before vs After Indexes\n"]
    md_content.append("| Query Name | Stage Before | Stage After | Docs Examined (Before) | Docs Examined (After) | Execution Time (Before) | Execution Time (After) |")
    md_content.append("|---|:---:|:---:|:---:|:---:|:---:|:---:|")
    for r in explain_results:
        b = r["before_index"]
        a = r["after_index"]
        md_content.append(f"| `{r['query_name']}` | `{b['stage']}` | **`{a['stage']}`** | {b['totalDocsExamined']} | **{a['totalDocsExamined']}** | {b['executionTimeMillis']} ms | **{a['executionTimeMillis']} ms** |")
    
    with open(EVIDENCE_DIR / "explain_comparison.md", "w", encoding="utf-8") as f:
        f.write("\n".join(md_content) + "\n")

    # -------------------------------------------------------------------------
    # TEST 2: Aggregation Reports
    # -------------------------------------------------------------------------
    print("\n\033[1m\033[93m[2/5] Testing 5 Aggregation Reports...\033[0m")
    agg_results = {}
    for agg_name, agg_fn in AGGREGATION_REGISTRY.items():
        res = agg_fn()
        agg_results[agg_name] = {
            "record_count": len(res),
            "sample": res[:3],
        }
        print(f"  ✔ Aggregation '{agg_name}': generated {len(res)} aggregated buckets")

    with open(EVIDENCE_DIR / "aggregations_results.json", "w", encoding="utf-8") as f:
        json.dump(agg_results, f, ensure_ascii=False, indent=2, default=str)

    # -------------------------------------------------------------------------
    # TEST 3: Materialized Views & Incremental Refresh
    # -------------------------------------------------------------------------
    print("\n\033[1m\033[93m[3/5] Testing Materialized Views & Incremental Refresh...\033[0m")
    # 1. Full Build
    full_refresh_res = refresh_all_materialized_views(incremental=False)
    print(f"  ✔ Initial Full Build: Daily Sales ({full_refresh_res['daily_sales_summary']['total_documents_in_view']} docs), Customer Metrics ({full_refresh_res['customer_metrics']['total_documents_in_view']} docs)")

    # 2. Simulate an update on a specific date to test Incremental refresh
    target_date = "2026-05-15"
    print(f"  -> Performing incremental refresh targeting date '{target_date}'...")
    inc_res = refresh_daily_sales_summary(incremental=True, target_dates=[target_date])
    print(f"  ✔ Incremental Refresh result: mode={inc_res['mode']}, affected_keys={inc_res['affected_keys']}, duration={inc_res['duration_ms']}ms")

    mv_status = list_materialized_views_status()
    with open(EVIDENCE_DIR / "materialized_views_evidence.json", "w", encoding="utf-8") as f:
        json.dump({
            "full_refresh": full_refresh_res,
            "incremental_refresh_test": inc_res,
            "views_status": mv_status,
        }, f, ensure_ascii=False, indent=2, default=str)

    # -------------------------------------------------------------------------
    # TEST 4: Scheduled Jobs & MongoDB Logging
    # -------------------------------------------------------------------------
    print("\n\033[1m\033[93m[4/5] Testing Scheduled Jobs Execution & Logging...\033[0m")
    job1_log = run_job("refresh_materialized_views", incremental=True)
    print(f"  ✔ Job 1 ('refresh_materialized_views'): Status={job1_log['status']}, Duration={job1_log['duration_ms']}ms")

    job2_log = run_job("generate_analytics_snapshot")
    print(f"  ✔ Job 2 ('generate_analytics_snapshot'): Status={job2_log['status']}, Duration={job2_log['duration_ms']}ms")

    all_logs = get_job_logs(limit=10)
    with open(EVIDENCE_DIR / "jobs_execution_logs.json", "w", encoding="utf-8") as f:
        json.dump(all_logs, f, ensure_ascii=False, indent=2, default=str)

    # -------------------------------------------------------------------------
    # TEST 5: FastAPI Endpoints Verification via TestClient
    # -------------------------------------------------------------------------
    print("\n\033[1m\033[93m[5/5] Testing FastAPI Endpoints...\033[0m")
    test_client = TestClient(app)

    endpoints_tested = {}

    # 1. GET /health
    r_health = test_client.get("/health")
    endpoints_tested["GET /health"] = {"status_code": r_health.status_code, "data": r_health.json()}
    assert r_health.status_code == 200

    # 2. POST /indexes
    r_idx = test_client.post("/indexes")
    endpoints_tested["POST /indexes"] = {"status_code": r_idx.status_code, "data": r_idx.json()}
    assert r_idx.status_code == 200

    # 3. GET /queries
    r_q = test_client.get("/queries")
    endpoints_tested["GET /queries"] = {"status_code": r_q.status_code, "queries_count": len(r_q.json()["queries"])}
    assert r_q.status_code == 200

    # 4. GET /queries/orders_by_city_status
    r_q_city = test_client.get("/queries/orders_by_city_status?city=صنعاء&status=تم%20الدفع")
    endpoints_tested["GET /queries/orders_by_city_status"] = {"status_code": r_q_city.status_code, "count": r_q_city.json().get("count")}
    assert r_q_city.status_code == 200

    # 5. GET /queries/orders_by_city_status with explain=true
    r_q_exp = test_client.get("/queries/orders_by_city_status?city=صنعاء&status=تم%20الدفع&explain=true")
    endpoints_tested["GET /queries/orders_by_city_status?explain=true"] = {"status_code": r_q_exp.status_code, "has_execution_stats": "executionStats" in r_q_exp.json()}
    assert r_q_exp.status_code == 200

    # 6. GET /aggregations
    r_aggs = test_client.get("/aggregations")
    endpoints_tested["GET /aggregations"] = {"status_code": r_aggs.status_code, "aggregations_count": len(r_aggs.json()["aggregations"])}
    assert r_aggs.status_code == 200

    # 7. GET /aggregations/sales_by_city
    r_agg_city = test_client.get("/aggregations/sales_by_city?limit=5")
    endpoints_tested["GET /aggregations/sales_by_city"] = {"status_code": r_agg_city.status_code, "records_returned": r_agg_city.json()["records_returned"]}
    assert r_agg_city.status_code == 200

    # 8. POST /refresh-mv
    r_ref = test_client.post("/refresh-mv", json={"incremental": True})
    endpoints_tested["POST /refresh-mv"] = {"status_code": r_ref.status_code, "status": r_ref.json()["status"]}
    assert r_ref.status_code == 200

    # 9. GET /jobs
    r_jobs = test_client.get("/jobs")
    endpoints_tested["GET /jobs"] = {"status_code": r_jobs.status_code, "jobs_count": len(r_jobs.json()["registered_jobs"])}
    assert r_jobs.status_code == 200

    # 10. POST /jobs/generate_analytics_snapshot/run
    r_run = test_client.post("/jobs/generate_analytics_snapshot/run")
    endpoints_tested["POST /jobs/generate_analytics_snapshot/run"] = {"status_code": r_run.status_code, "job_status": r_run.json()["execution_log"]["status"]}
    assert r_run.status_code == 200

    with open(EVIDENCE_DIR / "api_endpoints_test_results.json", "w", encoding="utf-8") as f:
        json.dump(endpoints_tested, f, ensure_ascii=False, indent=2, default=str)

    print("  ✔ All 10 API endpoints tested successfully with 200 OK responses!")

    print("\n\033[96m" + "=" * 75 + "\033[0m")
    print("\033[1m\033[92mALL PHASE 2 BENCHMARKS COMPLETED WITH 100% SUCCESS!\033[0m")
    print(f"Evidence files saved to: {EVIDENCE_DIR}")
    print("\033[96m" + "=" * 75 + "\033[0m\n")


if __name__ == "__main__":
    run_all_benchmarks()
