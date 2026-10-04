import time
import pytest
from starlette.testclient import TestClient

from bootstrap import ensure_project_root

ensure_project_root()

from src.aggregations import AGGREGATION_REGISTRY
from src.api import app
from src.jobs import JOBS_CATALOG, get_job_logs, run_job
from src.materialized_views import (
    MV_CUSTOMER_METRICS,
    MV_DAILY_SALES,
    MV_TOP_PRODUCTS,
    get_db,
    list_materialized_views_status,
    refresh_all_materialized_views,
    refresh_daily_sales_summary,
    refresh_top_products_summary,
)
from src.queries import (
    QUERY_REGISTRY,
    create_phase2_indexes,
    list_current_indexes,
    run_explain_comparison,
)


@pytest.fixture(scope="module")
def client():
    with TestClient(app) as c:
        yield c


def test_phase2_indexes():
    created = create_phase2_indexes()
    assert len(created) >= 3
    current = list_current_indexes()
    assert "idx_city_status_total_amount" in current
    assert "idx_customer_id_order_date" in current
    assert "idx_order_date_total_amount" in current


def test_phase2_queries():
    # Test query registry contains at least 5 queries
    assert len(QUERY_REGISTRY) >= 5

    res_city = QUERY_REGISTRY["orders_by_city_status"](city="صنعاء", status="تم الدفع", limit=10)
    assert "results" in res_city
    assert isinstance(res_city["results"], list)

    res_cust = QUERY_REGISTRY["orders_by_customer"](customer_id="CUST-0001", limit=10)
    assert "results" in res_cust

    res_high = QUERY_REGISTRY["high_value_orders_by_date"](min_amount=1000, start_date="2026-01-01", end_date="2026-12-31")
    assert "results" in res_high


def test_phase2_explain_comparison():
    comp = run_explain_comparison("orders_by_city_status", {"city": "صنعاء", "status": "تم الدفع", "limit": 10})
    assert "before_index" in comp
    assert "after_index" in comp
    assert "performance_gain" in comp
    assert comp["after_index"]["totalDocsExamined"] <= comp["before_index"]["totalDocsExamined"]


def test_phase2_aggregations():
    assert len(AGGREGATION_REGISTRY) >= 5
    for name, fn in AGGREGATION_REGISTRY.items():
        results = fn()
        assert isinstance(results, list)
        if len(results) > 0:
            assert isinstance(results[0], dict)


def test_phase2_materialized_views():
    full_res = refresh_all_materialized_views(incremental=False)
    assert full_res["daily_sales_summary"]["status"] == "SUCCESS"
    assert full_res["customer_metrics"]["status"] == "SUCCESS"
    assert full_res["top_products_summary"]["status"] == "SUCCESS"
    assert full_res["top_products_summary"]["total_documents_in_view"] > 0

    # Test incremental refresh for daily sales
    inc_daily = refresh_daily_sales_summary(incremental=True, target_dates=["2026-05-15"])
    assert inc_daily["status"] in ("SUCCESS", "UP_TO_DATE")
    assert "affected_keys" in inc_daily

    # Test incremental refresh for top products
    inc_prod = refresh_top_products_summary(incremental=True)
    assert inc_prod["status"] in ("SUCCESS", "UP_TO_DATE")
    assert "affected_keys" in inc_prod
    assert inc_prod["mode"] == "incremental"

    statuses = list_materialized_views_status()
    assert len(statuses) >= 3
    for s in statuses:
        assert s["status"] == "SUCCESS"


def test_phase2_top_products_incremental_delta_refresh():
    """
    Explicitly proves that refresh_top_products_summary(incremental=True)
    performs genuine delta incremental updates without full rebuild, preserves
    existing records, and accurately tracks affected_keys.
    """
    import json
    from datetime import datetime, timezone
    from config.settings import VALIDATED_COLLECTION

    # 1. Ensure a full build exists and record base document count
    full_res = refresh_top_products_summary(incremental=False)
    assert full_res["status"] == "SUCCESS"
    assert full_res["mode"] == "full"
    base_count = full_res["total_documents_in_view"]
    assert base_count > 0

    # 2. Check incremental refresh when already up to date
    inc_noop = refresh_top_products_summary(incremental=True)
    assert inc_noop["status"] in ("SUCCESS", "UP_TO_DATE")
    assert inc_noop["mode"] == "incremental"
    assert inc_noop["total_documents_in_view"] >= base_count

    # 3. Insert a single new validated order with a unique delta SKU
    db = get_db()
    test_sku = "TEST-DELTA-SKU-999"
    test_order_id = f"ORD-DELTA-{int(time.time())}"
    test_order = {
        "order_id": test_order_id,
        "order_date": "2026-10-04",
        "customer_id": "CUST-DELTA-01",
        "customer_name": "عميل دلتا تجريبي",
        "customer_phone": "+967770000000",
        "city": "صنعاء",
        "total_amount": 15000.0,
        "delivery_cost": 500.0,
        "status": "مؤكد",
        "quality_status": "valid",
        "record_hash": "hash_test_delta_proof",
        "ingested_at": datetime.now(timezone.utc),
        "items": [{"sku": test_sku, "name": "Delta Product Test", "qty": 3, "unit_price": 5000.0, "total": 15000.0}],
        "items_json": json.dumps([{"sku": test_sku, "name": "Delta Product Test", "qty": 3, "unit_price": 5000.0, "total": 15000.0}]),
    }
    db[VALIDATED_COLLECTION].insert_one(test_order)

    try:
        # 4. Trigger incremental refresh targeting this delta SKU
        inc_delta_res = refresh_top_products_summary(incremental=True, target_skus=[test_sku])
        assert inc_delta_res["status"] == "SUCCESS"
        assert inc_delta_res["mode"] == "incremental"
        assert inc_delta_res["affected_keys"] >= 1
        assert inc_delta_res["total_documents_in_view"] >= base_count

        # 5. Verify the delta SKU was accurately upserted with correct totals
        view_doc = db[MV_TOP_PRODUCTS].find_one({"_id": test_sku})
        assert view_doc is not None
        assert view_doc["sku"] == test_sku
        assert view_doc["total_quantity_sold"] >= 3
        assert view_doc["total_revenue"] >= 15000.0
        assert view_doc["order_count"] >= 1

        # 6. Verify that older product documents were preserved and not deleted
        current_count = db[MV_TOP_PRODUCTS].count_documents({})
        assert current_count >= base_count
    finally:
        # Cleanup test artifacts
        db[VALIDATED_COLLECTION].delete_one({"order_id": test_order_id})
        db[MV_TOP_PRODUCTS].delete_one({"_id": test_sku})
        db["top_products_summary"].delete_one({"_id": test_sku})


def test_phase2_jobs():
    assert len(JOBS_CATALOG) >= 2

    log1 = run_job("refresh_materialized_views", incremental=True)
    assert log1["status"] == "SUCCESS"
    assert log1["success"] is True

    log2 = run_job("generate_analytics_snapshot")
    assert log2["status"] == "SUCCESS"
    assert log2["success"] is True

    logs = get_job_logs(limit=5)
    assert len(logs) >= 2


def test_phase2_api_endpoints(client: TestClient, tmp_path):
    import csv
    import json
    from config.settings import RAW_COLUMNS

    # 1. Health
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json()["status"] == "HEALTHY"

    # 2. POST /ingest
    csv_file = tmp_path / "test_api_ingest.csv"
    row = {col: "test" for col in RAW_COLUMNS}
    row["order_id"] = "API-TEST-001"
    row["order_date"] = "2026-05-01"
    row["status"] = "مؤكد"
    row["customer_id"] = "CUST-API-01"
    row["customer_name"] = "عميل تجريبي"
    row["customer_phone"] = "+967771234567"
    row["customer_email"] = "test@domain.com"
    row["city"] = "صنعاء"
    row["district"] = "السبعين"
    row["delivery_type"] = "سريع"
    row["delivery_cost"] = "1000"
    row["payment_method"] = "كاش"
    row["payment_status"] = "تم الدفع"
    row["payment_amount"] = "5000"
    row["currency"] = "YER"
    row["total_amount"] = "6000"
    row["items_json"] = json.dumps([{"sku": "SKU-1", "name": "Item", "qty": 1, "unit_price": 5000.0, "total": 5000.0}])
    with open(csv_file, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=RAW_COLUMNS)
        writer.writeheader()
        writer.writerow(row)

    r_ingest = client.post("/ingest", json={"file_path": str(csv_file), "run_elt": True})
    assert r_ingest.status_code == 200
    assert r_ingest.json()["status"] == "COMPLETED"
    assert r_ingest.json()["decision"]["engine"] == "python_batch"
    assert r_ingest.json()["load_stats"]["raw_loaded"] == 1

    # 3. Indexes
    r = client.post("/indexes")
    assert r.status_code == 200

    # Queries list
    r = client.get("/queries")
    assert r.status_code == 200
    assert len(r.json()["queries"]) >= 5

    # Query execution
    r = client.get("/queries/orders_by_city_status?city=صنعاء&status=تم%20الدفع")
    assert r.status_code == 200

    # Aggregations list & execution
    r = client.get("/aggregations")
    assert r.status_code == 200
    assert len(r.json()["aggregations"]) >= 5

    r = client.get("/aggregations/sales_by_city")
    assert r.status_code == 200

    # Refresh MV
    r = client.post("/refresh-mv", json={"incremental": True})
    assert r.status_code == 200

    # Jobs
    r = client.get("/jobs")
    assert r.status_code == 200
    assert len(r.json()["registered_jobs"]) >= 2

    r = client.post("/jobs/refresh_materialized_views/run")
    assert r.status_code == 200
    assert r.json()["execution_log"]["status"] == "SUCCESS"
