from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Query, status
from pydantic import BaseModel, Field
from pymongo import MongoClient

import sys
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent
_src_dir = Path(__file__).resolve().parent
for _p in (_project_root, _src_dir):
    _ps = str(_p)
    if _ps not in sys.path:
        sys.path.insert(0, _ps)

from bootstrap import PROJECT_ROOT, ensure_project_root

ensure_project_root()

from config.settings import (
    MONGO_DATABASE,
    MONGO_TIMEOUT_MS,
    MONGO_URI,
    QUARANTINE_COLLECTION,
    RAW_COLLECTION,
    VALIDATED_COLLECTION,
    ensure_directories,
)
from src.aggregations import AGGREGATION_REGISTRY
from src.file_router import route_file
from src.jobs import (
    JOBS_CATALOG,
    BackgroundJobScheduler,
    get_job_logs,
    list_registered_jobs,
    run_job,
)
from src.materialized_views import (
    MV_CUSTOMER_METRICS,
    MV_DAILY_SALES,
    MV_TOP_PRODUCTS,
    list_materialized_views_status,
    refresh_all_materialized_views,
    refresh_customer_metrics,
    refresh_daily_sales_summary,
    refresh_top_products_summary,
)
from src.metrics import append_run_metrics
from src.mongo_setup import setup_mongodb
from src.queries import (
    QUERY_REGISTRY,
    TARGET_INDEXES,
    create_phase2_indexes,
    list_current_indexes,
)

logger = logging.getLogger("pipeline_api")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifecycle manager for startup initialization and shutdown cleanup."""
    ensure_directories()
    setup_mongodb()
    scheduler = BackgroundJobScheduler.get_instance()
    scheduler.start()
    yield
    scheduler.stop()


app = FastAPI(
    title="Unified Enterprise Data Pipeline API",
    description="Unified execution, querying, analytics, and testing interface for Midterm Data Pipeline (Phase 1 & Phase 2).",
    version="2.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)


# ---------------------------------------------------------------------------
# Pydantic Request & Response Models
# ---------------------------------------------------------------------------
class IngestRequest(BaseModel):
    file_path: str = Field(..., description="Absolute or relative path to CSV file.")
    run_elt: bool = Field(True, description="Whether to execute ELT cleaning phase immediately after Raw Load.")


class RefreshMVRequest(BaseModel):
    incremental: bool = Field(True, description="If True, performs delta update using watermarks. If False, rebuilds view.")
    view_name: Optional[str] = Field(None, description="Optional single view name ('mv_daily_sales_summary' or 'mv_customer_metrics').")


# ---------------------------------------------------------------------------
# 1. Health & Cluster Status
# ---------------------------------------------------------------------------
@app.get("/health", tags=["System Health"], summary="Check API & Database Health")
def get_health() -> Dict[str, Any]:
    """Checks MongoDB connectivity, database presence, and collection record counts."""
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=MONGO_TIMEOUT_MS)
        client.admin.command("ping")
        db = client[MONGO_DATABASE]

        counts = {
            RAW_COLLECTION: db[RAW_COLLECTION].count_documents({}),
            VALIDATED_COLLECTION: db[VALIDATED_COLLECTION].count_documents({}),
            QUARANTINE_COLLECTION: db[QUARANTINE_COLLECTION].count_documents({}),
            MV_DAILY_SALES: db[MV_DAILY_SALES].count_documents({}),
            MV_CUSTOMER_METRICS: db[MV_CUSTOMER_METRICS].count_documents({}),
        }
        client.close()

        return {
            "status": "HEALTHY",
            "database": MONGO_DATABASE,
            "mongo_uri": MONGO_URI,
            "collections_status": counts,
            "scheduler_active": BackgroundJobScheduler.get_instance().is_running(),
        }
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Database connection failed: {exc}",
        )


# ---------------------------------------------------------------------------
# 2. Ingest Data via Phase 1 Pipeline
# ---------------------------------------------------------------------------
@app.post("/ingest", tags=["Data Ingestion"], summary="Ingest CSV using Phase 1 File Router")
def ingest_file(req: IngestRequest) -> Dict[str, Any]:
    """
    Ingests CSV file by strictly calling Phase 1 File Router, selecting engine
    (Python Batch or PySpark), and executing ELT quality pipeline.
    """
    target_path = Path(req.file_path)
    if not target_path.is_absolute():
        target_path = PROJECT_ROOT / target_path

    if not target_path.is_file():
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Target file not found on disk: {target_path}",
        )

    try:
        decision = route_file(target_path)

        if decision["engine"] == "python_batch":
            from src.batch_loader import load_csv_to_raw
            load_stats = load_csv_to_raw(decision["file_path"], decision["run_id"])
        else:
            from src.spark_loader import load_csv_to_raw
            load_stats = load_csv_to_raw(decision["file_path"], decision["run_id"])

        metrics = {
            "run_id": decision["run_id"],
            "file_name": decision["file_name"],
            "file_size_mb": decision["file_size_mb"],
            "engine_used": decision["engine"],
            "source_file": decision["file_path"],
            "threshold_mb": decision["threshold_mb"],
            **load_stats,
            "mongo_database": MONGO_DATABASE,
            "mongo_uri": MONGO_URI,
        }
        append_run_metrics(metrics)

        elt_stats = None
        if req.run_elt:
            from src.elt_pipeline import process_run
            elt_stats = process_run(decision["run_id"], decision["file_path"])

        return {
            "status": "COMPLETED",
            "decision": decision,
            "load_stats": load_stats,
            "elt_stats": elt_stats,
        }
    except Exception as exc:
        logger.error(f"Ingestion failed: {exc}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Ingestion error: {exc}",
        )


# ---------------------------------------------------------------------------
# 3. Phase 2 Index Management
# ---------------------------------------------------------------------------
@app.post("/indexes", tags=["Indexes & Optimization"], summary="Create & Enforce Phase 2 Indexes")
def create_indexes() -> Dict[str, Any]:
    """Ensures that all optimized indexes for Phase 2 queries are created on orders_validated."""
    try:
        created = create_phase2_indexes()
        current_indexes = list_current_indexes()
        return {
            "status": "SUCCESS",
            "created_indexes": created,
            "target_definitions": TARGET_INDEXES,
            "all_current_indexes": current_indexes,
        }
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create indexes: {exc}",
        )


# ---------------------------------------------------------------------------
# 4. Queries & Explain
# ---------------------------------------------------------------------------
@app.get("/queries", tags=["Queries"], summary="List All Registered Queries")
def list_queries() -> Dict[str, Any]:
    """Returns documentation and specifications of all 5 practical queries."""
    return {
        "queries": [
            {
                "name": "orders_by_customer",
                "description": "Find orders for a specific customer sorted chronologically.",
                "supported_parameters": ["customer_id", "limit", "explain"],
                "optimal_index": "idx_customer_id_order_date",
            },
            {
                "name": "orders_by_city_status",
                "description": "Filter orders by city and status sorted by total_amount descending.",
                "supported_parameters": ["city", "status", "limit", "explain"],
                "optimal_index": "idx_city_status_total_amount (Compound Index)",
            },
            {
                "name": "high_value_orders_by_date",
                "description": "Filter high-value orders within a date range.",
                "supported_parameters": ["min_amount", "start_date", "end_date", "limit", "explain"],
                "optimal_index": "idx_order_date_total_amount",
            },
            {
                "name": "orders_by_payment_details",
                "description": "Filter orders by payment status and payment method.",
                "supported_parameters": ["payment_status", "payment_method", "limit", "explain"],
                "optimal_index": "idx_validated_payment",
            },
            {
                "name": "recent_orders_by_delivery",
                "description": "Filter orders by delivery type and city.",
                "supported_parameters": ["delivery_type", "city", "limit", "explain"],
                "optimal_index": "idx_delivery_type_city",
            },
        ]
    }


@app.get("/queries/{name}", tags=["Queries"], summary="Execute a Specific Query")
def execute_query(
    name: str,
    customer_id: Optional[str] = Query(None, description="For orders_by_customer"),
    city: Optional[str] = Query(None, description="For orders_by_city_status or recent_orders_by_delivery"),
    order_status: Optional[str] = Query(None, alias="status", description="For orders_by_city_status"),
    min_amount: Optional[float] = Query(5000.0, description="For high_value_orders_by_date"),
    start_date: Optional[str] = Query("2026-01-01", description="For high_value_orders_by_date"),
    end_date: Optional[str] = Query("2026-12-31", description="For high_value_orders_by_date"),
    payment_status: Optional[str] = Query("تم الدفع", description="For orders_by_payment_details"),
    payment_method: Optional[str] = Query(None, description="For orders_by_payment_details"),
    delivery_type: Optional[str] = Query("سريع", description="For recent_orders_by_delivery"),
    limit: int = Query(50, ge=1, le=500),
    explain: bool = Query(False, description="If True, returns MongoDB explain executionStats"),
) -> Dict[str, Any]:
    """Executes a specific query by name with parameters and optional explain plan."""
    if name not in QUERY_REGISTRY:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Query '{name}' not found. Valid queries: {list(QUERY_REGISTRY.keys())}",
        )

    try:
        if name == "orders_by_customer":
            if not customer_id:
                # Pick an existing customer_id from DB if not provided
                client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=MONGO_TIMEOUT_MS)
                sample = client[MONGO_DATABASE][VALIDATED_COLLECTION].find_one({}, {"customer_id": 1})
                client.close()
                customer_id = sample.get("customer_id") if sample else "CUST-0001"
            return QUERY_REGISTRY[name](customer_id=customer_id, limit=limit, explain=explain)

        elif name == "orders_by_city_status":
            city_val = city or "صنعاء"
            status_val = order_status or "تم الدفع"
            return QUERY_REGISTRY[name](city=city_val, status=status_val, limit=limit, explain=explain)

        elif name == "high_value_orders_by_date":
            return QUERY_REGISTRY[name](
                min_amount=min_amount or 5000.0,
                start_date=start_date or "2026-01-01",
                end_date=end_date or "2026-12-31",
                limit=limit,
                explain=explain,
            )

        elif name == "orders_by_payment_details":
            return QUERY_REGISTRY[name](
                payment_status=payment_status or "تم الدفع",
                payment_method=payment_method,
                limit=limit,
                explain=explain,
            )

        elif name == "recent_orders_by_delivery":
            return QUERY_REGISTRY[name](
                delivery_type=delivery_type or "سريع",
                city=city or "صنعاء",
                limit=limit,
                explain=explain,
            )

        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Unsupported query handler")
    except Exception as exc:
        logger.error(f"Error executing query {name}: {exc}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Query execution error: {exc}",
        )


# ---------------------------------------------------------------------------
# 5. Aggregation Reports
# ---------------------------------------------------------------------------
@app.get("/aggregations", tags=["Aggregations"], summary="List All Aggregation Reports")
def list_aggregations() -> Dict[str, Any]:
    """Lists all available aggregation reports and their descriptions."""
    return {
        "aggregations": [
            {"name": "sales_by_city", "description": "Total sales volume, order counts, and averages grouped by city."},
            {"name": "top_products", "description": "Top selling products ranked by revenue, volume, and total units sold."},
            {"name": "top_customers", "description": "Top spending customers, order counts, and average order value."},
            {"name": "sales_by_period", "description": "Chronological revenue trend grouped by date/period."},
            {"name": "orders_by_status", "description": "Distribution of orders by fulfillment and payment status."},
            {"name": "delivery_performance", "description": "Logistics efficiency and delivery cost breakdown per city."},
        ]
    }


@app.get("/aggregations/{name}", tags=["Aggregations"], summary="Execute an Aggregation Report")
def execute_aggregation(
    name: str,
    limit: int = Query(20, ge=1, le=100),
    min_orders: int = Query(1, ge=1),
) -> Dict[str, Any]:
    """Executes a specific aggregation report by name and returns live aggregated results."""
    if name not in AGGREGATION_REGISTRY:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Aggregation '{name}' not found. Valid options: {list(AGGREGATION_REGISTRY.keys())}",
        )

    try:
        fn = AGGREGATION_REGISTRY[name]
        if name in ("sales_by_city", "top_customers"):
            results = fn(limit=limit, min_orders=min_orders)
        elif name in ("sales_by_period", "top_products"):
            results = fn(limit=limit)
        else:
            results = fn()

        return {
            "aggregation_name": name,
            "records_returned": len(results),
            "results": results,
        }
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Aggregation calculation error: {exc}",
        )


# ---------------------------------------------------------------------------
# 6. Materialized Views & Incremental Refresh
# ---------------------------------------------------------------------------
@app.post("/refresh-mv", tags=["Materialized Views"], summary="Refresh Materialized Views")
def trigger_refresh_mv(req: RefreshMVRequest) -> Dict[str, Any]:
    """
    Triggers an incremental or full refresh of registered Materialized Views.
    Supports incremental partial sync using delta watermarks.
    """
    try:
        view = req.view_name
        if view in (MV_DAILY_SALES, "daily_sales_summary"):
            res = refresh_daily_sales_summary(incremental=req.incremental)
        elif view in (MV_TOP_PRODUCTS, "top_products_summary"):
            res = refresh_top_products_summary(incremental=req.incremental)
        elif view in (MV_CUSTOMER_METRICS, "customer_metrics"):
            res = refresh_customer_metrics(incremental=req.incremental)
        else:
            res = refresh_all_materialized_views(incremental=req.incremental)

        return {
            "status": "SUCCESS",
            "refresh_response": res,
            "views_status": list_materialized_views_status(),
        }
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Materialized view refresh error: {exc}",
        )


# ---------------------------------------------------------------------------
# 7. Scheduled Jobs
# ---------------------------------------------------------------------------
@app.get("/jobs", tags=["Scheduled Jobs"], summary="List Registered Jobs & Recent Execution Logs")
def get_jobs(job_name: Optional[str] = None) -> Dict[str, Any]:
    """Lists registered background jobs, their schedules, and chronological execution history."""
    try:
        registered = list_registered_jobs()
        recent_logs = get_job_logs(job_name=job_name, limit=20)
        return {
            "registered_jobs": registered,
            "recent_logs": recent_logs,
        }
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch job status: {exc}",
        )


@app.post("/jobs/{name}/run", tags=["Scheduled Jobs"], summary="Manually Execute a Scheduled Job")
def trigger_job(name: str) -> Dict[str, Any]:
    """Manually triggers an immediate execution of a registered job, returning the execution log."""
    if name not in JOBS_CATALOG:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job '{name}' not found. Valid jobs: {list(JOBS_CATALOG.keys())}",
        )

    try:
        log_entry = run_job(name)
        return {
            "status": "COMPLETED",
            "execution_log": log_entry,
        }
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Job execution failed: {exc}",
        )
