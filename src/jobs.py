from __future__ import annotations

import logging
import threading
import time
import traceback
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional
from uuid import uuid4
from pymongo import DESCENDING, MongoClient

import sys
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent
_src_dir = Path(__file__).resolve().parent
for _p in (_project_root, _src_dir):
    _ps = str(_p)
    if _ps not in sys.path:
        sys.path.insert(0, _ps)

from bootstrap import ensure_project_root

ensure_project_root()

from config.settings import (
    MONGO_DATABASE,
    MONGO_TIMEOUT_MS,
    MONGO_URI,
)
from src.aggregations import AGGREGATION_REGISTRY
from src.materialized_views import refresh_all_materialized_views

logger = logging.getLogger(__name__)

JOB_LOGS_COLLECTION = "job_execution_logs"
ANALYTICS_SNAPSHOTS_COLLECTION = "analytics_snapshots"


def get_db():
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=MONGO_TIMEOUT_MS)
    return client[MONGO_DATABASE]


# ---------------------------------------------------------------------------
# Core Job Execution Functions
# ---------------------------------------------------------------------------
def execute_materialized_views_refresh(incremental: bool = True) -> Dict[str, Any]:
    """Job 1 Function: Performs scheduled/manual refresh of all Materialized Views."""
    return refresh_all_materialized_views(incremental=incremental)


def execute_analytics_snapshot() -> Dict[str, Any]:
    """
    Job 2 Function: Runs all 5 aggregation reports and compiles an official
    executive periodic analytics snapshot into the analytics_snapshots collection.
    """
    db = get_db()
    snapshot_time = datetime.now(timezone.utc).isoformat()

    sales_by_city = AGGREGATION_REGISTRY["sales_by_city"](limit=10)
    top_customers = AGGREGATION_REGISTRY["top_customers"](limit=10)
    sales_by_period = AGGREGATION_REGISTRY["sales_by_period"](limit=10)
    orders_by_status = AGGREGATION_REGISTRY["orders_by_status"]()
    delivery_perf = AGGREGATION_REGISTRY["delivery_performance"]()

    snapshot_doc = {
        "snapshot_id": uuid4().hex,
        "created_at": snapshot_time,
        "reports_summary": {
            "cities_tracked": len(sales_by_city),
            "top_customers_count": len(top_customers),
            "days_recorded": len(sales_by_period),
            "status_combinations": len(orders_by_status),
            "delivery_routes": len(delivery_perf),
        },
        "sales_by_city": sales_by_city,
        "top_customers": top_customers,
        "sales_by_period": sales_by_period,
        "orders_by_status": orders_by_status,
        "delivery_performance": delivery_perf,
    }

    db[ANALYTICS_SNAPSHOTS_COLLECTION].insert_one(snapshot_doc)
    # Remove MongoDB internal _id before returning dict
    snapshot_doc.pop("_id", None)
    return snapshot_doc


# ---------------------------------------------------------------------------
# Registered Job Definitions
# ---------------------------------------------------------------------------
JOBS_CATALOG: Dict[str, Dict[str, Any]] = {
    "refresh_materialized_views": {
        "job_name": "refresh_materialized_views",
        "description": "Periodically executes incremental or full refresh of all Materialized Views.",
        "schedule": "Every 30 minutes (Interval: 1800s)",
        "interval_seconds": 1800,
        "target_function": execute_materialized_views_refresh,
    },
    "generate_analytics_snapshot": {
        "job_name": "generate_analytics_snapshot",
        "description": "Executes all 5 Aggregation Pipelines and archives an executive analytics snapshot.",
        "schedule": "Every 60 minutes (Interval: 3600s)",
        "interval_seconds": 3600,
        "target_function": execute_analytics_snapshot,
    },
}


# ---------------------------------------------------------------------------
# Job Execution Harness & MongoDB Logger
# ---------------------------------------------------------------------------
def run_job(job_name: str, **kwargs) -> Dict[str, Any]:
    """
    Executes a registered job with strict start/end time tracking,
    exception boundary handling, and persistent MongoDB execution logging.
    """
    if job_name not in JOBS_CATALOG:
        raise ValueError(f"Job '{job_name}' not found. Available jobs: {list(JOBS_CATALOG.keys())}")

    job_meta = JOBS_CATALOG[job_name]
    target_fn: Callable = job_meta["target_function"]

    job_id = uuid4().hex
    start_time = datetime.now(timezone.utc)
    t0 = time.perf_counter()

    success = False
    status = "RUNNING"
    error_details = None
    metrics_result = None

    try:
        if kwargs:
            metrics_result = target_fn(**kwargs)
        else:
            metrics_result = target_fn()
        success = True
        status = "SUCCESS"
    except Exception as exc:
        success = False
        status = "FAILED"
        error_details = {
            "error_type": type(exc).__name__,
            "message": str(exc),
            "traceback": traceback.format_exc(),
        }
        logger.error(f"Job '{job_name}' failed: {exc}", exc_info=True)

    duration_ms = (time.perf_counter() - t0) * 1000
    end_time = datetime.now(timezone.utc)

    log_entry = {
        "job_id": job_id,
        "job_name": job_name,
        "schedule": job_meta["schedule"],
        "start_time": start_time.isoformat(),
        "end_time": end_time.isoformat(),
        "duration_ms": round(duration_ms, 2),
        "success": success,
        "status": status,
        "error_details": error_details,
        "metrics": metrics_result if success else None,
    }

    try:
        db = get_db()
        db[JOB_LOGS_COLLECTION].insert_one(dict(log_entry))
        log_entry.pop("_id", None)
    except Exception as log_err:
        logger.error(f"Failed to persist job log: {log_err}")

    return log_entry


def get_job_logs(job_name: Optional[str] = None, limit: int = 20) -> List[Dict[str, Any]]:
    """Retrieves chronological job execution logs from MongoDB."""
    db = get_db()
    filt = {"job_name": job_name} if job_name else {}
    cursor = db[JOB_LOGS_COLLECTION].find(filt, {"_id": 0}).sort("start_time", DESCENDING).limit(limit)
    return list(cursor)


def list_registered_jobs() -> List[Dict[str, Any]]:
    """Lists registered jobs with their schedule and latest execution log status."""
    db = get_db()
    jobs_list = []
    for name, data in JOBS_CATALOG.items():
        last_log = db[JOB_LOGS_COLLECTION].find_one({"job_name": name}, {"_id": 0}, sort=[("start_time", DESCENDING)])
        jobs_list.append({
            "job_name": name,
            "description": data["description"],
            "schedule": data["schedule"],
            "interval_seconds": data["interval_seconds"],
            "last_run": last_log,
        })
    return jobs_list


# ---------------------------------------------------------------------------
# Background Daemon Scheduler
# ---------------------------------------------------------------------------
class BackgroundJobScheduler:
    """Lightweight native background thread scheduler for periodic jobs."""
    _instance: Optional[BackgroundJobScheduler] = None

    def __init__(self):
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()

    @classmethod
    def get_instance(cls) -> BackgroundJobScheduler:
        if cls._instance is None:
            cls._instance = BackgroundJobScheduler()
        return cls._instance

    def start(self) -> None:
        with self._lock:
            if self._running:
                return
            self._running = True
            self._thread = threading.Thread(target=self._run_loop, daemon=True, name="JobSchedulerThread")
            self._thread.start()
            logger.info("Background job scheduler started.")

    def stop(self) -> None:
        with self._lock:
            self._running = False
            logger.info("Background job scheduler stopped.")

    def is_running(self) -> bool:
        return self._running

    def _run_loop(self) -> None:
        last_run_times: Dict[str, float] = {}
        while self._running:
            now = time.time()
            for job_name, config in JOBS_CATALOG.items():
                interval = config["interval_seconds"]
                last_run = last_run_times.get(job_name, 0)
                if now - last_run >= interval:
                    try:
                        run_job(job_name)
                    except Exception as e:
                        logger.error(f"Scheduler tick error on {job_name}: {e}")
                    last_run_times[job_name] = now
            time.sleep(5)
