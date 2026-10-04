from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from pymongo import ASCENDING, MongoClient, ReplaceOne
from pymongo.collection import Collection

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
    VALIDATED_COLLECTION,
)

logger = logging.getLogger(__name__)

MV_DAILY_SALES = "mv_daily_sales_summary"
MV_CUSTOMER_METRICS = "mv_customer_metrics"
MV_METADATA_COLLECTION = "mv_refresh_metadata"


def get_db():
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=MONGO_TIMEOUT_MS)
    return client[MONGO_DATABASE]


def get_metadata(view_name: str) -> Optional[Dict[str, Any]]:
    db = get_db()
    return db[MV_METADATA_COLLECTION].find_one({"view_name": view_name})


def update_metadata(
    view_name: str,
    refresh_mode: str,
    records_processed: int,
    affected_keys_count: int,
    duration_ms: float,
    status: str = "SUCCESS",
    error_message: Optional[str] = None,
) -> None:
    db = get_db()
    db[MV_METADATA_COLLECTION].update_one(
        {"view_name": view_name},
        {
            "$set": {
                "view_name": view_name,
                "last_refreshed_at": datetime.now(timezone.utc).isoformat(),
                "refresh_mode": refresh_mode,
                "records_processed": records_processed,
                "affected_keys_count": affected_keys_count,
                "duration_ms": round(duration_ms, 2),
                "status": status,
                "error_message": error_message,
            }
        },
        upsert=True,
    )


# ---------------------------------------------------------------------------
# Materialized View 1: Daily Sales Summary (mv_daily_sales_summary)
# ---------------------------------------------------------------------------
def refresh_daily_sales_summary(
    incremental: bool = True,
    target_dates: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Refreshes mv_daily_sales_summary.
    If incremental=True:
      - Determines newly inserted/updated dates or uses target_dates.
      - Recalculates aggregations ONLY for those affected dates.
      - Upserts into mv_daily_sales_summary without re-processing older unchanged dates.
    If incremental=False:
      - Rebuilds the entire view from orders_validated.
    """
    db = get_db()
    source_coll = db[VALIDATED_COLLECTION]
    target_coll = db[MV_DAILY_SALES]
    t0 = time.perf_counter()

    # Ensure target index
    target_coll.create_index([("date", ASCENDING)], unique=True)

    metadata = get_metadata(MV_DAILY_SALES)
    is_initial_build = metadata is None or target_coll.count_documents({}) == 0

    mode = "full" if (not incremental or is_initial_build) else "incremental"
    match_filter: Dict[str, Any] = {"order_date": {"$ne": None}, "total_amount": {"$ne": None}}

    if mode == "incremental":
        if target_dates:
            match_filter["order_date"] = {"$regex": f"^({'|'.join(target_dates)})"}
        elif metadata and "last_refreshed_at" in metadata:
            last_sync = metadata["last_refreshed_at"]
            # Find distinct dates for records with recent ingested_at or order_date
            recent_dates = source_coll.distinct(
                "order_date",
                {"ingested_at": {"$gte": datetime.fromisoformat(last_sync)}} if "ingested_at" in source_coll.find_one() or {} else {}
            )
            # Take substring YYYY-MM-DD
            day_strings = list({str(d)[:10] for d in recent_dates if d})
            if day_strings:
                match_filter["order_date"] = {"$regex": f"^({'|'.join(day_strings)})"}
            else:
                # No recent updates found, nothing to do
                duration = (time.perf_counter() - t0) * 1000
                update_metadata(MV_DAILY_SALES, "incremental", 0, 0, duration)
                return {
                    "view_name": MV_DAILY_SALES,
                    "mode": "incremental",
                    "status": "UP_TO_DATE",
                    "affected_keys": 0,
                    "duration_ms": round(duration, 2),
                }

    pipeline = [
        {"$match": match_filter},
        {
            "$project": {
                "date": {"$substrCP": ["$order_date", 0, 10]},
                "total_amount": 1,
                "delivery_cost": {"$ifNull": ["$delivery_cost", 0]},
                "status": 1,
            }
        },
        {
            "$group": {
                "_id": "$date",
                "date": {"$first": "$date"},
                "total_sales": {"$sum": "$total_amount"},
                "order_count": {"$sum": 1},
                "avg_order_value": {"$avg": "$total_amount"},
                "total_delivery_fees": {"$sum": "$delivery_cost"},
                "completed_orders": {"$sum": {"$cond": [{"$in": ["$status", ["تم الدفع", "مؤكد"]]}, 1, 0]}},
                "pending_orders": {"$sum": {"$cond": [{"$eq": ["$status", "بانتظار الدفع"]}, 1, 0]}},
            }
        },
        {
            "$project": {
                "_id": 1,
                "date": 1,
                "total_sales": {"$round": ["$total_sales", 2]},
                "order_count": 1,
                "avg_order_value": {"$round": ["$avg_order_value", 2]},
                "total_delivery_fees": {"$round": ["$total_delivery_fees", 2]},
                "completed_orders": 1,
                "pending_orders": 1,
                "updated_at": {"$literal": datetime.now(timezone.utc).isoformat()},
            }
        },
    ]

    results = list(source_coll.aggregate(pipeline))

    if mode == "full":
        target_coll.delete_many({})
        if results:
            target_coll.insert_many(results)
    else:
        # Incremental replace / upsert on _id (date)
        if results:
            operations = [
                ReplaceOne({"_id": doc["_id"]}, doc, upsert=True)
                for doc in results
            ]
            target_coll.bulk_write(operations)

    duration = (time.perf_counter() - t0) * 1000
    affected_keys = len(results)
    total_docs = target_coll.count_documents({})

    update_metadata(MV_DAILY_SALES, mode, total_docs, affected_keys, duration)

    return {
        "view_name": MV_DAILY_SALES,
        "mode": mode,
        "status": "SUCCESS",
        "affected_keys": affected_keys,
        "total_documents_in_view": total_docs,
        "duration_ms": round(duration, 2),
    }


# ---------------------------------------------------------------------------
# Materialized View 2: Customer Performance Metrics (mv_customer_metrics)
# ---------------------------------------------------------------------------
def refresh_customer_metrics(
    incremental: bool = True,
    target_customer_ids: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Refreshes mv_customer_metrics.
    If incremental=True:
      - Recalculates metrics ONLY for specified or newly modified customers.
      - Upserts into mv_customer_metrics without scanning the rest of the customer base.
    If incremental=False:
      - Full rebuild of the customer summary table.
    """
    db = get_db()
    source_coll = db[VALIDATED_COLLECTION]
    target_coll = db[MV_CUSTOMER_METRICS]
    t0 = time.perf_counter()

    # Ensure index on customer_id
    target_coll.create_index([("customer_id", ASCENDING)], unique=True)

    metadata = get_metadata(MV_CUSTOMER_METRICS)
    is_initial_build = metadata is None or target_coll.count_documents({}) == 0

    mode = "full" if (not incremental or is_initial_build) else "incremental"
    match_filter: Dict[str, Any] = {"customer_id": {"$ne": None}, "total_amount": {"$ne": None}}

    if mode == "incremental":
        if target_customer_ids:
            match_filter["customer_id"] = {"$in": target_customer_ids}
        elif metadata and "last_refreshed_at" in metadata:
            last_sync = metadata["last_refreshed_at"]
            recent_custs = source_coll.distinct(
                "customer_id",
                {"ingested_at": {"$gte": datetime.fromisoformat(last_sync)}} if "ingested_at" in source_coll.find_one() or {} else {}
            )
            if recent_custs:
                match_filter["customer_id"] = {"$in": recent_custs}
            else:
                duration = (time.perf_counter() - t0) * 1000
                update_metadata(MV_CUSTOMER_METRICS, "incremental", 0, 0, duration)
                return {
                    "view_name": MV_CUSTOMER_METRICS,
                    "mode": "incremental",
                    "status": "UP_TO_DATE",
                    "affected_keys": 0,
                    "duration_ms": round(duration, 2),
                }

    pipeline = [
        {"$match": match_filter},
        {
            "$group": {
                "_id": "$customer_id",
                "customer_id": {"$first": "$customer_id"},
                "customer_name": {"$first": "$customer_name"},
                "customer_phone": {"$first": "$customer_phone"},
                "city": {"$first": "$city"},
                "total_spend": {"$sum": "$total_amount"},
                "order_count": {"$sum": 1},
                "avg_order_value": {"$avg": "$total_amount"},
                "latest_order_date": {"$max": "$order_date"},
            }
        },
        {
            "$project": {
                "_id": 1,
                "customer_id": 1,
                "customer_name": 1,
                "customer_phone": 1,
                "city": 1,
                "total_spend": {"$round": ["$total_spend", 2]},
                "order_count": 1,
                "avg_order_value": {"$round": ["$avg_order_value", 2]},
                "latest_order_date": 1,
                "updated_at": {"$literal": datetime.now(timezone.utc).isoformat()},
            }
        },
    ]

    results = list(source_coll.aggregate(pipeline))

    if mode == "full":
        target_coll.delete_many({})
        if results:
            target_coll.insert_many(results)
    else:
        if results:
            operations = [
                ReplaceOne({"_id": doc["_id"]}, doc, upsert=True)
                for doc in results
            ]
            target_coll.bulk_write(operations)

    duration = (time.perf_counter() - t0) * 1000
    affected_keys = len(results)
    total_docs = target_coll.count_documents({})

    update_metadata(MV_CUSTOMER_METRICS, mode, total_docs, affected_keys, duration)

    return {
        "view_name": MV_CUSTOMER_METRICS,
        "mode": mode,
        "status": "SUCCESS",
        "affected_keys": affected_keys,
        "total_documents_in_view": total_docs,
        "duration_ms": round(duration, 2),
    }


def refresh_all_materialized_views(incremental: bool = True) -> Dict[str, Any]:
    """Refreshes all registered materialized views."""
    r1 = refresh_daily_sales_summary(incremental=incremental)
    r2 = refresh_customer_metrics(incremental=incremental)
    return {
        "daily_sales_summary": r1,
        "customer_metrics": r2,
        "refreshed_at": datetime.now(timezone.utc).isoformat(),
        "incremental": incremental,
    }


def list_materialized_views_status() -> List[Dict[str, Any]]:
    """Returns the current state and metadata of all registered views."""
    db = get_db()
    views = [MV_DAILY_SALES, MV_CUSTOMER_METRICS]
    status_list = []
    for v in views:
        meta = get_metadata(v) or {}
        count = db[v].count_documents({})
        sample = list(db[v].find({}, {"_id": 0}).limit(2))
        status_list.append({
            "view_name": v,
            "document_count": count,
            "last_refreshed_at": meta.get("last_refreshed_at"),
            "last_mode": meta.get("refresh_mode"),
            "duration_ms": meta.get("duration_ms"),
            "status": meta.get("status", "NOT_INITIALIZED"),
            "sample_records": sample,
        })
    return status_list
