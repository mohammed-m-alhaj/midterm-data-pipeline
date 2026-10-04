from __future__ import annotations

import logging
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional
from pymongo import ASCENDING, DESCENDING, MongoClient
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

# ---------------------------------------------------------------------------
# Index Specifications for Phase 2
# ---------------------------------------------------------------------------
# Index 1: Compound Index for City + Status + Total Amount (ESR Rule: Equality, Sort, Range)
INDEX_CITY_STATUS_TOTAL = {
    "name": "idx_city_status_total_amount",
    "keys": [("city", ASCENDING), ("status", ASCENDING), ("total_amount", DESCENDING)],
    "description": "Compound index supporting equality filtering on city and status with index-ordered scan on total_amount."
}

# Index 2: Compound Index for Customer ID + Order Date
INDEX_CUSTOMER_DATE = {
    "name": "idx_customer_id_order_date",
    "keys": [("customer_id", ASCENDING), ("order_date", DESCENDING)],
    "description": "Compound index for fast customer order history lookups sorted chronologically."
}

# Index 3: Compound Index for Order Date + Total Amount
INDEX_DATE_TOTAL = {
    "name": "idx_order_date_total_amount",
    "keys": [("order_date", ASCENDING), ("total_amount", ASCENDING)],
    "description": "Compound index for date range filtering combined with amount boundary queries."
}

# Index 4: Compound Index for Delivery Type + City
INDEX_DELIVERY_CITY = {
    "name": "idx_delivery_type_city",
    "keys": [("delivery_type", ASCENDING), ("city", ASCENDING)],
    "description": "Index supporting delivery logistics routing and regional filtering."
}

TARGET_INDEXES = [
    INDEX_CITY_STATUS_TOTAL,
    INDEX_CUSTOMER_DATE,
    INDEX_DATE_TOTAL,
    INDEX_DELIVERY_CITY,
]


def get_collection(collection_name: str = VALIDATED_COLLECTION) -> Collection:
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=MONGO_TIMEOUT_MS)
    db = client[MONGO_DATABASE]
    return db[collection_name]


# ---------------------------------------------------------------------------
# Index Management Functions
# ---------------------------------------------------------------------------
def create_phase2_indexes(collection_name: str = VALIDATED_COLLECTION) -> List[str]:
    """Creates the defined indexes on orders_validated."""
    coll = get_collection(collection_name)
    created = []
    for idx in TARGET_INDEXES:
        name = coll.create_index(idx["keys"], name=idx["name"])
        created.append(name)
    return created


def drop_phase2_indexes(collection_name: str = VALIDATED_COLLECTION) -> List[str]:
    """Drops the phase 2 indexes to allow clean before/after benchmarking."""
    coll = get_collection(collection_name)
    existing_indexes = coll.index_information()
    dropped = []
    for idx in TARGET_INDEXES:
        idx_name = idx["name"]
        if idx_name in existing_indexes:
            coll.drop_index(idx_name)
            dropped.append(idx_name)
    return dropped


def list_current_indexes(collection_name: str = VALIDATED_COLLECTION) -> Dict[str, Any]:
    """Returns detailed information about all indexes currently present on the collection."""
    coll = get_collection(collection_name)
    return coll.index_information()


# ---------------------------------------------------------------------------
# 5 Practical Queries on orders_validated
# ---------------------------------------------------------------------------
def query_orders_by_customer(
    customer_id: str,
    limit: int = 50,
    explain: bool = False,
    collection_name: str = VALIDATED_COLLECTION
) -> Dict[str, Any]:
    """
    Query 1: Retrieve all orders for a specific customer sorted by order_date descending.
    Target Index: idx_customer_id_order_date
    """
    coll = get_collection(collection_name)
    filt = {"customer_id": customer_id}
    sort = [("customer_id", ASCENDING), ("order_date", DESCENDING)]
    projection = {"_id": 0, "order_id": 1, "customer_id": 1, "order_date": 1, "status": 1, "total_amount": 1, "city": 1}

    cursor = coll.find(filt, projection).sort(sort).limit(limit)
    if explain:
        return cursor.explain()
    return {"query_name": "orders_by_customer", "count": len(list(cursor.clone())), "results": list(cursor)}


def query_orders_by_city_status(
    city: str,
    status: str,
    limit: int = 50,
    explain: bool = False,
    collection_name: str = VALIDATED_COLLECTION
) -> Dict[str, Any]:
    """
    Query 2: Filter orders by city and status, sorted by total_amount descending.
    Target Index: idx_city_status_total_amount (Compound Index)
    """
    coll = get_collection(collection_name)
    filt = {"city": city, "status": status}
    sort = [("city", ASCENDING), ("status", ASCENDING), ("total_amount", DESCENDING)]
    projection = {"_id": 0, "order_id": 1, "customer_id": 1, "city": 1, "status": 1, "total_amount": 1, "payment_method": 1}

    cursor = coll.find(filt, projection).sort(sort).limit(limit)
    if explain:
        return cursor.explain()
    return {"query_name": "orders_by_city_status", "count": len(list(cursor.clone())), "results": list(cursor)}


def query_high_value_orders_by_date(
    min_amount: float,
    start_date: str,
    end_date: str,
    limit: int = 50,
    explain: bool = False,
    collection_name: str = VALIDATED_COLLECTION
) -> Dict[str, Any]:
    """
    Query 3: Filter high-value orders within a specific date range.
    Target Index: idx_order_date_total_amount
    """
    coll = get_collection(collection_name)
    filt = {
        "order_date": {"$gte": start_date, "$lte": end_date},
        "total_amount": {"$gte": min_amount}
    }
    sort = [("order_date", ASCENDING), ("total_amount", ASCENDING)]
    projection = {"_id": 0, "order_id": 1, "order_date": 1, "total_amount": 1, "customer_name": 1, "city": 1}

    cursor = coll.find(filt, projection).sort(sort).limit(limit)
    if explain:
        return cursor.explain()
    return {"query_name": "high_value_orders_by_date", "count": len(list(cursor.clone())), "results": list(cursor)}


def query_orders_by_payment_details(
    payment_status: str,
    payment_method: Optional[str] = None,
    limit: int = 50,
    explain: bool = False,
    collection_name: str = VALIDATED_COLLECTION
) -> Dict[str, Any]:
    """
    Query 4: Filter orders by payment_status and optionally payment_method.
    """
    coll = get_collection(collection_name)
    filt: Dict[str, Any] = {"payment_status": payment_status}
    if payment_method:
        filt["payment_method"] = payment_method

    sort = [("order_id", ASCENDING)]
    projection = {"_id": 0, "order_id": 1, "customer_id": 1, "payment_status": 1, "payment_method": 1, "payment_amount": 1, "total_amount": 1}

    cursor = coll.find(filt, projection).sort(sort).limit(limit)
    if explain:
        return cursor.explain()
    return {"query_name": "orders_by_payment_details", "count": len(list(cursor.clone())), "results": list(cursor)}


def query_recent_orders_by_delivery(
    delivery_type: str,
    city: str,
    limit: int = 50,
    explain: bool = False,
    collection_name: str = VALIDATED_COLLECTION
) -> Dict[str, Any]:
    """
    Query 5: Filter orders by delivery_type and city.
    Target Index: idx_delivery_type_city
    """
    coll = get_collection(collection_name)
    filt = {"delivery_type": delivery_type, "city": city}
    sort = [("delivery_type", ASCENDING), ("city", ASCENDING)]
    projection = {"_id": 0, "order_id": 1, "city": 1, "delivery_type": 1, "delivery_cost": 1, "total_amount": 1, "order_date": 1}

    cursor = coll.find(filt, projection).sort(sort).limit(limit)
    if explain:
        return cursor.explain()
    return {"query_name": "recent_orders_by_delivery", "count": len(list(cursor.clone())), "results": list(cursor)}


QUERY_REGISTRY = {
    "orders_by_customer": query_orders_by_customer,
    "orders_by_city_status": query_orders_by_city_status,
    "high_value_orders_by_date": query_high_value_orders_by_date,
    "orders_by_payment_details": query_orders_by_payment_details,
    "recent_orders_by_delivery": query_recent_orders_by_delivery,
}


# ---------------------------------------------------------------------------
# Explain Execution Statistics Extractor
# ---------------------------------------------------------------------------
def extract_execution_stats(explain_plan: Dict[str, Any]) -> Dict[str, Any]:
    """Parses MongoDB explain output to extract standard performance metrics."""
    execution_stats = explain_plan.get("executionStats", {})
    query_planner = explain_plan.get("queryPlanner", {})
    winning_plan = query_planner.get("winningPlan", {})

    # Detect the scanning stage (COLLSCAN, IXSCAN, FETCH, SORT)
    stage = winning_plan.get("stage", "UNKNOWN")
    child_stage = winning_plan.get("inputStage", {}).get("stage") if "inputStage" in winning_plan else None
    primary_stage = child_stage if stage in ("FETCH", "PROJECTION_SIMPLE", "SORT") and child_stage else stage

    index_name = None
    if "inputStage" in winning_plan and "indexName" in winning_plan["inputStage"]:
        index_name = winning_plan["inputStage"]["indexName"]
    elif "indexName" in winning_plan:
        index_name = winning_plan["indexName"]

    return {
        "executionSuccess": execution_stats.get("executionSuccess", True),
        "nReturned": execution_stats.get("nReturned", 0),
        "executionTimeMillis": execution_stats.get("executionTimeMillis", 0),
        "totalKeysExamined": execution_stats.get("totalKeysExamined", 0),
        "totalDocsExamined": execution_stats.get("totalDocsExamined", 0),
        "stage": primary_stage,
        "outerStage": stage,
        "indexName": index_name,
    }


def run_explain_comparison(
    query_name: str,
    query_kwargs: Dict[str, Any],
    collection_name: str = VALIDATED_COLLECTION
) -> Dict[str, Any]:
    """Runs explain for a query before indexes, creates indexes, runs explain after, and compares."""
    query_fn = QUERY_REGISTRY[query_name]

    # 1. Drop Phase 2 indexes
    drop_phase2_indexes(collection_name)
    explain_before = query_fn(explain=True, collection_name=collection_name, **query_kwargs)
    stats_before = extract_execution_stats(explain_before)

    # 2. Create Phase 2 indexes
    create_phase2_indexes(collection_name)
    explain_after = query_fn(explain=True, collection_name=collection_name, **query_kwargs)
    stats_after = extract_execution_stats(explain_after)

    docs_ratio = (
        stats_before["totalDocsExamined"] / max(stats_after["totalDocsExamined"], 1)
        if stats_after["totalDocsExamined"] > 0
        else stats_before["totalDocsExamined"]
    )

    return {
        "query_name": query_name,
        "parameters": query_kwargs,
        "before_index": stats_before,
        "after_index": stats_after,
        "performance_gain": {
            "time_saved_ms": stats_before["executionTimeMillis"] - stats_after["executionTimeMillis"],
            "docs_examined_reduction": stats_before["totalDocsExamined"] - stats_after["totalDocsExamined"],
            "docs_ratio": round(docs_ratio, 2),
            "stage_transition": f"{stats_before['stage']} -> {stats_after['stage']}",
        },
        "raw_explain_before": explain_before,
        "raw_explain_after": explain_after,
    }
