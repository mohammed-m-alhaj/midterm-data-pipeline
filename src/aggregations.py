from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional
from pymongo import MongoClient
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


def get_collection(collection_name: str = VALIDATED_COLLECTION) -> Collection:
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=MONGO_TIMEOUT_MS)
    db = client[MONGO_DATABASE]
    return db[collection_name]


# ---------------------------------------------------------------------------
# Aggregation Report 1: Sales By City
# ---------------------------------------------------------------------------
def report_sales_by_city(
    limit: int = 20,
    min_orders: int = 1,
    collection_name: str = VALIDATED_COLLECTION,
) -> List[Dict[str, Any]]:
    """
    Report 1: Aggregates sales volume, order counts, and order statistics grouped by city.
    """
    coll = get_collection(collection_name)
    pipeline = [
        {"$match": {"city": {"$ne": None}, "total_amount": {"$ne": None}}},
        {
            "$group": {
                "_id": "$city",
                "total_sales": {"$sum": "$total_amount"},
                "order_count": {"$sum": 1},
                "avg_order_value": {"$avg": "$total_amount"},
                "max_order_value": {"$max": "$total_amount"},
                "min_order_value": {"$min": "$total_amount"},
                "total_delivery_fees": {"$sum": {"$ifNull": ["$delivery_cost", 0]}},
            }
        },
        {"$match": {"order_count": {"$gte": min_orders}}},
        {"$sort": {"total_sales": -1}},
        {"$limit": limit},
        {
            "$project": {
                "_id": 0,
                "city": "$_id",
                "total_sales": {"$round": ["$total_sales", 2]},
                "order_count": 1,
                "avg_order_value": {"$round": ["$avg_order_value", 2]},
                "max_order_value": {"$round": ["$max_order_value", 2]},
                "min_order_value": {"$round": ["$min_order_value", 2]},
                "total_delivery_fees": {"$round": ["$total_delivery_fees", 2]},
            }
        },
    ]
    return list(coll.aggregate(pipeline))


# ---------------------------------------------------------------------------
# Aggregation Report 2: Top Customers by Spend
# ---------------------------------------------------------------------------
def report_top_customers(
    limit: int = 20,
    min_orders: int = 1,
    collection_name: str = VALIDATED_COLLECTION,
) -> List[Dict[str, Any]]:
    """
    Report 2: Identifies highest spending customers, order counts, and order patterns.
    """
    coll = get_collection(collection_name)
    pipeline = [
        {"$match": {"customer_id": {"$ne": None}, "total_amount": {"$ne": None}}},
        {
            "$group": {
                "_id": "$customer_id",
                "customer_name": {"$first": "$customer_name"},
                "customer_phone": {"$first": "$customer_phone"},
                "city": {"$first": "$city"},
                "total_spent": {"$sum": "$total_amount"},
                "total_orders": {"$sum": 1},
                "avg_order_value": {"$avg": "$total_amount"},
                "latest_order_date": {"$max": "$order_date"},
            }
        },
        {"$match": {"total_orders": {"$gte": min_orders}}},
        {"$sort": {"total_spent": -1}},
        {"$limit": limit},
        {
            "$project": {
                "_id": 0,
                "customer_id": "$_id",
                "customer_name": 1,
                "customer_phone": 1,
                "city": 1,
                "total_spent": {"$round": ["$total_spent", 2]},
                "total_orders": 1,
                "avg_order_value": {"$round": ["$avg_order_value", 2]},
                "latest_order_date": 1,
            }
        },
    ]
    return list(coll.aggregate(pipeline))


# ---------------------------------------------------------------------------
# Aggregation Report 3: Sales by Period (Daily / Monthly Trend)
# ---------------------------------------------------------------------------
def report_sales_by_period(
    period_format: str = "%Y-%m-%d",
    limit: int = 30,
    collection_name: str = VALIDATED_COLLECTION,
) -> List[Dict[str, Any]]:
    """
    Report 3: Aggregates chronological revenue trend by day/period.
    """
    coll = get_collection(collection_name)
    pipeline = [
        {"$match": {"order_date": {"$ne": None}, "total_amount": {"$ne": None}}},
        {
            "$project": {
                "period": {"$substrCP": ["$order_date", 0, 10]},
                "total_amount": 1,
                "delivery_cost": {"$ifNull": ["$delivery_cost", 0]},
                "order_id": 1,
            }
        },
        {
            "$group": {
                "_id": "$period",
                "total_revenue": {"$sum": "$total_amount"},
                "order_count": {"$sum": 1},
                "avg_daily_order": {"$avg": "$total_amount"},
                "total_delivery_revenue": {"$sum": "$delivery_cost"},
            }
        },
        {"$sort": {"_id": -1}},
        {"$limit": limit},
        {
            "$project": {
                "_id": 0,
                "period": "$_id",
                "total_revenue": {"$round": ["$total_revenue", 2]},
                "order_count": 1,
                "avg_daily_order": {"$round": ["$avg_daily_order", 2]},
                "total_delivery_revenue": {"$round": ["$total_delivery_revenue", 2]},
            }
        },
    ]
    return list(coll.aggregate(pipeline))


# ---------------------------------------------------------------------------
# Aggregation Report 4: Orders by Status & Payment Distribution
# ---------------------------------------------------------------------------
def report_orders_by_status(
    collection_name: str = VALIDATED_COLLECTION,
) -> List[Dict[str, Any]]:
    """
    Report 4: Groups orders by fulfillment and payment status, summarizing volumes.
    """
    coll = get_collection(collection_name)
    pipeline = [
        {"$match": {"status": {"$ne": None}}},
        {
            "$group": {
                "_id": {
                    "order_status": "$status",
                    "payment_status": {"$ifNull": ["$payment_status", "UNKNOWN"]},
                },
                "order_count": {"$sum": 1},
                "total_volume": {"$sum": {"$ifNull": ["$total_amount", 0]}},
                "avg_amount": {"$avg": {"$ifNull": ["$total_amount", 0]}},
            }
        },
        {"$sort": {"order_count": -1}},
        {
            "$project": {
                "_id": 0,
                "order_status": "$_id.order_status",
                "payment_status": "$_id.payment_status",
                "order_count": 1,
                "total_volume": {"$round": ["$total_volume", 2]},
                "avg_amount": {"$round": ["$avg_amount", 2]},
            }
        },
    ]
    return list(coll.aggregate(pipeline))


# ---------------------------------------------------------------------------
# Aggregation Report 5: Delivery Performance by City
# ---------------------------------------------------------------------------
def report_delivery_performance(
    collection_name: str = VALIDATED_COLLECTION,
) -> List[Dict[str, Any]]:
    """
    Report 5: Analyzes logistics efficiency, delivery speed types, and delivery cost breakdown per city.
    """
    coll = get_collection(collection_name)
    pipeline = [
        {"$match": {"city": {"$ne": None}, "delivery_type": {"$ne": None}}},
        {
            "$group": {
                "_id": {
                    "city": "$city",
                    "delivery_type": "$delivery_type",
                },
                "total_shipments": {"$sum": 1},
                "total_delivery_cost": {"$sum": {"$ifNull": ["$delivery_cost", 0]}},
                "avg_delivery_cost": {"$avg": {"$ifNull": ["$delivery_cost", 0]}},
                "total_merchandise_value": {"$sum": {"$ifNull": ["$total_amount", 0]}},
            }
        },
        {"$sort": {"total_shipments": -1}},
        {
            "$project": {
                "_id": 0,
                "city": "$_id.city",
                "delivery_type": "$_id.delivery_type",
                "total_shipments": 1,
                "total_delivery_cost": {"$round": ["$total_delivery_cost", 2]},
                "avg_delivery_cost": {"$round": ["$avg_delivery_cost", 2]},
                "total_merchandise_value": {"$round": ["$total_merchandise_value", 2]},
            }
        },
    ]
    return list(coll.aggregate(pipeline))


AGGREGATION_REGISTRY = {
    "sales_by_city": report_sales_by_city,
    "top_customers": report_top_customers,
    "sales_by_period": report_sales_by_period,
    "orders_by_status": report_orders_by_status,
    "delivery_performance": report_delivery_performance,
}
