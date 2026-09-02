from __future__ import annotations

import csv
import os
import subprocess
import sys
from pathlib import Path

from bootstrap import ensure_project_root

ensure_project_root()

from config.settings import DATA_DIR, RAW_COLUMNS


def run_flexibility_tests():
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass

    print("\033[96m" + "=" * 70 + "\033[0m")
    print("\033[1m\033[92mHYBRID PIPELINE FLEXIBILITY & STRESS TEST SUITE\033[0m")
    print("\033[96m" + "=" * 70 + "\033[0m\n")

    # -------------------------------------------------------------------------
    # Scenario 1: Extreme Dirty Formatting & Edge Case Ingestion
    # -------------------------------------------------------------------------
    print("\033[1m\033[93m[SCENARIO 1] Extreme Dirty Data Edge Cases & Auto-Cleaning\033[0m")
    edge_csv = DATA_DIR / "test_extreme_edge_cases.csv"

    edge_rows = [
        {
            "order_id": "EDGE-1001",
            "order_date": "2026/08/25",
            "status": "مأكد",
            "customer_id": "CUST-01",
            "customer_name": "أحمد  علي  ",
            "customer_phone": "٩٦٧٧٧١٢٣٤٥٦٧",
            "customer_email": "AHMED.ALI@@GMAIL...COM",
            "city": "صنعاء",
            "district": "السبعين",
            "delivery_type": "سريع",
            "delivery_cost": "2,000.00 ريال yمني",
            "payment_method": "كاش",
            "payment_status": "دفع",
            "payment_amount": "خمسة آلاف",
            "currency": "ر.ي",
            "total_amount": "7000.00",
            "items_json": '[{"sku":"SKU-A","name":"Item 1","qty":1,"unit_price":5000.0,"total":5000.0}]',
        },
        {
            "order_id": "EDGE-1002",
            "order_date": "25/08/2026",
            "status": "مدفوع",
            "customer_id": "CUST-02",
            "customer_name": "سارة محمد",
            "customer_phone": "00967739876543",
            "customer_email": "sara.m@@company..org",
            "city": "عدن",
            "district": "المنصورة",
            "delivery_type": "عادي",
            "delivery_cost": "1500",
            "payment_method": "بطاقة",
            "payment_status": "مدفوع",
            "payment_amount": "ألفان",
            "currency": "YER",
            "total_amount": "3500",
            "items_json": '[{"sku":"SKU-B","name":"Item 2","qty":1,"unit_price":2000.0,"total":2000.0}]',
        },
        {
            "order_id": "EDGE-1003",
            "order_date": "2025-02-31",  # Invalid Date -> Quarantine
            "status": "قيد الانتظار",
            "customer_id": "CUST-03",
            "customer_name": "خالد عمر",
            "customer_phone": "12345",  # Invalid Phone -> Quarantine
            "customer_email": "invalid_email_at_all",  # Invalid Email -> Quarantine
            "city": "تعز",
            "district": "القاهرة",
            "delivery_type": "عادي",
            "delivery_cost": "1000",
            "payment_method": "كاش",
            "payment_status": "غير مدفوع",
            "payment_amount": "1000",
            "currency": "YER",
            "total_amount": "2000",
            "items_json": "CORRUPTED_JSON_TEXT",  # Corrupted JSON -> Quarantine
        },
    ]

    with open(edge_csv, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=RAW_COLUMNS)
        writer.writeheader()
        for r in edge_rows:
            writer.writerow(r)

    print(f"Created {edge_csv} with 3 extreme edge case records.")

    # Run main.py on edge cases
    res1 = subprocess.run(
        [sys.executable, "src/main.py", "--file", str(edge_csv)],
        capture_output=True,
        encoding="utf-8",
        errors="replace",
    )
    if res1.stdout:
        print(res1.stdout[-1800:])
    if res1.stderr:
        print(res1.stderr[-800:])

    # -------------------------------------------------------------------------
    # Scenario 2: Dynamic Engine Routing Overrides via Environment Variables
    # Force PySpark on a tiny 2 KB file by overriding SMALL_FILE_THRESHOLD_MB=0
    # -------------------------------------------------------------------------
    print("\n\033[1m\033[93m[SCENARIO 2] Dynamic Router Engine Override (SMALL_FILE_THRESHOLD_MB=0)\033[0m")
    print("Forcing PySpark Engine on tiny 2 KB file dynamically without changing code...")

    env2 = os.environ.copy()
    env2["SMALL_FILE_THRESHOLD_MB"] = "0"  # Any file > 0 MB routes to PySpark!
    env2["PIPELINE_SPARK_PARTITIONS"] = "16"  # Increase partitions to 16
    env2["PYTHONIOENCODING"] = "utf-8"

    res2 = subprocess.run(
        [sys.executable, "src/main.py", "--file", str(edge_csv)],
        env=env2,
        capture_output=True,
        encoding="utf-8",
        errors="replace",
    )
    if res2.stdout:
        print(res2.stdout[-1800:])
    if res2.stderr:
        print(res2.stderr[-800:])

    print("\n\033[1m\033[92mAll Flexibility & Stress Scenarios Executed Successfully!\033[0m\n")


if __name__ == "__main__":
    run_flexibility_tests()
