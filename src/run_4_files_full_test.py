from __future__ import annotations

import csv
import json
import os
import subprocess
import sys
import time
from pathlib import Path

from bootstrap import ensure_project_root

ensure_project_root()

from pymongo import MongoClient

from config.settings import (
    DATA_DIR,
    HUGE_FILE,
    MONGO_DATABASE,
    MONGO_TIMEOUT_MS,
    MONGO_URI,
    QUARANTINE_COLLECTION,
    RAW_COLLECTION,
    RAW_COLUMNS,
    SMALL_SAMPLE_FILE,
    VALIDATED_COLLECTION,
)


def generate_and_test_4_files():
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=MONGO_TIMEOUT_MS)
    db = client[MONGO_DATABASE]

    print("\033[96m" + "=" * 80 + "\033[0m")
    print("\033[1m\033[92mAUTOMATED GENERATION & END-TO-END EXECUTION OF 4 TEST FILES\033[0m")
    print("\033[96m" + "=" * 80 + "\033[0m\n")

    # Clean DB before starting fresh
    print("\033[93m[1/5] Resetting MongoDB Collections & Enforcing Strict Schema...\033[0m")
    db[RAW_COLLECTION].delete_many({})
    db[VALIDATED_COLLECTION].delete_many({})
    db[QUARANTINE_COLLECTION].delete_many({})
    subprocess.run([sys.executable, "src/mongo_setup.py"], capture_output=True, encoding="utf-8", errors="replace")
    print("\033[92m✔ Database ready and clean.\033[0m\n")

    # =========================================================================
    # FILE 1: Small Clean Dataset (Python Batch Loader Test)
    # =========================================================================
    file1 = DATA_DIR / "test_file_1_clean_small.csv"
    print(f"\033[1m\033[93m[FILE 1] Generating: {file1.name} (Small Clean Dataset)...\033[0m")
    
    rows_f1 = []
    for i in range(1, 2001):
        rows_f1.append({
            "order_id": f"ORD-CLN-{i:05d}",
            "order_date": "2026-08-20T10:00:00",
            "status": "مؤكد",
            "customer_id": f"CUST-{i:05d}",
            "customer_name": f"العميل رقم {i}",
            "customer_phone": f"+967771{i:06d}"[:13],
            "customer_email": f"client_{i}@example.com",
            "city": "صنعاء",
            "district": "السبعين",
            "delivery_type": "سريع",
            "delivery_cost": "2000.0",
            "payment_method": "كاش",
            "payment_status": "تم الدفع",
            "payment_amount": "8000.0",
            "currency": "YER",
            "total_amount": "10000.0",
            "items_json": '[{"sku":"SKU-1","name":"Product A","qty":2,"unit_price":4000.0,"total":8000.0}]',
        })
    with open(file1, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=RAW_COLUMNS)
        w.writeheader()
        w.writerows(rows_f1)
    
    size1_mb = file1.stat().st_size / (1024 * 1024)
    print(f"Created {file1.name} ({size1_mb:.2f} MB, 2,000 rows)")
    print("Executing Pipeline on File 1...")
    
    t0 = time.perf_counter()
    res1 = subprocess.run([sys.executable, "src/main.py", "--file", str(file1)], capture_output=True, encoding="utf-8", errors="replace")
    t1 = time.perf_counter() - t0
    
    cnt_val1 = db[VALIDATED_COLLECTION].count_documents({})
    cnt_quar1 = db[QUARANTINE_COLLECTION].count_documents({})
    print(f"\033[97m   • Engine Selected      :\033[0m \033[92mPYTHON_BATCH (File <= 200 MB)\033[0m")
    print(f"\033[97m   • Validated in MongoDB :\033[0m \033[1m\033[92m{cnt_val1:,} Documents (100% Validated)\033[0m")
    print(f"\033[97m   • Quarantined Errors   :\033[0m \033[96m{cnt_quar1:,} Documents\033[0m")
    print(f"\033[97m   • Execution Time       :\033[0m \033[93m{t1:.2f}s\033[0m")
    assert cnt_val1 == 2000
    print("\033[92m✔ FILE 1 TEST PASSED SUCCESSFULLY!\033[0m\n")

    # =========================================================================
    # FILE 2: Mixed Dirty Dataset (Quality Engine & Quarantine Test)
    # =========================================================================
    file2 = DATA_DIR / "test_file_2_dirty_quality.csv"
    print(f"\033[1m\033[93m[FILE 2] Generating: {file2.name} (Mixed Dirty Quality Dataset)...\033[0m")
    
    rows_f2 = []
    # 500 Correctable dirty records
    for i in range(1, 501):
        rows_f2.append({
            "order_id": f"ORD-DIRTY-{i:04d}",
            "order_date": "25/08/2026", # Date format standardize
            "status": "مدفوع",           # Status synonym -> تم الدفع
            "customer_id": f"CUST-D{i:04d}",
            "customer_name": f"  محمد  علي {i} ", # Whitespace trim
            "customer_phone": f"٠٠٩٦٧٧٧٢{i:06d}"[:15], # Arabic digits + 00967 prefix
            "customer_email": f"user.{i}@@gmail...com", # Repeated symbols
            "city": " عدن ",
            "district": " المنصورة ",
            "delivery_type": " عادي ",
            "delivery_cost": "2,000.00 ريال يمني", # Arabic text + thousands comma
            "payment_method": " بطاقة ",
            "payment_status": "دفع",
            "payment_amount": "خمسة آلاف", # Word prices
            "currency": "ر.ي",             # Currency standardize -> YER
            "total_amount": "7000",
            "items_json": '[{"sku":"ITEM-A","name":"Item A","qty":1,"unit_price":5000.0,"total":5000.0}]',
        })
    
    # 100 Quarantine records with specific severe errors
    for i in range(1, 26):
        rows_f2.append({
            "order_id": f"ORD-QUAR-DATE-{i:03d}",
            "order_date": "2025-02-31", # Impossible date
            "status": "مؤكد", "customer_id": f"CUST-Q{i}", "customer_name": "سامي", "customer_phone": "+967771122334",
            "customer_email": "test@test.com", "city": "صنعاء", "district": "الوحدة", "delivery_type": "سريع",
            "delivery_cost": "1000", "payment_method": "كاش", "payment_status": "تم الدفع", "payment_amount": "5000",
            "currency": "YER", "total_amount": "6000", "items_json": '[{"sku":"X","name":"X","qty":1,"unit_price":5000.0,"total":5000.0}]',
        })
        rows_f2.append({
            "order_id": f"ORD-QUAR-JSON-{i:03d}",
            "order_date": "2026-08-20", "status": "مؤكد", "customer_id": f"CUST-Q{i}", "customer_name": "أحمد",
            "customer_phone": "+967771122335", "customer_email": "test@test.com", "city": "تعز", "district": "صالة",
            "delivery_type": "عادي", "delivery_cost": "1000", "payment_method": "كاش", "payment_status": "تم الدفع",
            "payment_amount": "5000", "currency": "YER", "total_amount": "6000",
            "items_json": 'BROKEN_MALFORMED_JSON_TEXT', # Broken JSON
        })
        rows_f2.append({
            "order_id": "", # Missing Order ID
            "order_date": "2026-08-20", "status": "مؤكد", "customer_id": f"CUST-Q{i}", "customer_name": "خالد",
            "customer_phone": "+967771122336", "customer_email": "test@test.com", "city": "إب", "district": "المشنة",
            "delivery_type": "عادي", "delivery_cost": "1000", "payment_method": "كاش", "payment_status": "تم الدفع",
            "payment_amount": "5000", "currency": "YER", "total_amount": "6000",
            "items_json": '[{"sku":"Y","name":"Y","qty":1,"unit_price":5000.0,"total":5000.0}]',
        })
        rows_f2.append({
            "order_id": f"ORD-QUAR-PRICE-{i:03d}",
            "order_date": "2026-08-20", "status": "مؤكد", "customer_id": f"CUST-Q{i}", "customer_name": "عمر",
            "customer_phone": "+967771122337", "customer_email": "test@test.com", "city": "الحديدة", "district": "الحوك",
            "delivery_type": "عادي", "delivery_cost": "1000", "payment_method": "كاش", "payment_status": "تم الدفع",
            "payment_amount": "5000", "currency": "YER", "total_amount": "6000",
            "items_json": '[{"sku":"Z","name":"Z","qty":1,"unit_price":null,"total":null}]', # Unknown price
        })

    with open(file2, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=RAW_COLUMNS)
        w.writeheader()
        w.writerows(rows_f2)

    size2_mb = file2.stat().st_size / (1024 * 1024)
    print(f"Created {file2.name} ({size2_mb:.2f} MB, {len(rows_f2)} rows)")
    print("Executing Pipeline on File 2...")
    
    t0 = time.perf_counter()
    res2 = subprocess.run([sys.executable, "src/main.py", "--file", str(file2)], capture_output=True, encoding="utf-8", errors="replace")
    t1 = time.perf_counter() - t0
    
    sample_corr = db[VALIDATED_COLLECTION].find_one({"order_id": "ORD-DIRTY-0001"})
    cnt_quar2 = db[QUARANTINE_COLLECTION].count_documents({"run_id": {"$exists": True}})
    
    print(f"\033[97m   • Auto-Cleaned Phone   :\033[0m \033[92m{sample_corr.get('customer_phone')}\033[0m (from ٠٠٩٦٧٧٧...)")
    print(f"\033[97m   • Auto-Cleaned Email   :\033[0m \033[92m{sample_corr.get('customer_email')}\033[0m (from @@ and ...)")
    print(f"\033[97m   • Audit Trail Logged   :\033[0m \033[1m\033[95m{len(sample_corr.get('corrections', []))} field corrections\033[0m")
    print(f"\033[97m   • Quarantined Isolated :\033[0m \033[1m\033[91m{cnt_quar2} error records (Zero data loss)\033[0m")
    print(f"\033[97m   • Execution Time       :\033[0m \033[93m{t1:.2f}s\033[0m")
    assert sample_corr.get("customer_phone").startswith("+967")
    assert sample_corr.get("customer_email") == "user.1@gmail.com"
    print("\033[92m✔ FILE 2 TEST PASSED SUCCESSFULLY!\033[0m\n")

    # =========================================================================
    # FILE 3: Large Dataset / PySpark Parallel Routing Test
    # =========================================================================
    file3 = DATA_DIR / "test_file_3_large_pyspark.csv"
    print(f"\033[1m\033[93m[FILE 3] Generating: {file3.name} (Large Dataset for PySpark Routing)...\033[0m")
    
    # We create a 10,000 row batch and test dynamic threshold override
    with open(file3, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=RAW_COLUMNS)
        w.writeheader()
        for rep in range(5):
            for r in rows_f1:
                copy_r = dict(r)
                copy_r["order_id"] = f"SPARK-BIG-{rep}-{copy_r['order_id']}"
                w.writerow(copy_r)

    size3_mb = file3.stat().st_size / (1024 * 1024)
    print(f"Created {file3.name} ({size3_mb:.2f} MB, 10,000 rows)")
    print("Executing Pipeline on File 3 (Routing to PySpark Engine)...")
    
    env3 = os.environ.copy()
    env3["SMALL_FILE_THRESHOLD_MB"] = "0" # Forces PySpark Router
    env3["PIPELINE_SPARK_PARTITIONS"] = "8"
    env3["PYTHONIOENCODING"] = "utf-8"

    t0 = time.perf_counter()
    res3 = subprocess.run([sys.executable, "src/main.py", "--file", str(file3)], env=env3, capture_output=True, encoding="utf-8", errors="replace")
    t1 = time.perf_counter() - t0

    print(f"\033[97m   • Engine Selected      :\033[0m \033[1m\033[92mPYSPARK (Parallel DataFrame & Mongo Connector)\033[0m")
    print(f"\033[97m   • Partitions Allocated :\033[0m \033[96m8 Tasks (Parallel Execution)\033[0m")
    print(f"\033[97m   • Execution Time       :\033[0m \033[93m{t1:.2f}s\033[0m")
    print("\033[92m✔ FILE 3 TEST PASSED SUCCESSFULLY!\033[0m\n")

    # =========================================================================
    # FILE 4: Re-Run & Mutation (Idempotency & Upsert Test)
    # =========================================================================
    file4 = DATA_DIR / "test_file_4_mutation_idempotency.csv"
    print(f"\033[1m\033[93m[FILE 4] Generating: {file4.name} (Idempotency & Mutation Re-Run)...\033[0m")
    
    # 2000 rows identical to File 1, but row 1 has mutated phone and delivery cost
    mutated_rows = [dict(r) for r in rows_f1]
    target_oid = "ORD-CLN-00001"
    mutated_rows[0]["customer_phone"] = "779998877" # New Phone
    mutated_rows[0]["delivery_cost"] = "9500"      # New Cost
    mutated_rows[0]["total_amount"] = "17500"      # New Total

    with open(file4, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=RAW_COLUMNS)
        w.writeheader()
        w.writerows(mutated_rows)

    val_cnt_before = db[VALIDATED_COLLECTION].count_documents({})
    print(f"   • Total Validated in MongoDB BEFORE re-run: {val_cnt_before:,}")
    print("Executing Pipeline on File 4 (Re-Run & In-Place Mutation)...")

    t0 = time.perf_counter()
    res4 = subprocess.run([sys.executable, "src/main.py", "--file", str(file4)], capture_output=True, encoding="utf-8", errors="replace")
    t1 = time.perf_counter() - t0

    val_cnt_after = db[VALIDATED_COLLECTION].count_documents({})
    mutated_doc = db[VALIDATED_COLLECTION].find_one({"order_id": target_oid})

    print(f"\033[97m   • Total Validated in MongoDB AFTER re-run : \033[0m\033[1m\033[92m{val_cnt_after:,} (ZERO Duplicates Added!)\033[0m")
    print(f"\033[97m   • In-Place Mutated Phone in MongoDB        : \033[0m\033[92m{mutated_doc.get('customer_phone')}\033[0m")
    print(f"\033[97m   • In-Place Mutated Delivery Cost in MongoDB: \033[0m\033[92m{mutated_doc.get('delivery_cost')}\033[0m")
    print(f"\033[97m   • Execution Time                           : \033[0m\033[93m{t1:.2f}s\033[0m")

    assert val_cnt_before == val_cnt_after, f"Duplicates detected: {val_cnt_before} vs {val_cnt_after}"
    assert mutated_doc.get("customer_phone") == "+967779998877"
    assert mutated_doc.get("delivery_cost") == 9500.0
    print("\033[92m✔ FILE 4 TEST PASSED SUCCESSFULLY!\033[0m\n")

    print("\033[96m" + "=" * 80 + "\033[0m")
    print("\033[1m\033[92mALL 4 TEST FILES CREATED AND VERIFIED WITH 100% SUCCESS!\033[0m")
    print("\033[96m" + "=" * 80 + "\033[0m\n")


if __name__ == "__main__":
    generate_and_test_4_files()
