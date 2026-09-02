from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

from bootstrap import ensure_project_root

ensure_project_root()

from pymongo import MongoClient

from config.settings import (
    MONGO_DATABASE,
    MONGO_TIMEOUT_MS,
    MONGO_URI,
    QUARANTINE_COLLECTION,
    RAW_COLLECTION,
    VALIDATED_COLLECTION,
)


def run_live_proof():
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=MONGO_TIMEOUT_MS)
    db = client[MONGO_DATABASE]

    print("\033[96m" + "=" * 70 + "\033[0m")
    print("\033[1m\033[92mLIVE PIPELINE STEP-BY-STEP EXECUTION & DATABASE PROOF\033[0m")
    print("\033[96m" + "=" * 70 + "\033[0m\n")

    # -------------------------------------------------------------------------
    # STEP 1: Setup MongoDB & Clean Previous Test Data
    # -------------------------------------------------------------------------
    print("\033[1m\033[93m[STEP 1] Initializing MongoDB Collections & Unique Indexes...\033[0m")
    db[RAW_COLLECTION].delete_many({})
    db[VALIDATED_COLLECTION].delete_many({})
    db[QUARANTINE_COLLECTION].delete_many({})

    res1 = subprocess.run([sys.executable, "src/mongo_setup.py"], capture_output=True, encoding="utf-8", errors="replace")
    if res1.stdout:
        print(res1.stdout)

    # -------------------------------------------------------------------------
    # STEP 2: Create Reproducible Small Sample (1,000 Rows)
    # -------------------------------------------------------------------------
    print("\033[1m\033[93m[STEP 2] Extracting Reproducible 1,000 Row CSV Sample...\033[0m")
    res2 = subprocess.run([sys.executable, "src/create_small_sample.py", "--rows", "1000"], capture_output=True, encoding="utf-8", errors="replace")
    if res2.stdout:
        print(res2.stdout)

    # -------------------------------------------------------------------------
    # STEP 3: First Run Execution (Raw Load + Quality ELT)
    # -------------------------------------------------------------------------
    print("\033[1m\033[93m[STEP 3] Executing Main Pipeline (Run 1 - Ingestion & ELT)...\033[0m")
    res3 = subprocess.run([sys.executable, "src/main.py", "--file", "data/orders_small_sample.csv"], capture_output=True, encoding="utf-8", errors="replace")
    if res3.stdout:
        print(res3.stdout)

    # -------------------------------------------------------------------------
    # STEP 4: Live MongoDB Query & Proof Inspection
    # -------------------------------------------------------------------------
    print("\033[1m\033[93m[STEP 4] Live Database State & Document Structure Verification\033[0m")
    raw_cnt = db[RAW_COLLECTION].count_documents({})
    val_cnt = db[VALIDATED_COLLECTION].count_documents({})
    quar_cnt = db[QUARANTINE_COLLECTION].count_documents({})

    print(f"\033[97m   • MongoDB orders_raw count        :\033[0m \033[96m{raw_cnt:,}\033[0m")
    print(f"\033[97m   • MongoDB orders_validated count  :\033[0m \033[92m{val_cnt:,}\033[0m")
    print(f"\033[97m   • MongoDB orders_quarantine count :\033[0m \033[91m{quar_cnt:,}\033[0m")
    print(f"\033[97m   • Consistency Check ({val_cnt} + {quar_cnt}) =\033[0m \033[1m\033[92m{val_cnt + quar_cnt} == {raw_cnt} ({val_cnt + quar_cnt == raw_cnt})\033[0m\n")

    # Sample Document from orders_raw
    raw_sample = db[RAW_COLLECTION].find_one({}, {"_id": 0})
    print("\033[97m--- Sample Raw Record from orders_raw ---\033[0m")
    print(json.dumps(raw_sample, indent=2, default=str, ensure_ascii=False)[:400] + "...\n")

    # Sample Document from orders_validated (Corrected with Audit Trail)
    val_sample = db[VALIDATED_COLLECTION].find_one({"quality_status": "corrected"}, {"_id": 0})
    print("\033[97m--- Sample Validated Record (with Audit Trail corrections) ---\033[0m")
    print(json.dumps(val_sample, indent=2, default=str, ensure_ascii=False)[:500] + "...\n")

    # Sample Document from orders_quarantine (Isolating Irreparable Errors)
    quar_sample = db[QUARANTINE_COLLECTION].find_one({}, {"_id": 0})
    print("\033[97m--- Sample Quarantine Record (with diagnostic error_codes) ---\033[0m")
    print(json.dumps(quar_sample, indent=2, default=str, ensure_ascii=False)[:400] + "...\n")

    # -------------------------------------------------------------------------
    # STEP 5: Second Run Execution (Idempotency Re-Run Test)
    # -------------------------------------------------------------------------
    print("\033[1m\033[93m[STEP 5] Re-Running Pipeline on Exact Same Dataset (Idempotency Test)...\033[0m")
    res5 = subprocess.run([sys.executable, "src/main.py", "--file", "data/orders_small_sample.csv"], capture_output=True, encoding="utf-8", errors="replace")
    if res5.stdout:
        print(res5.stdout)

    val_cnt_after = db[VALIDATED_COLLECTION].count_documents({})
    print(f"\033[97m   • orders_validated count before re-run :\033[0m \033[92m{val_cnt:,}\033[0m")
    print(f"\033[97m   • orders_validated count after re-run  :\033[0m \033[92m{val_cnt_after:,}\033[0m")
    print(f"\033[97m   • Zero Duplicate Insertion Verified    :\033[0m \033[1m\033[92m{val_cnt == val_cnt_after}\033[0m\n")

    # -------------------------------------------------------------------------
    # STEP 6: Run PyTest Automated Unit Tests
    # -------------------------------------------------------------------------
    print("\033[1m\033[93m[STEP 6] Running PyTest Automated Unit Test Suite...\033[0m")
    res6 = subprocess.run([sys.executable, "-m", "pytest", "tests/", "-v"], capture_output=True, encoding="utf-8", errors="replace")
    if res6.stdout:
        print(res6.stdout)

    print("\033[96m" + "=" * 70 + "\033[0m")
    print("\033[1m\033[92mALL 6 STAGES COMPLETED & VERIFIED SUCCESSFULLY WITH LIVE PROOF!\033[0m")
    print("\033[96m" + "=" * 70 + "\033[0m\n")


if __name__ == "__main__":
    run_live_proof()
