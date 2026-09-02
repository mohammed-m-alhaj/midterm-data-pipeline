"""Real Update Test & Evidence Generator for Path A."""
from __future__ import annotations

import csv
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from bootstrap import PROJECT_ROOT, ensure_project_root

ensure_project_root()
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

from pymongo import MongoClient

from config.settings import (
    IVY_JARS_DIR,
    MONGO_DATABASE,
    MONGO_TIMEOUT_MS,
    MONGO_URI,
    RAW_COLUMNS,
    REPORTS_DIR,
    SPARK_DRIVER_MEMORY,
    SPARK_EXECUTOR_MEMORY,
    SPARK_MASTER_URL,
    UPDATE_TEST_FILE,
    VALIDATED_COLLECTION,
)

SCREENSHOT_DIR = REPORTS_DIR / "screenshots"
SCREENSHOT_DIR.mkdir(parents=True, exist_ok=True)

CHROME_PATH = "C:/Program Files/Google/Chrome/Application/chrome.exe"
if not os.path.exists(CHROME_PATH):
    CHROME_PATH = "C:/Program Files (x86)/Google/Chrome/Application/chrome.exe"
if not os.path.exists(CHROME_PATH):
    CHROME_PATH = "C:/Program Files (x86)/Microsoft/Edge/Application/msedge.exe"


def main():
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=MONGO_TIMEOUT_MS)
    db = client[MONGO_DATABASE]

    # 1. Fetch an existing validated order
    doc = db[VALIDATED_COLLECTION].find_one({"quality_status": "corrected"})
    target_oid = doc["order_id"]
    old_phone = doc.get("customer_phone", "")
    old_cost = doc.get("delivery_cost", 0.0)
    old_hash = doc.get("record_hash", "")

    new_phone = "771234567"
    expected_cost = 8500.0 if float(old_cost or 0) == 7500.0 else 7500.0
    new_cost = str(int(expected_cost))

    initial_validated_count = db[VALIDATED_COLLECTION].count_documents({})

    print(f"[Update Test] Selected target order_id: {target_oid}")
    print(f"[Update Test] Old phone: {old_phone} | Old delivery_cost: {old_cost}")
    print(f"[Update Test] Initial validated count: {initial_validated_count}")

    # 2. Write CSV with the updated record
    update_csv = UPDATE_TEST_FILE
    row = {col: str(doc.get(col, "")) for col in RAW_COLUMNS}
    row["order_id"] = target_oid
    row["customer_phone"] = new_phone
    row["delivery_cost"] = new_cost
    row["status"] = "تم الدفع"
    row["currency"] = "YER"
    row["items_json"] = str(doc.get("items_json", '[{"sku":"SKU-1","name":"Test","qty":1,"unit_price":10000.0,"total":10000.0}]'))

    with open(update_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=RAW_COLUMNS)
        writer.writeheader()
        writer.writerow(row)

    print(f"[Update Test] Created {update_csv}")

    # 3. Execute pipeline via spark-submit on Standalone Master
    pyspark_path = subprocess.check_output(
        [sys.executable, "-c", "import pyspark, os; print(os.path.dirname(pyspark.__file__))"]
    ).decode().strip()

    spark_submit = Path(pyspark_path) / "bin" / "spark-submit.cmd"
    ivy_jars = list(IVY_JARS_DIR.glob("*.jar")) if IVY_JARS_DIR.is_dir() else []
    jars_arg = ",".join(str(j) for j in ivy_jars) if ivy_jars else ""

    spark_master = SPARK_MASTER_URL

    cmd = [
        str(spark_submit),
        "--master", spark_master,
        "--driver-memory", SPARK_DRIVER_MEMORY,
        "--executor-memory", SPARK_EXECUTOR_MEMORY,
        "--conf", "spark.sql.adaptive.enabled=true",
        "--conf", "spark.sql.ansi.enabled=false",
    ]
    if jars_arg:
        cmd.extend(["--jars", jars_arg])
    else:
        cmd.extend(["--packages", "org.mongodb.spark:mongo-spark-connector_2.13:11.1.0"])

    cmd.extend(["src/main.py", "--file", str(update_csv)])

    env = os.environ.copy()
    env["PIPELINE_SPARK_MASTER"] = spark_master
    env["PIPELINE_INPUT_FILE"] = str(update_csv)
    env["PIPELINE_RUN_ELT_AFTER_RAW"] = "true"
    env["PIPELINE_ALLOW_FULL_LOCAL_ELT"] = "true"

    print("[Update Test] Running spark-submit on Standalone Master...")
    res = subprocess.run(cmd, env=env, check=True, capture_output=True, encoding="utf-8", errors="replace")
    if res.stdout:
        print(res.stdout[-1500:])
    if res.stderr:
        print(res.stderr[-800:])

    # 4. Verify MongoDB state
    updated_doc = db[VALIDATED_COLLECTION].find_one({"order_id": target_oid})
    new_doc_phone = updated_doc.get("customer_phone")
    new_doc_cost = updated_doc.get("delivery_cost")
    new_doc_hash = updated_doc.get("record_hash")
    total_validated = db[VALIDATED_COLLECTION].count_documents({})

    print(f"[Update Test] Updated doc phone in MongoDB: {new_doc_phone}")
    print(f"[Update Test] Updated doc cost in MongoDB : {new_doc_cost}")
    print(f"[Update Test] Total validated documents   : {total_validated}")

    assert total_validated == initial_validated_count, f"Expected {initial_validated_count}, got {total_validated}"
    assert new_doc_phone == "+967771234567", f"Expected +967771234567, got {new_doc_phone}"
    assert new_doc_cost == expected_cost, f"Expected {expected_cost}, got {new_doc_cost}"
    assert new_doc_hash != old_hash, "Record hash must change after update"

    # 5. Capture 11_update_evidence.png (optional)
    try:
        from src.generate_all_evidence import capture_html, html_card

        update_html = html_card(
            title="Real Update Verification — In-Place Mutation via SHA-256 Hash Diff",
            subtitle=f"Order {target_oid} updated with new phone and delivery cost without inserting duplicate",
            badge="Update: SUCCESS (1 Record)",
            content_html=f"""
            <div class="stat-grid">
              <div class="stat-box"><div class="stat-val">1</div><div class="stat-lbl">Processed Records</div></div>
              <div class="stat-box"><div class="stat-val success">1</div><div class="stat-lbl">Updated Count</div></div>
              <div class="stat-box"><div class="stat-val">0</div><div class="stat-lbl">Inserted Count</div></div>
              <div class="stat-box"><div class="stat-val success">{total_validated:,}</div><div class="stat-lbl">Total Validated (Zero Dupes)</div></div>
            </div>
            <table class="diff-table">
              <thead>
                <tr>
                  <th>Field</th>
                  <th>Before Update (Run 1)</th>
                  <th>After Update (Mutation)</th>
                  <th>Resolution Status</th>
                </tr>
              </thead>
              <tbody>
                <tr>
                  <td><strong>order_id</strong></td>
                  <td>{target_oid}</td>
                  <td>{target_oid}</td>
                  <td style="color:#38bdf8;">Stable Primary Key</td>
                </tr>
                <tr>
                  <td><strong>customer_phone</strong></td>
                  <td class="diff-old">{old_phone}</td>
                  <td class="diff-new">{new_doc_phone}</td>
                  <td style="color:#4ade80;">Updated in-place</td>
                </tr>
                <tr>
                  <td><strong>delivery_cost</strong></td>
                  <td class="diff-old">{old_cost}</td>
                  <td class="diff-new">{new_doc_cost}</td>
                  <td style="color:#4ade80;">Updated in-place</td>
                </tr>
                <tr>
                  <td><strong>record_hash (SHA-256)</strong></td>
                  <td class="diff-old" style="font-size:11px;">{old_hash[:32]}...</td>
                  <td class="diff-new" style="font-size:11px;">{new_doc_hash[:32]}...</td>
                  <td style="color:#4ade80;">New Hash Recomputed</td>
                </tr>
              </tbody>
            </table>
            """,
            theme="mongo",
        )
        capture_html(update_html, SCREENSHOT_DIR / "11_update_evidence.png")
        print("[Update Test] 11_update_evidence.png saved successfully!")
    except Exception:
        print("[Update Test] Image evidence step skipped.")

    print("\033[1m\033[92m[Update Test PASSED] Order updated in-place cleanly without duplicate insertion!\033[0m")


if __name__ == "__main__":
    main()
