from __future__ import annotations

import csv
from pathlib import Path

from bootstrap import ensure_project_root

ensure_project_root()

from config.settings import DATA_DIR, HUGE_FILE, RAW_COLUMNS


def generate_4_test_files():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    source_file = HUGE_FILE if HUGE_FILE.is_file() else DATA_DIR / "orders_small_sample.csv"
    if not source_file.is_file():
        raise FileNotFoundError(f"Source file not found: {source_file}")

    print(f"Reading from source: {source_file}")
    rows = []
    with open(source_file, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
            if len(rows) >= 60000:
                break

    print(f"Loaded {len(rows)} template rows.")

    # -------------------------------------------------------------------------
    # File 1: test_1_small_clean.csv (5,000 rows - Small Clean Batch)
    # -------------------------------------------------------------------------
    file1 = DATA_DIR / "test_1_small_clean.csv"
    with open(file1, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=RAW_COLUMNS)
        writer.writeheader()
        for r in rows[:5000]:
            writer.writerow({k: r.get(k, "") for k in RAW_COLUMNS})
    print(f"Created File 1: {file1} ({file1.stat().st_size / (1024*1024):.2f} MB, 5,000 rows)")

    # -------------------------------------------------------------------------
    # File 2: test_2_small_dirty.csv (10,000 rows - Mixed Quality & Quarantine)
    # -------------------------------------------------------------------------
    file2 = DATA_DIR / "test_2_small_dirty.csv"
    with open(file2, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=RAW_COLUMNS)
        writer.writeheader()
        for r in rows[5000:15000]:
            writer.writerow({k: r.get(k, "") for k in RAW_COLUMNS})
    print(f"Created File 2: {file2} ({file2.stat().st_size / (1024*1024):.2f} MB, 10,000 rows)")

    # -------------------------------------------------------------------------
    # File 3: test_3_large_dataset.csv (500,000 rows ~ 215 MB -> PySpark Engine)
    # -------------------------------------------------------------------------
    file3 = DATA_DIR / "test_3_large_dataset.csv"
    repeat_count = 17  # 30,000 rows * 17 iterations ~ 510,000 rows (~215 MB)
    target_count = 0
    with open(file3, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=RAW_COLUMNS)
        writer.writeheader()
        subset = rows[15000:45000]
        for rep in range(repeat_count):
            for i, r in enumerate(subset):
                copy_r = dict(r)
                copy_r["order_id"] = f"LARGE-ORDER-{rep:03d}-{i:05d}"
                writer.writerow({k: copy_r.get(k, "") for k in RAW_COLUMNS})
                target_count += 1

    size_mb = file3.stat().st_size / (1024 * 1024)
    print(f"Created File 3: {file3} ({size_mb:.2f} MB, {target_count:,} rows) -> Triggers PySpark Router")

    # -------------------------------------------------------------------------
    # File 4: test_4_duplicate_update.csv (Re-running File 1 with updates)
    # -------------------------------------------------------------------------
    file4 = DATA_DIR / "test_4_duplicate_update.csv"
    with open(file4, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=RAW_COLUMNS)
        writer.writeheader()
        for i, r in enumerate(rows[:5000]):
            copy_r = dict(r)
            if i % 10 == 0:
                copy_r["customer_phone"] = "779998877"
                copy_r["delivery_cost"] = "9900"
            writer.writerow({k: copy_r.get(k, "") for k in RAW_COLUMNS})
    print(f"Created File 4: {file4} ({file4.stat().st_size / (1024*1024):.2f} MB, 5,000 rows) -> Tests Idempotency & In-Place Update")

    print("\nAll 4 test files generated successfully!")


if __name__ == "__main__":
    generate_4_test_files()
