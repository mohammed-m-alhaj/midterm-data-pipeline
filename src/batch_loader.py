from __future__ import annotations

import csv
import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from bootstrap import ensure_project_root

ensure_project_root()

from pymongo import MongoClient
from pymongo.errors import BulkWriteError

from config.settings import (
    BATCH_SIZE,
    MONGO_DATABASE,
    MONGO_TIMEOUT_MS,
    MONGO_URI,
    RAW_COLLECTION,
    RAW_COLUMNS,
    SMALL_SAMPLE_FILE,
)
from src.common import get_gpu_info

logger = logging.getLogger(__name__)


def load_csv_to_raw(file_path: str | Path, run_id: str, engine_used: str = "python_batch", batch_size: int | None = None) -> dict:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    input_path = Path(file_path)
    if not input_path.is_file():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    actual_batch_size = int(batch_size) if batch_size and int(batch_size) > 0 else BATCH_SIZE
    started = time.perf_counter()
    client: MongoClient | None = None
    inserted = 0
    failed = 0
    rows_read = 0
    batch_no = 0
    batch: list[dict] = []

    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=MONGO_TIMEOUT_MS)
        client.admin.command("ping")
        collection = client[MONGO_DATABASE][RAW_COLLECTION]
        source_file = str(input_path.resolve())
        ingested_at = datetime.now(timezone.utc)

        with input_path.open("r", encoding="utf-8-sig", newline="") as src:
            reader = csv.DictReader(src)
            missing = [c for c in RAW_COLUMNS if c not in (reader.fieldnames or [])]
            if missing:
                raise ValueError(f"Missing required CSV columns: {missing}")

            for row_number, row in enumerate(reader, start=2):
                rows_read += 1
                raw_record = {key: row.get(key) for key in RAW_COLUMNS}
                raw_payload = json.dumps(raw_record, ensure_ascii=False, separators=(",", ":"))
                batch.append(
                    {
                        "run_id": run_id,
                        "source_file": source_file,
                        "source_row_number": row_number,
                        "ingested_at": ingested_at,
                        "engine_used": engine_used,
                        "raw_record": raw_payload,
                    }
                )

                if len(batch) >= actual_batch_size:
                    batch_no += 1
                    t0 = time.perf_counter()
                    try:
                        result = collection.insert_many(batch, ordered=False)
                        inserted += len(result.inserted_ids)
                    except BulkWriteError as exc:
                        details = exc.details or {}
                        inserted += int(details.get("nInserted", 0))
                        failed += len(details.get("writeErrors", []))
                        print(f"\033[91mBatch {batch_no} failed: {exc}\033[0m")
                    finally:
                        elapsed = time.perf_counter() - t0
                        rate = len(batch) / elapsed if elapsed > 0 else 0.0
                        print(
                            f"\033[97mBatch \033[96m{batch_no:>3}\033[0m: "
                            f"rows=\033[92m{len(batch):>6,}\033[0m "
                            f"elapsed=\033[93m{elapsed:>6.2f}s\033[0m "
                            f"rate=\033[1m\033[95m{rate:>10.1f} rows/s\033[0m"
                        )
                    batch.clear()

            if batch:
                batch_no += 1
                t0 = time.perf_counter()
                try:
                    result = collection.insert_many(batch, ordered=False)
                    inserted += len(result.inserted_ids)
                except BulkWriteError as exc:
                    details = exc.details or {}
                    inserted += int(details.get("nInserted", 0))
                    failed += len(details.get("writeErrors", []))
                    print(f"\033[91mBatch {batch_no} failed: {exc}\033[0m")
                finally:
                    elapsed = time.perf_counter() - t0
                    rate = len(batch) / elapsed if elapsed > 0 else 0.0
                    print(
                        f"\033[97mBatch \033[96m{batch_no:>3}\033[0m: "
                        f"rows=\033[92m{len(batch):>6,}\033[0m "
                        f"elapsed=\033[93m{elapsed:>6.2f}s\033[0m "
                        f"rate=\033[1m\033[95m{rate:>10.1f} rows/s\033[0m"
                    )

        total_seconds = time.perf_counter() - started
        gpu_info = get_gpu_info()

        print("\033[96m" + "=" * 65 + "\033[0m")
        print("\033[1m\033[92mPYTHON BATCH RAW LOAD & HARDWARE MONITORING\033[0m")
        print("\033[96m" + "=" * 65 + "\033[0m")
        print(f"\033[97mHost Hardware Info :\033[0m \033[95m{gpu_info}\033[0m")
        print(f"\033[97mRows read          :\033[0m \033[96m{rows_read:,}\033[0m")
        print(f"\033[97mRaw inserted       :\033[0m \033[92m{inserted:,}\033[0m")
        print(f"\033[97mBatch failures     :\033[0m \033[91m{failed:,}\033[0m")
        print(f"\033[97mBatches            :\033[0m \033[96m{batch_no:,}\033[0m")
        print(f"\033[97mElapsed seconds    :\033[0m \033[93m{total_seconds:.2f}s\033[0m")
        print(f"\033[97mThroughput         :\033[0m \033[1m\033[92m{rows_read / total_seconds if total_seconds else 0:.2f} rows/s\033[0m")
        print("\033[96m" + "=" * 65 + "\033[0m\n")

        return {
            "rows_read": rows_read,
            "raw_loaded": inserted,
            "batch_failures": failed,
            "batch_size": BATCH_SIZE,
            "batches": batch_no,
            "elapsed_seconds": total_seconds,
            "throughput": rows_read / total_seconds if total_seconds else 0.0,
        }
    finally:
        if client is not None:
            try:
                client.close()
            except Exception as exc:
                logger.warning("MongoDB client shutdown warning: %s", exc)


if __name__ == "__main__":
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    target = sys.argv[1] if len(sys.argv) > 1 else SMALL_SAMPLE_FILE
    run_id = uuid4().hex
    load_csv_to_raw(target, run_id)
