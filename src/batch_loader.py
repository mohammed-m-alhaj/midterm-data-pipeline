from __future__ import annotations

from common import PROJECT_ROOT  # noqa: F401

import csv
import json
import logging
import time
from pathlib import Path

from pymongo import MongoClient
from pymongo.errors import BulkWriteError

from config.settings import (
    BATCH_SIZE,
    MONGO_DATABASE,
    MONGO_URI,
    RAW_COLLECTION,
    RAW_COLUMNS,
)

logger = logging.getLogger(__name__)


def load_csv_to_raw(file_path: str | Path, run_id: str, engine_used: str = "python_batch") -> dict:
    input_path = Path(file_path)
    if not input_path.is_file():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    started = time.perf_counter()
    client: MongoClient | None = None
    inserted = 0
    failed = 0
    rows_read = 0
    batch_no = 0
    batch: list[dict] = []

    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        collection = client[MONGO_DATABASE][RAW_COLLECTION]
        source_file = str(input_path.resolve())
        ingested_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

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
                        "record_raw": raw_payload,
                    }
                )

                if len(batch) >= BATCH_SIZE:
                    batch_no += 1
                    t0 = time.perf_counter()
                    try:
                        result = collection.insert_many(batch, ordered=False)
                        inserted += len(result.inserted_ids)
                    except BulkWriteError as exc:
                        details = exc.details or {}
                        inserted += int(details.get("nInserted", 0))
                        failed += len(details.get("writeErrors", []))
                        print(f"Batch {batch_no} failed: {exc}")
                    finally:
                        elapsed = time.perf_counter() - t0
                        rate = len(batch) / elapsed if elapsed > 0 else 0.0
                        print(
                            f"Batch {batch_no:>5}: rows={len(batch):>6,} "
                            f"elapsed={elapsed:>7.2f}s rate={rate:>10.1f} rows/s"
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
                    print(f"Batch {batch_no} failed: {exc}")
                finally:
                    elapsed = time.perf_counter() - t0
                    rate = len(batch) / elapsed if elapsed > 0 else 0.0
                    print(
                        f"Batch {batch_no:>5}: rows={len(batch):>6,} "
                        f"elapsed={elapsed:>7.2f}s rate={rate:>10.1f} rows/s"
                    )

        total_seconds = time.perf_counter() - started
        gpu_info = "N/A"
        try:
            import subprocess
            gpu_out = subprocess.check_output("nvidia-smi --query-gpu=name,memory.total --format=csv,noheader", shell=True, stderr=subprocess.DEVNULL).decode().strip()
            if gpu_out:
                gpu_info = f"{gpu_out} [ACTIVE]"
        except Exception:
            pass

        print("=" * 60)
        print("PYTHON BATCH RAW LOAD & HARDWARE MONITORING")
        print("=" * 60)
        print(f"Host Hardware Info : {gpu_info}")
        print(f"Rows read       : {rows_read:,}")
        print(f"Raw inserted    : {inserted:,}")
        print(f"Batch failures  : {failed:,}")
        print(f"Batches         : {batch_no:,}")
        print(f"Elapsed seconds : {total_seconds:.2f}")
        print(f"Throughput      : {rows_read / total_seconds if total_seconds else 0:.2f} rows/s")
        print("=" * 60)

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
    import sys
    from uuid import uuid4
    from config.settings import SMALL_SAMPLE_FILE

    target = sys.argv[1] if len(sys.argv) > 1 else SMALL_SAMPLE_FILE
    run_id = uuid4().hex
    load_csv_to_raw(target, run_id)
