from __future__ import annotations

import argparse
import sys

from common import PROJECT_ROOT  # noqa: F401

from config.settings import (
    ALLOW_FULL_LOCAL_ELT,
    INPUT_FILE,
    LOCAL_ELT_MAX_MB,
    MONGO_DATABASE,
    MONGO_URI,
    RUN_ELT_AFTER_RAW,
    SPARK_MASTER_URL,
    ensure_directories,
)
from src.file_router import route_file
from src.mongo_setup import setup_mongodb
from src.metrics import append_run_metrics


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Hybrid Data Pipeline Router"
    )

    parser.add_argument(
        "--file",
        type=str,
        default=str(INPUT_FILE),
        help="CSV input file. Defaults to config.settings.INPUT_FILE",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    input_file = args.file

    ensure_directories()
    setup_mongodb()

    # ------------------------------------------------------------------
    # Router decides the engine — main.py never chooses the engine itself.
    # ------------------------------------------------------------------
    decision = route_file(input_file)

    gpu_info = "N/A"
    try:
        import subprocess
        gpu_out = subprocess.check_output("nvidia-smi --query-gpu=name,memory.total --format=csv,noheader", shell=True, stderr=subprocess.DEVNULL).decode().strip()
        if gpu_out:
            gpu_info = f"{gpu_out} [ACTIVE]"
    except Exception:
        pass

    print("=" * 70)
    print("HYBRID DATA PIPELINE - ROUTER & HARDWARE ACCELERATOR")
    print("=" * 70)
    print(f"GPU Accelerator : {gpu_info}")
    print(f"Run ID          : {decision['run_id']}")
    print(f"File            : {decision['file_path']}")
    print(f"File size       : {decision['file_size_mb']:.2f} MB")
    print(f"Threshold       : {decision['threshold_mb']} MB")
    print(f"Engine          : {decision['engine']}")
    print(f"Reason          : {decision['reason']}")
    print(f"Spark master    : {SPARK_MASTER_URL}")
    print("=" * 70)

    if decision["engine"] == "python_batch":
        from src.batch_loader import load_csv_to_raw

        load_stats = load_csv_to_raw(
            decision["file_path"],
            decision["run_id"],
        )

    else:
        from src.spark_loader import load_csv_to_raw

        load_stats = load_csv_to_raw(
            decision["file_path"],
            decision["run_id"],
        )

    metrics = {
        "run_id": decision["run_id"],
        "file_name": decision["file_name"],
        "file_size_mb": decision["file_size_mb"],
        "engine_used": decision["engine"],
        "source_file": decision["file_path"],
        "threshold_mb": decision["threshold_mb"],
        **load_stats,
        "mongo_database": MONGO_DATABASE,
        "mongo_uri": MONGO_URI,
    }

    append_run_metrics(metrics)

    # ------------------------------------------------------------------
    # ELT phase — controlled entirely by settings.py constants.
    # ------------------------------------------------------------------
    if RUN_ELT_AFTER_RAW:

        # Guard: refuse full local ELT for very large files unless on a
        # real Spark cluster (spark://...) or explicitly allowed.
        is_local_master = SPARK_MASTER_URL.startswith("local[")
        too_large_for_local = (
            is_local_master
            and decision["file_size_mb"] > LOCAL_ELT_MAX_MB
            and not ALLOW_FULL_LOCAL_ELT
        )

        if too_large_for_local:

            print(
                f"ELT deferred: full local processing is disabled "
                f"for files above {LOCAL_ELT_MAX_MB} MB."
            )

            print(
                "Use the 100k/1M benchmark locally or run the "
                "full ELT on Path A Spark Standalone."
            )

        else:

            from src.elt_pipeline import process_run

            process_run(
                decision["run_id"],
                decision["file_path"],
            )

    else:

        print(
            "ELT skipped because "
            "PIPELINE_RUN_ELT_AFTER_RAW=false"
        )

    print("Pipeline completed.")


if __name__ == "__main__":
    main()
