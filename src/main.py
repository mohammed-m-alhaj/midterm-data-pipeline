from __future__ import annotations

import argparse

from bootstrap import ensure_project_root

ensure_project_root()

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
from src.common import get_gpu_info
from src.file_router import route_file
from src.metrics import append_run_metrics
from src.mongo_setup import setup_mongodb


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

    gpu_info = get_gpu_info()

    print("\033[96m" + "=" * 70 + "\033[0m")
    print("\033[1m\033[92mHYBRID DATA PIPELINE - ROUTER & HARDWARE ACCELERATOR\033[0m")
    print("\033[96m" + "=" * 70 + "\033[0m")
    print(f"\033[97mGPU Accelerator :\033[0m \033[95m{gpu_info}\033[0m")
    print(f"\033[97mRun ID          :\033[0m \033[96m{decision['run_id']}\033[0m")
    print(f"\033[97mFile            :\033[0m \033[97m{decision['file_path']}\033[0m")
    print(f"\033[97mFile size       :\033[0m \033[93m{decision['file_size_mb']:.2f} MB\033[0m")
    print(f"\033[97mThreshold       :\033[0m \033[93m{decision['threshold_mb']} MB\033[0m")
    print(f"\033[97mEngine          :\033[0m \033[1m\033[92m{decision['engine']}\033[0m")
    print(f"\033[97mReason          :\033[0m \033[96m{decision['reason']}\033[0m")
    print(f"\033[97mSpark master    :\033[0m \033[96m{SPARK_MASTER_URL}\033[0m")
    print("\033[96m" + "=" * 70 + "\033[0m\n")

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
                f"\033[93mELT deferred: full local processing is disabled "
                f"for files above {LOCAL_ELT_MAX_MB} MB.\033[0m"
            )

            print(
                "\033[93mUse the 100k/1M benchmark locally or run the "
                "full ELT on Path A Spark Standalone.\033[0m"
            )

        else:
            from src.elt_pipeline import process_run

            process_run(
                decision["run_id"],
                decision["file_path"],
            )

    else:

        print(
            "\033[93mELT skipped because "
            "PIPELINE_RUN_ELT_AFTER_RAW=false\033[0m"
        )

    print("\033[1m\033[92mPipeline completed successfully.\033[0m\n")


if __name__ == "__main__":
    main()
