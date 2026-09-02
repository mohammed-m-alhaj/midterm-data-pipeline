from __future__ import annotations

import sys
from pathlib import Path
from uuid import uuid4

from bootstrap import ensure_project_root

ensure_project_root()

from config.settings import SMALL_FILE_THRESHOLD_MB, SMALL_SAMPLE_FILE


def route_file(file_path: str | Path) -> dict:
    """Inspect the file size and select Python Batch or PySpark engine according to Section 6.2."""
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    path = Path(file_path)
    if not path.is_file():
        raise FileNotFoundError(f"Input file not found: {path}")

    size_bytes = path.stat().st_size
    size_mb = size_bytes / (1024 * 1024)

    if size_mb <= SMALL_FILE_THRESHOLD_MB:
        engine = "python_batch"
        reason = (
            f"File size ({size_mb:.2f} MB) <= Config Threshold ({SMALL_FILE_THRESHOLD_MB} MB)"
        )
    else:
        engine = "pyspark"
        reason = (
            f"File size ({size_mb:.2f} MB) > Config Threshold ({SMALL_FILE_THRESHOLD_MB} MB)"
        )

    return {
        "run_id": uuid4().hex,
        "file_path": str(path.resolve()),
        "file_name": path.name,
        "file_size_bytes": size_bytes,
        "file_size_mb": round(size_mb, 2),
        "threshold_mb": SMALL_FILE_THRESHOLD_MB,
        "engine": engine,
        "reason": reason,
    }


if __name__ == "__main__":
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    target = sys.argv[1] if len(sys.argv) > 1 else SMALL_SAMPLE_FILE
    res = route_file(target)

    print("\033[96m" + "=" * 65 + "\033[0m")
    print("\033[1m\033[92mAUTOMATED ENGINE ROUTER DECISION (SECTION 6.2)\033[0m")
    print("\033[96m" + "=" * 65 + "\033[0m")
    print(f"\033[97mRun ID (Unique Execution) :\033[0m \033[96m{res['run_id']}\033[0m")
    print(f"\033[97mTarget File Name          :\033[0m \033[1m\033[92m{res['file_name']}\033[0m")
    print(f"\033[97mInspected File Size       :\033[0m \033[95m{res['file_size_mb']} MB ({res['file_size_bytes']:,} Bytes)\033[0m")
    print(f"\033[97mConfig Threshold Boundary :\033[0m \033[93m{res['threshold_mb']} MB\033[0m")
    print(f"\033[97mSelected Processing Engine:\033[0m \033[1m\033[92m{res['engine'].upper()}\033[0m")
    print(f"\033[97mRouter Decision Rationale :\033[0m \033[97m{res['reason']}\033[0m")
    print("\033[96m" + "=" * 65 + "\033[0m\n")
