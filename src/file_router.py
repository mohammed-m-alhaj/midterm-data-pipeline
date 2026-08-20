from __future__ import annotations

from common import PROJECT_ROOT  # noqa: F401

from pathlib import Path
from uuid import uuid4

from config.settings import SMALL_FILE_THRESHOLD_MB



def route_file(file_path: str | Path) -> dict:
    """Inspect the file and select Python Batch or PySpark."""
    path = Path(file_path)
    if not path.is_file():
        raise FileNotFoundError(f"Input file not found: {path}")

    size_bytes = path.stat().st_size
    size_mb = size_bytes / (1024 * 1024)

    if size_mb <= SMALL_FILE_THRESHOLD_MB:
        engine = "python_batch"
        reason = (
            f"file size ({size_mb:.2f} MB) <= "
            f"threshold ({SMALL_FILE_THRESHOLD_MB} MB)"
        )
    else:
        engine = "pyspark"
        reason = (
            f"file size ({size_mb:.2f} MB) > "
            f"threshold ({SMALL_FILE_THRESHOLD_MB} MB)"
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
    import sys
    from config.settings import SMALL_SAMPLE_FILE

    target = sys.argv[1] if len(sys.argv) > 1 else SMALL_SAMPLE_FILE
    res = route_file(target)
    print("=" * 60)
    print("STAGE 1 & 2: FILE DISCOVERY & ENGINE ROUTER")
    print("=" * 60)
    for k, v in res.items():
        print(f"{k:20}: {v}")
    print("=" * 60)

