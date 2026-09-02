from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

from bootstrap import ensure_project_root

ensure_project_root()

from config.settings import DATA_DIR, HUGE_FILE, INPUT_FILE, SMALL_SAMPLE_FILE, SMALL_SAMPLE_ROWS


def create_small_sample(input_file: str | Path = INPUT_FILE,
                        output_file: str | Path = SMALL_SAMPLE_FILE,
                        rows: int = SMALL_SAMPLE_ROWS) -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    input_path = Path(input_file)
    output_path = Path(output_file)

    # If input and output point to the same file (e.g. orders_small_sample.csv),
    # fallback to the main dataset orders_huge_mixed_quality.csv as source.
    if input_path.resolve() == output_path.resolve() or not input_path.is_file() or input_path.stat().st_size == 0:
        fallback = HUGE_FILE
        if fallback.is_file():
            input_path = fallback

    if not input_path.is_file():
        raise FileNotFoundError(f"Input file not found: {input_path}")
    if rows <= 0:
        raise ValueError("rows must be > 0")

    output_path.parent.mkdir(parents=True, exist_ok=True)

    with (
        input_path.open("r", encoding="utf-8-sig", newline="") as src,
        output_path.open("w", encoding="utf-8", newline="") as dst,
    ):
        reader = csv.reader(src)
        writer = csv.writer(dst)
        header = next(reader)
        writer.writerow(header)

        written = 0
        for row in reader:
            writer.writerow(row)
            written += 1
            if written >= rows:
                break

    output_size_bytes = output_path.stat().st_size
    output_size_mb = output_size_bytes / (1024 * 1024)

    print("\033[96m" + "=" * 65 + "\033[0m")
    print("\033[1m\033[92mSMALL CSV SAMPLE CREATION SUMMARY\033[0m")
    print("\033[96m" + "=" * 65 + "\033[0m")
    print(f"\033[97mCreated File Name   :\033[0m \033[1m\033[92m{output_path.name}\033[0m")
    print(f"\033[97mStorage Directory   :\033[0m \033[96m{output_path.parent.resolve()}\033[0m")
    print(f"\033[97mFull Saved Path     :\033[0m \033[93m{output_path.resolve()}\033[0m")
    print(f"\033[97mGenerated File Size :\033[0m \033[1m\033[95m{output_size_mb:.2f} MB ({output_size_bytes:,} Bytes)\033[0m")
    print(f"\033[97mSource File Used    :\033[0m \033[97m{input_path.name}\033[0m")
    print(f"\033[97mRows Extracted      :\033[0m \033[1m\033[92m{written:,} Rows\033[0m (Requested: {rows:,})")
    print("\033[96m" + "=" * 65 + "\033[0m\n")

    return written


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create a reproducible small CSV sample.")
    parser.add_argument("--input", default=str(INPUT_FILE), help="Source CSV file.")
    parser.add_argument("--output", default=str(SMALL_SAMPLE_FILE), help="Output sample CSV file.")
    parser.add_argument("--rows", type=int, default=SMALL_SAMPLE_ROWS, help="Number of data rows to copy.")
    args = parser.parse_args()
    create_small_sample(args.input, args.output, args.rows)
