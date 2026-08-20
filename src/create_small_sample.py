from __future__ import annotations

from common import PROJECT_ROOT  # noqa: F401

import csv
import argparse
from pathlib import Path

from config.settings import DATA_DIR, INPUT_FILE, SMALL_SAMPLE_FILE, SMALL_SAMPLE_ROWS


def create_small_sample(input_file: str | Path = INPUT_FILE,
                        output_file: str | Path = SMALL_SAMPLE_FILE,
                        rows: int = SMALL_SAMPLE_ROWS) -> int:
    input_path = Path(input_file)
    output_path = Path(output_file)

    # If input and output point to the same file (e.g. orders_small_sample.csv),
    # fallback to the main dataset orders_huge_mixed_quality.csv as source.
    if input_path.resolve() == output_path.resolve() or not input_path.is_file() or input_path.stat().st_size == 0:
        fallback = DATA_DIR / "orders_huge_mixed_quality.csv"
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

    print("=" * 60)
    print("SMALL SAMPLE CREATION")
    print("=" * 60)
    print(f"Source file : {input_path}")
    print(f"Output file : {output_path}")
    print(f"Rows        : {written}")
    print(f"Requested   : {rows}")
    print("=" * 60)
    return written


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create a reproducible small CSV sample.")
    parser.add_argument("--input", default=str(INPUT_FILE), help="Source CSV file.")
    parser.add_argument("--output", default=str(SMALL_SAMPLE_FILE), help="Output sample CSV file.")
    parser.add_argument("--rows", type=int, default=SMALL_SAMPLE_ROWS, help="Number of data rows to copy.")
    args = parser.parse_args()
    create_small_sample(args.input, args.output, args.rows)
