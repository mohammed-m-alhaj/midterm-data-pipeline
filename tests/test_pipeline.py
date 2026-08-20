"""Comprehensive pipeline integration tests.

Tests verify that all settings from config/settings.py propagate correctly
to the modules that use them.  Tests do NOT require a running MongoDB
instance — they mock external I/O and focus on configuration wiring.
"""
from __future__ import annotations

import csv
import os
import sys
import tempfile
from pathlib import Path
from unittest import mock

import pytest

# ------------------------------------------------------------------
# Ensure the project root is on sys.path so ``config`` resolves.
# ------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


# ==================================================================
# 1.  INPUT_FILE resolves to the correct path
# ==================================================================
def test_input_file_resolves_to_small_sample():
    """Default INPUT_FILE should point at orders_small_sample.csv."""
    from config.settings import INPUT_FILE, DATA_DIR

    assert INPUT_FILE == DATA_DIR / "orders_small_sample.csv"


def test_input_file_env_override(monkeypatch, tmp_path):
    """PIPELINE_INPUT_FILE env var overrides the default INPUT_FILE."""
    custom = tmp_path / "custom.csv"
    custom.write_text("header\n")
    monkeypatch.setenv("PIPELINE_INPUT_FILE", str(custom))

    # Re-import after env change
    import importlib
    import config.settings as _settings
    importlib.reload(_settings)

    assert _settings.INPUT_FILE == custom

    # Restore
    monkeypatch.delenv("PIPELINE_INPUT_FILE", raising=False)
    importlib.reload(_settings)


# ==================================================================
# 2.  Small file -> python_batch
# ==================================================================
def test_small_file_routes_to_python_batch(tmp_path):
    """A file smaller than the threshold should route to python_batch."""
    small_csv = tmp_path / "small.csv"
    small_csv.write_text("col1\nval1\n")

    from src.file_router import route_file
    result = route_file(str(small_csv))

    assert result["engine"] == "python_batch"
    assert result["file_size_mb"] <= result["threshold_mb"]


# ==================================================================
# 3.  Large file -> pyspark
# ==================================================================
def test_large_file_routes_to_pyspark(tmp_path):
    """A file larger than the threshold should route to pyspark."""
    big_csv = tmp_path / "big.csv"
    # Write ~201 MB of data (above the 200 MB threshold)
    with big_csv.open("w") as f:
        f.write("col1\n")
        # Each line is ~1024 bytes -> 201*1024 lines ≈ 201 MB
        line = "x" * 1023 + "\n"
        for _ in range(201 * 1024):
            f.write(line)

    from src.file_router import route_file
    result = route_file(str(big_csv))

    assert result["engine"] == "pyspark"
    assert result["file_size_mb"] > result["threshold_mb"]


# ==================================================================
# 4.  Missing file -> clear error
# ==================================================================
def test_missing_file_raises_with_path():
    """route_file on a non-existent path should raise FileNotFoundError."""
    from src.file_router import route_file

    with pytest.raises(FileNotFoundError, match="not_exists.csv"):
        route_file("/tmp/not_exists.csv")


# ==================================================================
# 5.  SMALL_SAMPLE_ROWS affects create_small_sample
# ==================================================================
def test_small_sample_rows_respected(tmp_path):
    """create_small_sample should copy exactly SMALL_SAMPLE_ROWS rows."""
    # Create a source CSV with 500 data rows.
    source = tmp_path / "source.csv"
    with source.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "value"])
        for i in range(500):
            writer.writerow([i, f"val_{i}"])

    output = tmp_path / "sample.csv"

    from src.create_small_sample import create_small_sample

    # Request 100 rows
    written = create_small_sample(
        input_file=source,
        output_file=output,
        rows=100,
    )
    assert written == 100

    # Count lines in output (header + 100 data rows)
    lines = output.read_text().strip().splitlines()
    assert len(lines) == 101  # 1 header + 100 data

    # Request 200 rows
    written2 = create_small_sample(
        input_file=source,
        output_file=output,
        rows=200,
    )
    assert written2 == 200


# ==================================================================
# 6.  BATCH_SIZE affects Python Batch
# ==================================================================
def test_batch_size_is_configurable():
    """BATCH_SIZE should be importable and match settings."""
    from config.settings import BATCH_SIZE

    assert isinstance(BATCH_SIZE, int)
    assert BATCH_SIZE > 0


def test_batch_size_env_override(monkeypatch):
    """PIPELINE_BATCH_SIZE env var should change BATCH_SIZE."""
    monkeypatch.setenv("PIPELINE_BATCH_SIZE", "500")

    import importlib
    import config.settings as _settings
    importlib.reload(_settings)

    assert _settings.BATCH_SIZE == 500

    monkeypatch.delenv("PIPELINE_BATCH_SIZE", raising=False)
    importlib.reload(_settings)


# ==================================================================
# 7.  SPARK_PARTITIONS affects spark_loader
# ==================================================================
def test_spark_partitions_is_configurable():
    """SPARK_PARTITIONS should be importable and match settings."""
    from config.settings import SPARK_PARTITIONS

    assert isinstance(SPARK_PARTITIONS, int)
    assert SPARK_PARTITIONS > 0


def test_spark_partitions_env_override(monkeypatch):
    """PIPELINE_SPARK_PARTITIONS env var should change SPARK_PARTITIONS."""
    monkeypatch.setenv("PIPELINE_SPARK_PARTITIONS", "16")

    import importlib
    import config.settings as _settings
    importlib.reload(_settings)

    assert _settings.SPARK_PARTITIONS == 16

    monkeypatch.delenv("PIPELINE_SPARK_PARTITIONS", raising=False)
    importlib.reload(_settings)


def test_spark_loader_imports_partitions():
    """spark_loader module should reference SPARK_PARTITIONS from settings."""
    import src.spark_loader as sl

    # The module should have access to SPARK_PARTITIONS
    assert hasattr(sl, "SPARK_PARTITIONS")


# ==================================================================
# 8-9.  Classification contract tests (quality_rules)
# ==================================================================
def test_classify_errors_returns_valid_for_empty():
    from src.quality_rules import classify_errors
    assert classify_errors([]) == "valid"


def test_classify_errors_returns_quarantine_for_errors():
    from src.quality_rules import classify_errors
    assert classify_errors(["MISSING_ORDER_ID"]) == "quarantine"
    assert classify_errors(["X", "Y"]) == "quarantine"


# ==================================================================
# 10.  Route returns all required keys
# ==================================================================
def test_route_file_returns_all_required_keys(tmp_path):
    """route_file should return all keys needed by main.py."""
    csv_file = tmp_path / "test.csv"
    csv_file.write_text("col1\nval1\n")

    from src.file_router import route_file
    result = route_file(str(csv_file))

    required_keys = {
        "file_path",
        "file_name",
        "file_size_mb",
        "threshold_mb",
        "engine",
        "reason",
        "run_id",
    }
    assert required_keys.issubset(result.keys())


# ==================================================================
# 11.  Threshold sourced from settings
# ==================================================================
def test_threshold_from_settings(tmp_path):
    """The threshold in the route result should match settings."""
    csv_file = tmp_path / "test.csv"
    csv_file.write_text("col1\nval1\n")

    from config.settings import SMALL_FILE_THRESHOLD_MB
    from src.file_router import route_file

    result = route_file(str(csv_file))
    assert result["threshold_mb"] == SMALL_FILE_THRESHOLD_MB


# ==================================================================
# 12.  Settings constants are self-consistent
# ==================================================================
def test_settings_type_consistency():
    """All numeric settings should be the correct type."""
    from config.settings import (
        BATCH_SIZE,
        LOCAL_ELT_MAX_MB,
        SMALL_FILE_THRESHOLD_MB,
        SMALL_SAMPLE_ROWS,
        SPARK_PARTITIONS,
    )

    assert isinstance(BATCH_SIZE, int)
    assert isinstance(LOCAL_ELT_MAX_MB, int)
    assert isinstance(SMALL_FILE_THRESHOLD_MB, int)
    assert isinstance(SMALL_SAMPLE_ROWS, int)
    assert isinstance(SPARK_PARTITIONS, int)


# ==================================================================
# 13.  Ensure __future__ and Path(__file__) are correct
# ==================================================================
def test_settings_module_has_correct_syntax():
    """Verify that settings.py uses 'from __future__' not 'from **future**'."""
    from config.settings import PROJECT_ROOT
    settings_path = PROJECT_ROOT / "config" / "settings.py"
    content = settings_path.read_text(encoding="utf-8")

    assert "from __future__ import annotations" in content
    assert "from **future**" not in content
    assert "Path(**file**)" not in content


# ==================================================================
# 14.  batch_loader must use streaming csv.DictReader, not list(reader)
# ==================================================================
def test_batch_loader_does_not_load_full_file_into_memory():
    """
    Section 6.3 requires streaming CSV reading (no list(reader) / no full load).
    Verify that batch_loader source code does NOT use the forbidden patterns.
    """
    from config.settings import PROJECT_ROOT
    batch_loader_path = PROJECT_ROOT / "src" / "batch_loader.py"
    content = batch_loader_path.read_text(encoding="utf-8")

    # Must NOT convert reader to a list (forbidden by brief)
    assert "list(reader)" not in content, "batch_loader must not use list(reader)"
    assert "pd.read_csv" not in content, "batch_loader must not use pandas read_csv"
    # Must use streaming DictReader
    assert "csv.DictReader" in content or "DictReader" in content


# ==================================================================
# 15.  orders_raw required metadata fields are present in batch_loader
# ==================================================================
def test_raw_document_contains_required_metadata_fields():
    """
    Section 6.5: each raw document must contain run_id, source_file,
    source_row_number, ingested_at, engine_used, raw_record.
    """
    from config.settings import PROJECT_ROOT
    batch_loader_path = PROJECT_ROOT / "src" / "batch_loader.py"
    content = batch_loader_path.read_text(encoding="utf-8")

    required_fields = [
        "run_id",
        "source_file",
        "source_row_number",
        "ingested_at",
        "engine_used",
        "raw_record",
    ]
    for field in required_fields:
        assert f'"{field}"' in content, (
            f"batch_loader must include required raw field: {field}"
        )


# ==================================================================
# 16.  Quarantine error codes — all 9 required codes defined
# ==================================================================
def test_all_required_quarantine_error_codes_defined():
    """
    Section 6.8: all 9 mandatory quarantine error codes must be defined
    in quality_rules.ERROR_CODES.
    """
    from src.quality_rules import ERROR_CODES

    required = {
        "MISSING_ORDER_ID",
        "MISSING_CUSTOMER_ID",
        "INVALID_IMPOSSIBLE_DATE",
        "CORRUPTED_ITEMS_JSON",
        "EMPTY_ITEMS",
        "UNKNOWN_PRICE",
        "AMBIGUOUS_NEGATIVE_VALUE",
        "DUPLICATE_ORDER_ID",
        "MULTIPLE_CONFLICTING_ERRORS",
    }
    missing = required - ERROR_CODES
    assert not missing, f"Missing required quarantine error codes: {missing}"


# ==================================================================
# 17.  metrics output has all required keys (Section 6.12)
# ==================================================================
def test_metrics_output_contains_required_keys():
    """
    Section 6.12: results.json must contain all required measurement keys.
    """
    import json
    from config.settings import REPORTS_DIR

    results_file = REPORTS_DIR / "results.json"
    if not results_file.exists():
        import pytest
        pytest.skip("results.json not yet generated — run the pipeline first")

    data = json.loads(results_file.read_text(encoding="utf-8"))
    assert isinstance(data, list) and len(data) > 0, "results.json must be a non-empty list"

    required_keys = {
        "run_id", "file_name", "file_size_mb", "engine_used",
        "rows_read", "raw_loaded", "elapsed_seconds", "throughput",
    }
    latest = data[-1]
    missing = required_keys - set(latest.keys())
    assert not missing, f"results.json missing required metric keys: {missing}"


# ==================================================================
# 18.  Idempotency: results.json must contain inserted/updated/unchanged
# ==================================================================
def test_metrics_output_contains_idempotency_keys():
    """
    Section 6.12 / 6.10: results.json must track inserted_count,
    updated_count, unchanged_count to prove Idempotency.
    """
    import json
    from config.settings import REPORTS_DIR

    results_file = REPORTS_DIR / "results.json"
    if not results_file.exists():
        import pytest
        pytest.skip("results.json not yet generated — run the pipeline first")

    data = json.loads(results_file.read_text(encoding="utf-8"))
    # Find any ELT run that has these keys
    elt_runs = [r for r in data if "inserted_count" in r or "updated_count" in r]
    assert elt_runs, (
        "No ELT run found in results.json with idempotency metrics "
        "(inserted_count / updated_count / unchanged_count)"
    )
