from __future__ import annotations

import os
from pathlib import Path
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env", override=False)

# ---------------------------------------------------------------------------
# Core Project Directories & Data Paths
# ---------------------------------------------------------------------------
DATA_DIR = PROJECT_ROOT / "data"
REPORTS_DIR = PROJECT_ROOT / "reports"

INPUT_FILE = Path(os.getenv("PIPELINE_INPUT_FILE", str(DATA_DIR / "orders_huge_mixed_quality.csv")))
SMALL_SAMPLE_FILE = Path(os.getenv("PIPELINE_SAMPLE_FILE", str(DATA_DIR / "orders_small_sample.csv")))
HUGE_FILE = Path(os.getenv("PIPELINE_HUGE_FILE", str(DATA_DIR / "orders_huge_mixed_quality.csv")))
UPDATE_TEST_FILE = Path(os.getenv("PIPELINE_UPDATE_TEST_FILE", str(DATA_DIR / "orders_update_test.csv")))

# ---------------------------------------------------------------------------
# Engine Router & Sampling Boundaries
# ---------------------------------------------------------------------------
SMALL_SAMPLE_ROWS = int(os.getenv("PIPELINE_SAMPLE_ROWS", "100000"))
SMALL_FILE_THRESHOLD_MB = int(os.getenv("SMALL_FILE_THRESHOLD_MB", "200"))  # Brief Requirement: 200 MB

# ---------------------------------------------------------------------------
# MongoDB Database & Collection Names
# ---------------------------------------------------------------------------
MONGO_URI = os.getenv("MONGO_URI", "mongodb://127.0.0.1:27017")
MONGO_DATABASE = os.getenv("MONGO_DATABASE", "midterm_pipeline")
MONGO_TIMEOUT_MS = int(os.getenv("PIPELINE_MONGO_TIMEOUT_MS", "5000"))

RAW_COLLECTION = os.getenv("MONGO_RAW_COLLECTION", "orders_raw")
VALIDATED_COLLECTION = os.getenv("MONGO_VALIDATED_COLLECTION", "orders_validated")
QUARANTINE_COLLECTION = os.getenv("MONGO_QUARANTINE_COLLECTION", "orders_quarantine")

# ---------------------------------------------------------------------------
# Python Batch Loader Execution Settings
# ---------------------------------------------------------------------------
BATCH_SIZE = int(os.getenv("PIPELINE_BATCH_SIZE", "2000"))
MAX_MONGO_WRITE_BATCH_SIZE = int(os.getenv("PIPELINE_MONGO_WRITE_BATCH_SIZE", "1000"))

# ---------------------------------------------------------------------------
# PySpark Cluster & Execution Settings
# ---------------------------------------------------------------------------
SPARK_MASTER_URL = os.getenv("PIPELINE_SPARK_MASTER", "local[*]")
ALLOW_SPARK_LOCAL_FALLBACK = os.getenv("PIPELINE_ALLOW_SPARK_LOCAL_FALLBACK", "true").lower() in ("1", "true", "yes")
DISABLE_SPARK_FALLBACK = os.getenv("PIPELINE_DISABLE_SPARK_FALLBACK", "false").lower() in ("1", "true", "yes")

SPARK_APP_NAME = os.getenv("PIPELINE_SPARK_APP_NAME", "MidtermDataPipeline")
SPARK_LOG_LEVEL = os.getenv("PIPELINE_SPARK_LOG_LEVEL", "WARN")
SPARK_PARTITIONS = int(os.getenv("PIPELINE_SPARK_PARTITIONS", "16"))
SPARK_WRITE_BATCH_SIZE = int(os.getenv("PIPELINE_SPARK_WRITE_BATCH_SIZE", "512"))

# Hardware Resources for Hardware Acceleration & Spark Driver/Executors
SPARK_DRIVER_MEMORY = os.getenv("PIPELINE_SPARK_DRIVER_MEMORY", "6g")
SPARK_EXECUTOR_MEMORY = os.getenv("PIPELINE_SPARK_EXECUTOR_MEMORY", "6g")
SPARK_EXECUTOR_CORES = os.getenv("PIPELINE_SPARK_EXECUTOR_CORES", "8")
SPARK_FALLBACK_DRIVER_MEMORY = SPARK_DRIVER_MEMORY
SPARK_FALLBACK_EXECUTOR_MEMORY = SPARK_EXECUTOR_MEMORY

ENABLE_GPU_ACCELERATION = os.getenv("PIPELINE_ENABLE_GPU", "true").lower() in ("1", "true", "yes")
MONGO_SPARK_CONNECTOR = os.getenv("MONGO_SPARK_CONNECTOR", "org.mongodb.spark:mongo-spark-connector_2.13:11.1.0")

IVY_JARS_DIR = Path(os.path.expanduser(os.getenv("PIPELINE_IVY_JARS_DIR", "~/.ivy2.5.2/jars")))

# ---------------------------------------------------------------------------
# Pipeline Flow Control Settings
# ---------------------------------------------------------------------------
RUN_ELT_AFTER_RAW = os.getenv("PIPELINE_RUN_ELT_AFTER_RAW", "true").lower() in ("1", "true", "yes")
ALLOW_FULL_LOCAL_ELT = os.getenv("PIPELINE_ALLOW_FULL_LOCAL_ELT", "true").lower() in ("1", "true", "yes")
LOCAL_ELT_MAX_MB = int(os.getenv("PIPELINE_LOCAL_ELT_MAX_MB", "2048"))

# ---------------------------------------------------------------------------
# Dataset Raw Column Schema Order (17 Columns)
# ---------------------------------------------------------------------------
RAW_COLUMNS = [
    "order_id",
    "order_date",
    "status",
    "customer_id",
    "customer_name",
    "customer_phone",
    "customer_email",
    "city",
    "district",
    "delivery_type",
    "delivery_cost",
    "payment_method",
    "payment_status",
    "payment_amount",
    "currency",
    "total_amount",
    "items_json",
]


def ensure_directories() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    (REPORTS_DIR / "screenshots").mkdir(parents=True, exist_ok=True)
