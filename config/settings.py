from __future__ import annotations

import os
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
REPORTS_DIR = PROJECT_ROOT / "reports"

# ---------------------------------------------------------------------------
# Input file — the single source of truth for which CSV the pipeline uses.
# Override via the PIPELINE_INPUT_FILE environment variable if needed.
# ---------------------------------------------------------------------------
INPUT_FILE = Path(
    os.getenv(
        "PIPELINE_INPUT_FILE",
        str(DATA_DIR / "orders_small_sample.csv"),
    )
)

SMALL_SAMPLE_FILE = Path(
    os.getenv(
        "PIPELINE_SAMPLE_FILE",
        str(DATA_DIR / "orders_small_sample.csv"),
    )
)

# ---------------------------------------------------------------------------
# SMALL_SAMPLE_ROWS — number of data rows that create_small_sample.py copies
# from the original huge CSV into the reproducible small sample file.
# This is NOT batch_size and NOT partitions — it is purely the row count for
# the sample extraction script.
# ---------------------------------------------------------------------------
SMALL_SAMPLE_ROWS = int(os.getenv("PIPELINE_SAMPLE_ROWS", "500000"))

# ---------------------------------------------------------------------------
# SMALL_FILE_THRESHOLD_MB — the file-size boundary (in megabytes) that the
# Router uses to decide the processing engine:
#   • file_size <= threshold  → python_batch
#   • file_size >  threshold  → pyspark
# Required by the brief to be 200 MB.
# ---------------------------------------------------------------------------
SMALL_FILE_THRESHOLD_MB = 200

# ---------------------------------------------------------------------------
# BATCH_SIZE — number of records per insert_many() call in Python Batch mode.
# Controls memory usage during the streaming CSV → MongoDB raw load.
# This is NOT the sample row count and NOT Spark partitions.
# ---------------------------------------------------------------------------
BATCH_SIZE = int(os.getenv("PIPELINE_BATCH_SIZE", "1000"))

# ---------------------------------------------------------------------------
# SPARK_PARTITIONS — number of DataFrame partitions used by PySpark after
# reading the CSV. Controls parallelism during the Spark → MongoDB write.
# This is NOT batch_size (Python) and NOT sample rows — it is Spark-specific.
# ---------------------------------------------------------------------------
SPARK_PARTITIONS = int(os.getenv("PIPELINE_SPARK_PARTITIONS", "8"))

# ---------------------------------------------------------------------------
# MongoDB connection.
# ---------------------------------------------------------------------------
MONGO_URI = os.getenv("MONGO_URI", "mongodb://127.0.0.1:27017")
MONGO_DATABASE = os.getenv("MONGO_DATABASE", "midterm_pipeline")
RAW_COLLECTION = "orders_raw"
VALIDATED_COLLECTION = "orders_validated"
QUARANTINE_COLLECTION = "orders_quarantine"

# ---------------------------------------------------------------------------
# Spark / MongoDB Spark Connector.
# ---------------------------------------------------------------------------
SPARK_MASTER_URL = os.getenv("PIPELINE_SPARK_MASTER", "local[*]")
ALLOW_SPARK_LOCAL_FALLBACK = os.getenv("PIPELINE_ALLOW_SPARK_LOCAL_FALLBACK", "false").lower() == "true"

MONGO_SPARK_CONNECTOR = "org.mongodb.spark:mongo-spark-connector_2.13:11.1.0"
SPARK_APP_NAME = os.getenv("PIPELINE_SPARK_APP_NAME", "MidtermDataPipeline")
SPARK_LOG_LEVEL = os.getenv("PIPELINE_SPARK_LOG_LEVEL", "WARN")
ENABLE_GPU_ACCELERATION = os.getenv("PIPELINE_ENABLE_GPU", "false").lower() == "true"

# ---------------------------------------------------------------------------
# DISABLE_SPARK_FALLBACK — when True, the pipeline will NOT silently fall
# back from spark:// to local[*] if the cluster is unreachable.  Set to
# True during Path A demo to guarantee real cluster execution.
# ---------------------------------------------------------------------------
DISABLE_SPARK_FALLBACK = os.getenv("PIPELINE_DISABLE_SPARK_FALLBACK", "false").lower() == "true"

# ---------------------------------------------------------------------------
# Pipeline behavior.
# ---------------------------------------------------------------------------
#RUN_ELT_AFTER_RAW = os.getenv("PIPELINE_RUN_ELT_AFTER_RAW", "true").lower() == "true"
RUN_ELT_AFTER_RAW = True


# Prevent an accidental full 12+ GB ELT run on one local laptop.
# Path A cluster runs or spark:// masters bypass this guard automatically.
#ALLOW_FULL_LOCAL_ELT = os.getenv("PIPELINE_ALLOW_FULL_LOCAL_ELT", "false").lower() == "true"
ALLOW_FULL_LOCAL_ELT = True

LOCAL_ELT_MAX_MB = int(os.getenv("PIPELINE_LOCAL_ELT_MAX_MB", "2048"))

# Mongo/Spark raw-load behavior.
MAX_MONGO_WRITE_BATCH_SIZE = int(os.getenv("PIPELINE_MONGO_WRITE_BATCH_SIZE", "1000"))

# ---------------------------------------------------------------------------
# Source column order from the supplied dataset.
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
