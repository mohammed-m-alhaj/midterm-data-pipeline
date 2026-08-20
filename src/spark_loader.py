from __future__ import annotations

from common import PROJECT_ROOT  # noqa: F401

import logging
import os
import time
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import current_timestamp, lit, monotonically_increasing_id
from pyspark.sql.types import StringType, StructField, StructType

from config.settings import (
    ALLOW_SPARK_LOCAL_FALLBACK,
    DATA_DIR,
    DISABLE_SPARK_FALLBACK,
    MONGO_DATABASE,
    MONGO_SPARK_CONNECTOR,
    MONGO_URI,
    RAW_COLLECTION,
    RAW_COLUMNS,
    SPARK_LOG_LEVEL,
    SPARK_MASTER_URL,
    SPARK_PARTITIONS,
)

logger = logging.getLogger(__name__)


def build_raw_schema() -> StructType:
    return StructType([StructField(name, StringType(), True) for name in RAW_COLUMNS])


def create_spark() -> SparkSession:
    from config.settings import ENABLE_GPU_ACCELERATION

    builder = (
        SparkSession.builder
        .appName("MidtermPipeline-SparkRawLoad")
        .master(SPARK_MASTER_URL)
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g")
        .config("spark.executor.cores", "4")
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.local.dir", str(PROJECT_ROOT / ".spark_temp"))
    )

    ivy_jars = list(Path(os.path.expanduser("~/.ivy2.5.2/jars")).glob("*.jar"))
    has_rapids = any("rapids" in j.name.lower() for j in ivy_jars)

    if ENABLE_GPU_ACCELERATION and has_rapids:
        builder = (
            builder
            .config("spark.plugins", "com.nvidia.spark.SQLPlugin")
            .config("spark.rapids.sql.enabled", "true")
        )

    if ivy_jars:
        builder = builder.config("spark.jars", ",".join(str(j) for j in ivy_jars))
    else:
        builder = builder.config("spark.jars.packages", MONGO_SPARK_CONNECTOR)

    try:
        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel(SPARK_LOG_LEVEL)
        return spark
    except Exception as exc:
        if SPARK_MASTER_URL.startswith("spark://"):
            if DISABLE_SPARK_FALLBACK:
                print(
                    f"[Spark Cluster FAIL] Standalone Master ({SPARK_MASTER_URL}) "
                    f"is unreachable and DISABLE_SPARK_FALLBACK=true. "
                    f"Will NOT fall back to local[*]."
                )
                raise
            if ALLOW_SPARK_LOCAL_FALLBACK:
                print(f"[Spark Cluster Notice] Standalone Master ({SPARK_MASTER_URL}) offline/unreachable. Falling back to local[*] mode...")
                try:
                    from pyspark import SparkContext
                    if getattr(SparkContext, "_active_spark_context", None) is not None:
                        SparkContext._active_spark_context.stop()
                    SparkContext._active_spark_context = None
                except Exception:
                    pass
                builder = (
                    SparkSession.builder
                    .appName("MidtermPipeline-SparkRawLoad")
                    .master("local[*]")
                    .config("spark.driver.memory", "4g")
                    .config("spark.executor.memory", "4g")
                    .config("spark.mongodb.write.connection.uri", MONGO_URI)
                    .config("spark.mongodb.write.database", MONGO_DATABASE)
                    .config("spark.mongodb.write.collection", RAW_COLLECTION)
                    .config("spark.sql.ansi.enabled", "false")
                    .config("spark.sql.adaptive.enabled", "true")
                    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                    .config("spark.local.dir", str(PROJECT_ROOT / ".spark_temp"))
                )
                spark = builder.getOrCreate()
                spark.sparkContext.setLogLevel(SPARK_LOG_LEVEL)
                return spark
            raise RuntimeError(
                f"Spark standalone master is unreachable: {SPARK_MASTER_URL}. "
                "Set PIPELINE_ALLOW_SPARK_LOCAL_FALLBACK=true for local dev "
                "or PIPELINE_DISABLE_SPARK_FALLBACK=true to fail loudly."
            ) from exc
        raise




def resolve_safe_spark_path(file_path: str | Path) -> str:
    p = Path(file_path)
    local_p = DATA_DIR / p.name
    if local_p.is_file():
        return str(local_p.resolve())
    p_str = str(file_path).replace("\\", "/")
    if p_str.startswith("//"):
        return f"file:{p_str}"
    return str(p.resolve())


def load_csv_to_raw(file_path: str | Path, run_id: str, engine_used: str = "pyspark") -> dict:
    target_path_str = resolve_safe_spark_path(file_path)
    input_path = Path(file_path)

    spark: SparkSession | None = None
    started = time.perf_counter()
    try:
        spark = create_spark()
        df = (
            spark.read
            .option("header", "true")
            .option("multiLine", "true")
            .option("quote", '"')
            .option("escape", '"')
            .schema(build_raw_schema())
            .csv(target_path_str)
        )

        input_partitions = df.rdd.getNumPartitions()
        # repartition justification (Section 6.4 of the brief):
        # The Spark Connector reads the CSV as 1 partition by default on local[*].
        # We repartition to SPARK_PARTITIONS (from config/settings.py) so that
        # MongoDB Spark Connector write tasks are distributed across all available
        # Executor cores/workers on the cluster — without this, only 1 task runs.
        # Effect is visible in Spark UI: Tasks = SPARK_PARTITIONS on the Write stage.
        repartitioned = df.repartition(SPARK_PARTITIONS)
        output_partitions = repartitioned.rdd.getNumPartitions()

        raw_record = F.to_json(F.struct(*[F.col(c) for c in RAW_COLUMNS]))
        enriched = (
            repartitioned
            .withColumn("run_id", lit(run_id))
            .withColumn("source_file", lit(str(input_path.resolve())))
            .withColumn("source_row_number", monotonically_increasing_id() + lit(2))
            .withColumn("ingested_at", current_timestamp())
            .withColumn("engine_used", lit(engine_used))
            .withColumn("raw_record", raw_record)
            .withColumn("record_raw", raw_record)
            .select(
                "run_id",
                "source_file",
                "source_row_number",
                "ingested_at",
                "engine_used",
                "raw_record",
                "record_raw",
            )
        )

        (
            enriched.write
            .format("mongodb")
            .mode("append")
            .option("connection.uri", MONGO_URI)
            .option("database", MONGO_DATABASE)
            .option("collection", RAW_COLLECTION)
            .option("convertJson", "false")
            .option("maxBatchSize", "512")
            .save()
        )

        elapsed = time.perf_counter() - started
        rows_read = enriched.count()
        application_id = spark.sparkContext.applicationId or "N/A"
        
        gpu_info = "N/A"
        try:
            import subprocess
            gpu_out = subprocess.check_output("nvidia-smi --query-gpu=name,memory.total --format=csv,noheader", shell=True, stderr=subprocess.DEVNULL).decode().strip()
            if gpu_out:
                gpu_info = f"{gpu_out} [ACTIVE]"
        except Exception:
            pass

        print("=" * 60)
        print("PYSPARK RAW LOAD & HARDWARE MONITORING")
        print("=" * 60)
        print(f"Host Hardware Info     : {gpu_info}")
        print(f"Rows read              : {rows_read:,}")
        print(f"Input partitions       : {input_partitions}")
        print(f"Requested partitions   : {SPARK_PARTITIONS}")
        print(f"Output partitions      : {output_partitions}")
        print(f"Elapsed seconds        : {elapsed:.2f}")
        print(f"Throughput             : {rows_read / elapsed if elapsed else 0:.2f} rows/s")
        print(f"Master                 : {SPARK_MASTER_URL}")
        print(f"Actual Spark master    : {spark.sparkContext.master}")
        print(f"Application ID         : {application_id}")
        print("=" * 60)

        return {
            "rows_read": rows_read,
            "raw_loaded": rows_read,
            "input_partitions": input_partitions,
            "output_partitions": output_partitions,
            "spark_partitions_requested": SPARK_PARTITIONS,
            "spark_master": SPARK_MASTER_URL,
            "actual_spark_master": spark.sparkContext.master,
            "application_id": application_id,
            "elapsed_seconds": elapsed,
            "throughput": rows_read / elapsed if elapsed else 0.0,
        }
    finally:
        if spark is not None:
            try:
                spark.stop()
            except Exception as exc:
                logger.warning("Spark shutdown warning: %s", exc)


if __name__ == "__main__":
    import sys
    from uuid import uuid4
    from config.settings import DATA_DIR, INPUT_FILE

    huge_file = DATA_DIR / "orders_huge_mixed_quality.csv"
    if len(sys.argv) > 1:
        target = sys.argv[1]
    elif huge_file.exists():
        target = huge_file
    else:
        target = INPUT_FILE

    run_id = uuid4().hex
    print(f"Executing PySpark Loader on target file: {target}")
    load_csv_to_raw(target, run_id)
