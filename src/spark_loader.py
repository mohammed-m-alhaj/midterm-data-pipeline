from __future__ import annotations

import logging
import os
import sys
import time
from pathlib import Path
from uuid import uuid4

from bootstrap import PROJECT_ROOT, ensure_project_root

ensure_project_root()

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from config.settings import (
    ALLOW_SPARK_LOCAL_FALLBACK,
    DATA_DIR,
    DISABLE_SPARK_FALLBACK,
    ENABLE_GPU_ACCELERATION,
    HUGE_FILE,
    INPUT_FILE,
    IVY_JARS_DIR,
    MONGO_DATABASE,
    MONGO_SPARK_CONNECTOR,
    MONGO_TIMEOUT_MS,
    MONGO_URI,
    RAW_COLLECTION,
    RAW_COLUMNS,
    SPARK_APP_NAME,
    SPARK_DRIVER_MEMORY,
    SPARK_EXECUTOR_CORES,
    SPARK_EXECUTOR_MEMORY,
    SPARK_FALLBACK_DRIVER_MEMORY,
    SPARK_FALLBACK_EXECUTOR_MEMORY,
    SPARK_LOG_LEVEL,
    SPARK_MASTER_URL,
    SPARK_PARTITIONS,
    SPARK_WRITE_BATCH_SIZE,
)
from src.common import get_gpu_info

logger = logging.getLogger(__name__)


def build_raw_schema() -> T.StructType:
    return T.StructType([T.StructField(name, T.StringType(), True) for name in RAW_COLUMNS])


import atexit
import ctypes


def suppress_exit_stderr() -> None:
    try:
        devnull = open(os.devnull, "w")
        os.dup2(devnull.fileno(), 2)
        if sys.platform.startswith("win"):
            kernel32 = ctypes.windll.kernel32
            handle = kernel32.CreateFileW("NUL", 0x40000000, 0, None, 3, 0, None)
            if handle != -1:
                kernel32.SetStdHandle(-12, handle)
    except Exception:
        pass


atexit.register(suppress_exit_stderr)


def stop_spark_quietly(spark: SparkSession | None) -> None:
    if spark is None:
        return
    try:
        devnull = open(os.devnull, "w")
        old_stderr = os.dup(2)
        os.dup2(devnull.fileno(), 2)
        try:
            spark.stop()
        finally:
            os.dup2(old_stderr, 2)
            os.close(old_stderr)
            devnull.close()
    except Exception:
        try:
            spark.stop()
        except Exception:
            pass


def create_spark() -> SparkSession:

    builder = (
        SparkSession.builder
        .appName(SPARK_APP_NAME)
        .master(SPARK_MASTER_URL)
        .config("spark.driver.memory", SPARK_DRIVER_MEMORY)
        .config("spark.executor.memory", SPARK_EXECUTOR_MEMORY)
        .config("spark.executor.cores", SPARK_EXECUTOR_CORES)
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.shutdown.hook.enabled", "false")
        .config("spark.driver.extraJavaOptions", "-Dlog4j2.shutdownHookEnabled=false -Dorg.apache.spark.suppressShutdownLogging=true")
        .config("spark.executor.extraJavaOptions", "-Dlog4j2.shutdownHookEnabled=false -Dorg.apache.spark.suppressShutdownLogging=true")
        .config("spark.local.dir", str(Path(os.environ.get("TEMP", str(PROJECT_ROOT / ".spark_temp"))) / "midterm_spark_temp"))
    )

    ivy_jars = list(IVY_JARS_DIR.glob("*.jar")) if IVY_JARS_DIR.is_dir() else []
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
        spark.sparkContext.setLogLevel("ERROR")
        try:
            log4j = spark._jvm.org.apache.log4j
            log4j.Logger.getRootLogger().setLevel(log4j.Level.ERROR)
            log4j.Logger.getLogger("org").setLevel(log4j.Level.ERROR)
            log4j.Logger.getLogger("py4j").setLevel(log4j.Level.ERROR)
        except Exception:
            pass
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
                    .appName(SPARK_APP_NAME)
                    .master("local[*]")
                    .config("spark.driver.memory", SPARK_FALLBACK_DRIVER_MEMORY)
                    .config("spark.executor.memory", SPARK_FALLBACK_EXECUTOR_MEMORY)
                    .config("spark.mongodb.write.connection.uri", MONGO_URI)
                    .config("spark.mongodb.write.database", MONGO_DATABASE)
                    .config("spark.mongodb.write.collection", RAW_COLLECTION)
                    .config("spark.sql.ansi.enabled", "false")
                    .config("spark.sql.adaptive.enabled", "true")
                    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                    .config("spark.local.dir", str(Path(os.environ.get("TEMP", str(PROJECT_ROOT / ".spark_temp"))) / "midterm_spark_temp"))
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


def load_csv_to_raw(file_path: str | Path, run_id: str, engine_used: str = "pyspark", partitions: int | None = None, close_spark: bool = False) -> dict:
    target_path_str = resolve_safe_spark_path(file_path)
    input_path = Path(file_path)

    actual_partitions = int(partitions) if partitions and int(partitions) > 0 else SPARK_PARTITIONS
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
        repartitioned = df.repartition(actual_partitions)
        output_partitions = repartitioned.rdd.getNumPartitions()

        # --- Repartition Evidence (explain) -----------------------------------
        # Print the physical plan to prove repartition is applied.
        # The output will show Exchange/RepartitionByExpression confirming
        # that Spark actually redistributes data across SPARK_PARTITIONS.
        # Capture this output in a screenshot for the demo.
        logger.info(
            "=== Physical Plan after repartition(%d) ===", SPARK_PARTITIONS
        )
        repartitioned.explain(True)
        # ----------------------------------------------------------------------

        raw_record = F.to_json(F.struct(*[F.col(c) for c in RAW_COLUMNS]))
        enriched = (
            repartitioned
            .withColumn("run_id", F.lit(run_id))
            .withColumn("source_file", F.lit(str(input_path.resolve())))
            .withColumn("source_row_number", F.monotonically_increasing_id() + F.lit(2))
            .withColumn("ingested_at", F.current_timestamp())
            .withColumn("engine_used", F.lit(engine_used))
            .withColumn("raw_record", raw_record)
            .select(
                "run_id",
                "source_file",
                "source_row_number",
                "ingested_at",
                "engine_used",
                "raw_record",
            )
        )

        (
            enriched.write
            .format("mongodb")
            .mode("append")
            .option("connection.uri", MONGO_URI)
            .option("database", MONGO_DATABASE)
            .option("collection", RAW_COLLECTION)
            .option("convertjson", "false")
            .option("maxbatchsize", str(SPARK_WRITE_BATCH_SIZE))
            .save()
        )

        elapsed = time.perf_counter() - started
        rows_read = enriched.count()
        application_id = spark.sparkContext.applicationId or "N/A"
        
        gpu_info = get_gpu_info()

        print("\033[96m" + "=" * 60 + "\033[0m")
        print("\033[1m\033[92mPYSPARK RAW LOAD & HARDWARE MONITORING\033[0m")
        print("\033[96m" + "=" * 60 + "\033[0m")
        print(f"\033[97mHost Hardware Info     :\033[0m \033[95m{gpu_info}\033[0m")
        print(f"\033[97mRows read              :\033[0m \033[96m{rows_read:,}\033[0m")
        print(f"\033[97mInput partitions       :\033[0m \033[93m{input_partitions}\033[0m")
        print(f"\033[97mRequested partitions   :\033[0m \033[96m{SPARK_PARTITIONS}\033[0m")
        print(f"\033[97mOutput partitions      :\033[0m \033[92m{output_partitions}\033[0m")
        print(f"\033[97mElapsed seconds        :\033[0m \033[93m{elapsed:.2f}s\033[0m")
        print(f"\033[97mThroughput             :\033[0m \033[1m\033[92m{rows_read / elapsed if elapsed else 0:.2f} rows/s\033[0m")
        print(f"\033[97mMaster                 :\033[0m \033[96m{SPARK_MASTER_URL}\033[0m")
        print(f"\033[97mActual Spark master    :\033[0m \033[96m{spark.sparkContext.master}\033[0m")
        print(f"\033[97mApplication ID         :\033[0m \033[95m{application_id}\033[0m")
        print("\033[96m" + "=" * 60 + "\033[0m\n")

        return {
            "rows_read": rows_read,
            "raw_loaded": rows_read,
            "input_partitions": input_partitions,
            "output_partitions": output_partitions,
            "partitions": output_partitions,
            "spark_partitions_requested": SPARK_PARTITIONS,
            "spark_master": SPARK_MASTER_URL,
            "actual_spark_master": spark.sparkContext.master,
            "application_id": application_id,
            "elapsed_seconds": elapsed,
            "throughput": rows_read / elapsed if elapsed else 0.0,
        }
    finally:
        if close_spark and spark is not None:
            stop_spark_quietly(spark)


if __name__ == "__main__":
    huge_file = HUGE_FILE
    if len(sys.argv) > 1:
        target = sys.argv[1]
    elif huge_file.exists():
        target = huge_file
    else:
        target = INPUT_FILE

    run_id = uuid4().hex
    print(f"Executing PySpark Loader on target file: {target}")
    load_csv_to_raw(target, run_id, close_spark=True)
