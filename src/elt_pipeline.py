from __future__ import annotations

from common import PROJECT_ROOT  # noqa: F401

import json
import time
from pathlib import Path

from pymongo import MongoClient
from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.functions import array, col, lit, size

from config.settings import (
    ALLOW_SPARK_LOCAL_FALLBACK,
    ALLOW_FULL_LOCAL_ELT,
    DISABLE_SPARK_FALLBACK,
    INPUT_FILE,
    LOCAL_ELT_MAX_MB,
    MONGO_DATABASE,
    MONGO_SPARK_CONNECTOR,
    MONGO_URI,
    QUARANTINE_COLLECTION,
    RAW_COLLECTION,
    RAW_COLUMNS,
    SPARK_APP_NAME,
    SPARK_LOG_LEVEL,
    SPARK_MASTER_URL,
    VALIDATED_COLLECTION,
)
from src.metrics import append_run_metrics


# RAW_COLUMNS is imported from config.settings — single source of truth.

RAW_SCHEMA = T.StructType([
    T.StructField("order_id", T.StringType(), True),
    T.StructField("order_date", T.StringType(), True),
    T.StructField("status", T.StringType(), True),
    T.StructField("customer_id", T.StringType(), True),
    T.StructField("customer_name", T.StringType(), True),
    T.StructField("customer_phone", T.StringType(), True),
    T.StructField("customer_email", T.StringType(), True),
    T.StructField("city", T.StringType(), True),
    T.StructField("district", T.StringType(), True),
    T.StructField("delivery_type", T.StringType(), True),
    T.StructField("delivery_cost", T.StringType(), True),
    T.StructField("payment_method", T.StringType(), True),
    T.StructField("payment_status", T.StringType(), True),
    T.StructField("payment_amount", T.StringType(), True),
    T.StructField("currency", T.StringType(), True),
    T.StructField("total_amount", T.StringType(), True),
    T.StructField("items_json", T.StringType(), True),
])

ITEM_SCHEMA = T.ArrayType(T.StructType([
    T.StructField("sku", T.StringType(), True),
    T.StructField("name", T.StringType(), True),
    T.StructField("qty", T.IntegerType(), True),
    T.StructField("unit_price", T.DoubleType(), True),
    T.StructField("total", T.DoubleType(), True),
]))

CORRECTION_STRUCT = T.StructType([
    T.StructField("field", T.StringType(), True),
    T.StructField("original_value", T.StringType(), True),
    T.StructField("corrected_value", T.StringType(), True),
    T.StructField("rule_code", T.StringType(), True),
])


ARABIC_TRANSLATION_FROM = "٠١٢٣٤٥٦٧٨٩"
ARABIC_TRANSLATION_TO = "0123456789"


def create_spark() -> SparkSession:
    import os
    from config.settings import ENABLE_GPU_ACCELERATION

    builder = (
        SparkSession.builder
        .appName(SPARK_APP_NAME + "-ELT")
        .master(SPARK_MASTER_URL)
        .config("spark.driver.memory", "6g")
        .config("spark.executor.memory", "6g")
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
    except Exception as exc:
        if SPARK_MASTER_URL.startswith("spark://"):
            if DISABLE_SPARK_FALLBACK:
                print(
                    f"[Spark Cluster FAIL] Standalone Master ({SPARK_MASTER_URL}) "
                    f"is unreachable and DISABLE_SPARK_FALLBACK=true. "
                    f"Will NOT fall back to local[*]."
                )
                raise
            print(f"[Spark Cluster Notice] Standalone Master ({SPARK_MASTER_URL}) offline/unreachable. Falling back to local[*] mode...")
            try:
                from pyspark import SparkContext
                if getattr(SparkContext, "_active_spark_context", None) is not None:
                    SparkContext._active_spark_context.stop()
                SparkContext._active_spark_context = None
            except Exception:
                pass
            builder = builder.master("local[*]")
            spark = builder.getOrCreate()
        elif ALLOW_SPARK_LOCAL_FALLBACK:
            builder = builder.master("local[*]")
            spark = builder.getOrCreate()
        else:
            raise RuntimeError(
                f"Spark standalone master is unreachable: {SPARK_MASTER_URL}. "
                "Path A evidence requires a real spark:// master. "
                "Set PIPELINE_ALLOW_SPARK_LOCAL_FALLBACK=true only for local development."
            ) from exc


    spark.sparkContext.setLogLevel(SPARK_LOG_LEVEL)
    try:

        jvm = spark._jvm
        jvm.org.apache.logging.log4j.core.config.Configurator.setLevel(
            "org.apache.spark.util.ShutdownHookManager",
            jvm.org.apache.logging.log4j.Level.OFF
        )
        jvm.org.apache.logging.log4j.core.config.Configurator.setLevel(
            "org.apache.spark.SparkEnv",
            jvm.org.apache.logging.log4j.Level.OFF
        )
    except Exception:
        pass
    return spark



def money_expr(column_name: str) -> F.Column:
    raw = F.trim(F.col(column_name))
    translated = F.translate(raw, ARABIC_TRANSLATION_FROM, ARABIC_TRANSLATION_TO)
    cleaned = F.regexp_replace(translated, "٫", ".")
    cleaned = F.regexp_replace(cleaned, "[,]", "")
    cleaned = F.regexp_replace(cleaned, r"(?i)ريال\s*يمني|ريال|ر\.ي|YER", "")
    cleaned = F.regexp_replace(cleaned, r"\s+", "")
    known = (
        F.when(cleaned.isin("ألفان", "الفان", "ألفين"), F.lit(2000.0))
        .when(cleaned.isin("خمسةآلاف", "خمسهآلاف", "خمسةالاف"), F.lit(5000.0))
        .when(cleaned.isin("عشرةآلاف", "عشرهآلاف"), F.lit(10000.0))
    )
    return F.coalesce(known, cleaned.cast(T.DoubleType()))


def standardize_phone_expr(column_name: str) -> F.Column:
    digits = F.regexp_replace(F.trim(F.col(column_name)), r"\D", "")
    local = F.when((F.length(digits) == 9) & digits.startswith("7"), F.concat(F.lit("+967"), digits))
    intl = F.when((F.length(digits) == 12) & digits.startswith("967"), F.concat(F.lit("+"), digits))
    return F.coalesce(local, intl, F.trim(F.col(column_name)))


def standardize_enum_expr(column_name: str) -> F.Column:
    text = F.regexp_replace(F.trim(F.col(column_name)), r"\s+", " ")
    return (
        F.when(text == "مدفوع", F.lit("تم الدفع"))
        .when(text == "دفع", F.lit("تم الدفع"))
        .when(text == "غير مدفوع", F.lit("بانتظار الدفع"))
        .otherwise(text)
    )


def correction(field: str, original: F.Column, corrected: F.Column, rule_code: str) -> F.Column:
    return F.when(
        original.isNotNull() & corrected.isNotNull() & (original != corrected),
        F.struct(
            F.lit(field).alias("field"),
            original.cast("string").alias("original_value"),
            corrected.cast("string").alias("corrected_value"),
            F.lit(rule_code).alias("rule_code"),
        ),
    ).otherwise(F.lit(None).cast(CORRECTION_STRUCT))


def non_null_array(items: list[F.Column]) -> F.Column:
    return F.filter(F.array(*items), lambda x: x.isNotNull())


def error_array(conditions: list[tuple[F.Column, str]]) -> F.Column:
    values = [F.when(cond, F.lit(code)).otherwise(F.lit(None).cast("string")) for cond, code in conditions]
    return non_null_array(values)


def latest_run_id() -> str | None:
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    try:
        doc = client[MONGO_DATABASE][RAW_COLLECTION].find_one(
            {}, {"run_id": 1, "_id": 0}, sort=[("ingested_at", -1)]
        )
        return doc.get("run_id") if doc else None
    finally:
        client.close()


def process_run(run_id: str, source_file: str | Path | None = None) -> dict:
    if SPARK_MASTER_URL.startswith("local[") and source_file:
        size_mb = Path(source_file).stat().st_size / (1024 * 1024)
        if size_mb > LOCAL_ELT_MAX_MB and not ALLOW_FULL_LOCAL_ELT:
            raise RuntimeError(
                f"Refusing full local ELT for {size_mb:.1f} MB. "
                f"Use Path A cluster or set PIPELINE_ALLOW_FULL_LOCAL_ELT=true."
            )

    spark: SparkSession | None = None
    started = time.perf_counter()

    try:
        spark = create_spark()
        raw = (
            spark.read
            .format("mongodb")
            .option("connection.uri", MONGO_URI)
            .option("database", MONGO_DATABASE)
            .option("collection", RAW_COLLECTION)
            .load()
            .filter(F.col("run_id") == run_id)
        )

        raw_count = raw.count()
        raw_json_col = F.coalesce(F.col("raw_record"), F.col("record_raw"))
        parsed = (
            raw
            .withColumn("raw_record", raw_json_col)
            .withColumn("record", F.from_json(F.col("raw_record"), RAW_SCHEMA))
            .select("run_id", "source_file", "source_row_number", "ingested_at", "engine_used", "raw_record", "record.*")
        )

        items = F.from_json(F.col("items_json"), ITEM_SCHEMA)
        parsed = parsed.withColumn("items", items)

        # ---------- safe normalizations ----------
        currency_clean = (
            F.when(F.upper(F.trim(F.col("currency"))).isin("YER", "ريال", "ريال يمني", "ر.ي"), F.lit("YER"))
            .otherwise(F.trim(F.col("currency")))
        )

        parsed = (
            parsed
            .withColumn("currency_clean", currency_clean)
            .withColumn("order_id_clean", F.trim(F.col("order_id")))
            .withColumn("customer_id_clean", F.trim(F.col("customer_id")))
            .withColumn("customer_name_clean", F.regexp_replace(F.trim(F.col("customer_name")), r"\s+", " "))
            .withColumn("city_clean", F.regexp_replace(F.trim(F.col("city")), r"\s+", " "))
            .withColumn("district_clean", F.regexp_replace(F.trim(F.col("district")), r"\s+", " "))
            .withColumn("status_clean", standardize_enum_expr("status"))
            .withColumn("payment_status_clean", standardize_enum_expr("payment_status"))
            .withColumn("delivery_type_clean", F.regexp_replace(F.trim(F.col("delivery_type")), r"\s+", " "))
            .withColumn("payment_method_clean", F.regexp_replace(F.trim(F.col("payment_method")), r"\s+", " "))
            .withColumn("phone_clean", standardize_phone_expr("customer_phone"))
        )

        email_base = F.lower(F.trim(F.col("customer_email")))
        email_repeated_fixed = F.regexp_replace(F.regexp_replace(email_base, r"@+", "@"), r"\.{2,}", ".")
        email_candidate_valid = email_repeated_fixed.rlike(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
        email_clean = F.when(email_candidate_valid, email_repeated_fixed).otherwise(email_base)

        # Date parsing supports the examples from the brief.
        parsed_date = F.coalesce(
            F.to_timestamp(F.col("order_date"), "yyyy-MM-dd'T'HH:mm:ss"),
            F.to_timestamp(F.col("order_date"), "yyyy-MM-dd"),
            F.to_timestamp(F.col("order_date"), "dd/MM/yyyy"),
            F.to_timestamp(F.col("order_date"), "dd-MM-yyyy"),
        )
        date_clean = F.date_format(parsed_date, "yyyy-MM-dd'T'HH:mm:ss")

        parsed = (
            parsed
            .withColumn("email_clean", email_clean)
            .withColumn("order_date_parsed", parsed_date)
            .withColumn("order_date_clean", date_clean)
            .withColumn("delivery_cost_num", money_expr("delivery_cost"))
            .withColumn("payment_amount_num", money_expr("payment_amount"))
            .withColumn("total_amount_num", money_expr("total_amount"))
        )

        # Detect duplicate business keys within the current run once.
        duplicates = (
            parsed.groupBy("order_id_clean")
            .count()
            .filter((F.col("order_id_clean").isNotNull()) & (F.col("count") > 1))
            .select(F.col("order_id_clean").alias("duplicate_order_id"))
            .withColumn("is_duplicate", F.lit(True))
        )
        parsed = parsed.join(
            duplicates,
            parsed.order_id_clean == duplicates.duplicate_order_id,
            "left",
        ).drop("duplicate_order_id")

        # JSON / item checks.
        items_is_corrupt = F.col("items_json").isNotNull() & F.col("items").isNull()
        items_is_empty = F.col("items").isNull() | (F.size(F.col("items")) == 0)
        negative_item_qty = F.coalesce(F.expr("exists(items, x -> x.qty < 0)"), F.lit(False))
        unknown_price = F.coalesce(
            F.expr("exists(items, x -> x.unit_price is null or x.total is null)"),
            F.lit(False),
        )

        items_total = F.expr(
            "aggregate(items, cast(0.0 as double), (acc, x) -> acc + "
            "coalesce(x.total, x.qty * x.unit_price), acc)"
        )
        # Some Spark versions interpret the above expression differently; use a second safe form below.
        items_total = F.aggregate(
            F.col("items"),
            F.lit(0.0),
            lambda acc, x: acc + F.coalesce(x["total"], x["qty"] * x["unit_price"]),
        )
        recalculated_total = items_total + F.coalesce(F.col("delivery_cost_num"), F.lit(0.0))

        amount_invalid = (
            (F.col("delivery_cost").isNotNull() & F.col("delivery_cost_num").isNull())
            | (F.col("payment_amount").isNotNull() & F.col("payment_amount_num").isNull())
            | (F.col("total_amount").isNotNull() & F.col("total_amount_num").isNull())
        )
        negative_value = (
            (F.col("delivery_cost_num") < 0)
            | (F.col("payment_amount_num") < 0)
            | (F.col("total_amount_num") < 0)
            | negative_item_qty
        )
        invalid_email = email_clean.isNull() | ~email_clean.rlike(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
        invalid_phone = F.col("customer_phone").isNotNull() & (~F.col("phone_clean").rlike(r"^\+9677\d{8}$"))
        invalid_currency = F.col("currency").isNotNull() & F.trim(F.col("currency")).isNotNull() & (~F.col("currency_clean").isin("YER"))
        invalid_date = F.col("order_date").isNull() | F.col("order_date_parsed").isNull()
        order_missing = F.col("order_id_clean").isNull() | (F.col("order_id_clean") == "")
        customer_missing = F.col("customer_id_clean").isNull() | (F.col("customer_id_clean") == "")

        base_errors = error_array([
            (order_missing, "MISSING_ORDER_ID"),
            (customer_missing, "MISSING_CUSTOMER_ID"),
            (invalid_date, "INVALID_IMPOSSIBLE_DATE"),
            (items_is_corrupt, "CORRUPTED_ITEMS_JSON"),
            (items_is_empty, "EMPTY_ITEMS"),
            (unknown_price, "UNKNOWN_PRICE"),
            (negative_value, "AMBIGUOUS_NEGATIVE_VALUE"),
            (F.coalesce(F.col("is_duplicate"), F.lit(False)), "DUPLICATE_ORDER_ID"),
            (invalid_email, "INVALID_EMAIL"),
            (invalid_phone, "INVALID_PHONE"),
            (invalid_currency, "INVALID_CURRENCY"),
            (amount_invalid, "INVALID_AMOUNT"),
        ])

        hard_errors = F.when(
            F.size(base_errors) > 1,
            F.concat(base_errors, F.array(F.lit("MULTIPLE_CONFLICTING_ERRORS")))
        ).otherwise(base_errors)

        # Correctable total only when item components are safe.
        can_recalculate_total = (
            (~items_is_corrupt)
            & (~items_is_empty)
            & (~unknown_price)
            & (~negative_item_qty)
            & F.col("delivery_cost_num").isNotNull()
            & (F.abs(recalculated_total - F.col("total_amount_num")) > F.lit(0.005))
        )
        total_clean = F.when(can_recalculate_total, F.round(recalculated_total, 2)).otherwise(F.col("total_amount_num"))

        corrections = non_null_array([
            correction("order_date", F.col("order_date"), F.col("order_date_clean"), "DATE_STANDARDIZE"),
            correction("customer_phone", F.col("customer_phone"), F.col("phone_clean"), "PHONE_NORMALIZE"),
            correction("customer_email", F.col("customer_email"), F.col("email_clean"), "EMAIL_REPEATED_SYMBOLS"),
            correction("customer_name", F.col("customer_name"), F.col("customer_name_clean"), "TRIM_WHITESPACE"),
            correction("city", F.col("city"), F.col("city_clean"), "TRIM_WHITESPACE"),
            correction("district", F.col("district"), F.col("district_clean"), "TRIM_WHITESPACE"),
            correction("status", F.col("status"), F.col("status_clean"), "STATUS_STANDARDIZE"),
            correction("payment_status", F.col("payment_status"), F.col("payment_status_clean"), "PAYMENT_STATUS_STANDARDIZE"),
            correction("delivery_type", F.col("delivery_type"), F.col("delivery_type_clean"), "DELIVERY_TYPE_TRIM"),
            correction("payment_method", F.col("payment_method"), F.col("payment_method_clean"), "PAYMENT_METHOD_TRIM"),
            correction("delivery_cost", F.col("delivery_cost"), F.col("delivery_cost_num"), "MONEY_NORMALIZE"),
            correction("payment_amount", F.col("payment_amount"), F.col("payment_amount_num"), "MONEY_NORMALIZE"),
            correction("total_amount", F.col("total_amount"), total_clean, "TOTAL_RECALCULATE"),
            correction("currency", F.col("currency"), F.col("currency_clean"), "CURRENCY_STANDARDIZE"),
        ])

        # Only valid/recoverable records should receive corrected status.
        hard_error_count = F.size(hard_errors)
        corrections = F.when(
            (hard_error_count > 0),
            F.lit([]).cast(T.ArrayType(CORRECTION_STRUCT)),
        ).otherwise(corrections)

        classified = (
            parsed
            .withColumn("error_codes", hard_errors)
            .withColumn("error_details", F.array_join(F.col("error_codes"), ","))
            .withColumn("corrections", corrections)
            .withColumn(
                "quality_status",
                F.when(F.size(F.col("error_codes")) > 0, F.lit("quarantine"))
                .when(F.size(F.col("corrections")) > 0, F.lit("corrected"))
                .otherwise(F.lit("valid")),
            )
            .withColumn(
                "order_id",
                F.col("order_id_clean"),
            )
            .withColumn("order_date", F.col("order_date_clean"))
            .withColumn("status", F.col("status_clean"))
            .withColumn("customer_id", F.col("customer_id_clean"))
            .withColumn("customer_name", F.col("customer_name_clean"))
            .withColumn("customer_phone", F.col("phone_clean"))
            .withColumn("customer_email", F.col("email_clean"))
            .withColumn("city", F.col("city_clean"))
            .withColumn("district", F.col("district_clean"))
            .withColumn("delivery_type", F.col("delivery_type_clean"))
            .withColumn("delivery_cost", F.round(F.col("delivery_cost_num"), 2))
            .withColumn("payment_method", F.col("payment_method_clean"))
            .withColumn("payment_status", F.col("payment_status_clean"))
            .withColumn("payment_amount", F.round(F.col("payment_amount_num"), 2))
            .withColumn("currency", F.col("currency_clean"))
            .withColumn("total_amount", F.round(total_clean, 2))
        )

        hash_cols = [
            "order_id", "order_date", "status", "customer_id", "customer_name",
            "customer_phone", "customer_email", "city", "district", "delivery_type",
            "delivery_cost", "payment_method", "payment_status", "payment_amount",
            "currency", "total_amount", "items_json",
        ]
        classified = classified.withColumn(
            "record_hash",
            F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in hash_cols]), 256),
        )

        classified = classified.select(
            "run_id", "source_file", "source_row_number", "ingested_at", "engine_used", "raw_record",
            "order_id", "order_date", "status", "customer_id", "customer_name",
            "customer_phone", "customer_email", "city", "district", "delivery_type",
            "delivery_cost", "payment_method", "payment_status", "payment_amount",
            "currency", "total_amount", "items_json", "record_hash",
            "quality_status", "corrections", "error_codes", "error_details",
        )

        # The classification result is used by metrics and two output branches.
        classified = classified.persist(StorageLevel.DISK_ONLY)

        summary_row = classified.agg(
            F.count("*").alias("raw_count"),
            F.sum(F.when(F.col("quality_status") == "valid", 1).otherwise(0)).alias("valid_count"),
            F.sum(F.when(F.col("quality_status") == "corrected", 1).otherwise(0)).alias("corrected_count"),
            F.sum(F.when(F.col("quality_status") == "quarantine", 1).otherwise(0)).alias("quarantine_count"),
        ).collect()[0]

        valid_df = classified.filter(F.col("quality_status").isin("valid", "corrected"))
        quarantine_df = classified.filter(F.col("quality_status") == "quarantine")

        # ---------- Final Upsert & Metrics Calculation ----------
        # Query existing order_ids + record_hashes BEFORE upsert so we can
        # accurately distinguish inserts, updates, and unchanged records.
        meta_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        try:
            existing_docs = meta_client[MONGO_DATABASE][VALIDATED_COLLECTION].find(
                {}, {"order_id": 1, "record_hash": 1, "_id": 0}
            )
            existing_hash_map = {
                doc["order_id"]: doc.get("record_hash")
                for doc in existing_docs
                if doc.get("order_id")
            }
        finally:
            meta_client.close()

        # Collect current valid/corrected order_ids + hashes for comparison.
        current_pairs = (
            valid_df
            .select("order_id", "record_hash")
            .filter(F.col("order_id").isNotNull())
            .collect()
        )
        current_map = {row["order_id"]: row["record_hash"] for row in current_pairs}

        # Compute accurate insert / update / unchanged counts.
        inserted_cnt = 0
        updated_cnt = 0
        unchanged_cnt = 0
        for oid, new_hash in current_map.items():
            if oid not in existing_hash_map:
                inserted_cnt += 1
            elif existing_hash_map[oid] != new_hash:
                updated_cnt += 1
            else:
                unchanged_cnt += 1

        (
            valid_df.write
            .format("mongodb")
            .mode("append")
            .option("connection.uri", MONGO_URI)
            .option("database", MONGO_DATABASE)
            .option("collection", VALIDATED_COLLECTION)
            .option("idFieldList", "order_id")
            .option("operationType", "replace")
            .option("upsertDocument", "true")
            .option("maxBatchSize", "512")
            .save()
        )

        # Quarantine: append mode is intentional — each run_id produces its
        # own quarantine audit trail.  The brief's Idempotency requirement
        # (Section 6.10) applies to orders_validated only; orders_raw and
        # orders_quarantine are historical / audit layers that may retain
        # entries per run_id.
        (
            quarantine_df.write
            .format("mongodb")
            .mode("append")
            .option("connection.uri", MONGO_URI)
            .option("database", MONGO_DATABASE)
            .option("collection", QUARANTINE_COLLECTION)
            .option("maxBatchSize", "512")
            .save()
        )

        write_counts = {
            "inserted_count": inserted_cnt,
            "updated_count": updated_cnt,
            "unchanged_count": unchanged_cnt,
        }

        error_rows = (
            classified
            .select(F.explode(F.col("error_codes")).alias("error_code"))
            .groupBy("error_code")
            .count()
            .collect()
        )
        error_case_counts = {row["error_code"]: int(row["count"]) for row in error_rows}

        elapsed = time.perf_counter() - started
        counts = {
            "run_id": run_id,
            "raw_count": int(summary_row["raw_count"] or 0),
            "valid_count": int(summary_row["valid_count"] or 0),
            "corrected_count": int(summary_row["corrected_count"] or 0),
            "quarantine_count": int(summary_row["quarantine_count"] or 0),
            "inserted_count": int(write_counts["inserted_count"] or 0),
            "updated_count": int(write_counts["updated_count"] or 0),
            "unchanged_count": int(write_counts["unchanged_count"] or 0),
            "elapsed_seconds": elapsed,
            "throughput": (int(summary_row["raw_count"] or 0) / elapsed) if elapsed else 0.0,
            "error_case_counts": error_case_counts,
        }
        append_run_metrics(counts)

        # Consistency assertion required by the brief.
        assert counts["raw_count"] == (
            counts["valid_count"] + counts["corrected_count"] + counts["quarantine_count"]
        ), "Run consistency equation failed"

        gpu_info = "N/A"
        try:
            import subprocess
            gpu_out = subprocess.check_output("nvidia-smi --query-gpu=name,memory.total --format=csv,noheader", shell=True, stderr=subprocess.DEVNULL).decode().strip()
            if gpu_out:
                gpu_info = f"{gpu_out} [ACTIVE]"
        except Exception:
            pass

        print("=" * 60)
        print("ELT + QUALITY + CLASSIFICATION & HARDWARE MONITORING")
        print("=" * 60)
        print(f"host_hardware_info  : {gpu_info}")
        for key, value in counts.items():
            if key != "run_id":
                print(f"{key:20}: {value}")
        print("=" * 60)
        return counts
    finally:
        if spark is not None:
            try:
                spark.stop()
            except Exception:
                pass



def get_latest_run_id() -> str | None:
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    try:
        col = client[MONGO_DATABASE][RAW_COLLECTION]
        doc = col.find_one({"_run_id": {"$exists": True}}, {"_run_id": 1}, sort=[("_id", -1)])
        if doc and "_run_id" in doc:
            return str(doc["_run_id"])
        from src.metrics import read_metrics
        history = read_metrics()
        if history:
            return history[-1].get("run_id")
        return None
    except Exception:
        return None
    finally:
        client.close()


if __name__ == "__main__":
    import sys
    arg = sys.argv[1] if len(sys.argv) > 1 else None
    if arg and len(arg) == 32 and not arg.endswith(".csv"):
        rid = arg
        target = sys.argv[2] if len(sys.argv) > 2 else None
    else:
        rid = get_latest_run_id()
        target = arg

    if not rid:
        print("No raw ingestion run_id found in orders_raw. Run batch_loader.py or spark_loader.py first.")
    else:
        print(f"Running ELT Pipeline for run_id: {rid}")
        process_run(rid, target)
