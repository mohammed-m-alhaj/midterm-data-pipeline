#!/usr/bin/env bash
# ==============================================================================
# Run Path A — Spark Standalone Pipeline (Linux)
# ==============================================================================
# Submits the pipeline to spark://127.0.0.1:7077 with fallback DISABLED.
# If Master is unreachable, the job will FAIL (not silently run local[*]).
# ==============================================================================
set -euo pipefail

MASTER_IP="${1:-127.0.0.1}"
MASTER_PORT="${2:-7077}"
INPUT_FILE="${3:-data/orders_1m_sample.csv}"

# Auto-detect SPARK_HOME if not set
if [ -z "${SPARK_HOME:-}" ]; then
    SPARK_HOME=$(python3 -c "import pyspark, os; print(os.path.dirname(pyspark.__file__))")
    export SPARK_HOME
fi

MASTER_URL="spark://${MASTER_IP}:${MASTER_PORT}"

# Path A safety: disable silent fallback to local[*]
export PIPELINE_SPARK_MASTER="$MASTER_URL"
export PIPELINE_INPUT_FILE="$INPUT_FILE"
export PIPELINE_RUN_ELT_AFTER_RAW="true"
export PIPELINE_ALLOW_FULL_LOCAL_ELT="true"
export PIPELINE_DISABLE_SPARK_FALLBACK="true"

echo "============================================================"
echo " PATH A - Spark Standalone (Single Machine, Linux)"
echo " Master         : $MASTER_URL"
echo " Input          : $INPUT_FILE"
echo " Fallback Guard : ENABLED (will fail if Master is down)"
echo "============================================================"

IVY_JARS=$(find "$HOME/.ivy2.5.2/jars" -name "*.jar" 2>/dev/null | tr '\n' ',' | sed 's/,$//')

if [ -n "$IVY_JARS" ]; then
    "$SPARK_HOME/bin/spark-submit" \
        --master "$MASTER_URL" \
        --driver-memory 2g \
        --executor-memory 2g \
        --conf "spark.sql.adaptive.enabled=true" \
        --conf "spark.sql.ansi.enabled=false" \
        --jars "$IVY_JARS" \
        src/main.py --file "$INPUT_FILE"
else
    "$SPARK_HOME/bin/spark-submit" \
        --master "$MASTER_URL" \
        --driver-memory 2g \
        --executor-memory 2g \
        --conf "spark.sql.adaptive.enabled=true" \
        --conf "spark.sql.ansi.enabled=false" \
        --packages "org.mongodb.spark:mongo-spark-connector_2.13:11.1.0" \
        src/main.py --file "$INPUT_FILE"
fi
