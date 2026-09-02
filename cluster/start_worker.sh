#!/usr/bin/env bash
# ==============================================================================
# Start Spark Worker — Local Standalone (Single Machine, Linux)
# ==============================================================================
# Connects to the Master on 127.0.0.1:7077.
# Configurable cores, memory, and work directory.
# ==============================================================================
set -euo pipefail

MASTER_IP="${1:-127.0.0.1}"
MASTER_PORT="${2:-7077}"
WORKER_CORES="${SPARK_WORKER_CORES:-4}"
WORKER_MEMORY="${SPARK_WORKER_MEMORY:-4g}"
WORK_DIR="${SPARK_WORKER_DIR:-.spark-runtime/worker}"

# Auto-detect SPARK_HOME if not set
if [ -z "${SPARK_HOME:-}" ]; then
    SPARK_HOME=$(python3 -c "import pyspark, os; print(os.path.dirname(pyspark.__file__))")
    export SPARK_HOME
fi

# Ensure work directory exists and is writable
mkdir -p "$WORK_DIR"

echo "============================================================"
echo " SPARK WORKER — LOCAL STANDALONE (Linux)"
echo "============================================================"
echo " Master URL    : spark://${MASTER_IP}:${MASTER_PORT}"
echo " Worker Cores  : $WORKER_CORES"
echo " Worker Memory : $WORKER_MEMORY"
echo " Work Dir      : $WORK_DIR"
echo "============================================================"

"$SPARK_HOME/sbin/start-worker.sh" \
    "spark://${MASTER_IP}:${MASTER_PORT}" \
    --cores "$WORKER_CORES" \
    --memory "$WORKER_MEMORY" \
    --work-dir "$WORK_DIR"

echo ""
echo "Spark Worker launched. Check Master UI at http://${MASTER_IP}:8080"
