#!/usr/bin/env bash
# ==============================================================================
# Start Spark Master — Local Standalone (Single Machine, Linux)
# ==============================================================================
# Path A setup: Master on 127.0.0.1.
# Run start_worker.sh separately to attach a Worker.
# ==============================================================================
set -euo pipefail

IP="127.0.0.1"

# Auto-detect SPARK_HOME if not set
if [ -z "${SPARK_HOME:-}" ]; then
    SPARK_HOME=$(python3 -c "import pyspark, os; print(os.path.dirname(pyspark.__file__))")
    export SPARK_HOME
fi

echo "============================================================"
echo " SPARK MASTER — LOCAL STANDALONE (Linux)"
echo "============================================================"
echo " SPARK_HOME : $SPARK_HOME"
echo " Bind Address: $IP"
echo " Master URL  : spark://${IP}:7077"
echo " Web UI      : http://${IP}:8080"
echo "============================================================"

"$SPARK_HOME/sbin/start-master.sh" --host "$IP" --port 7077

echo ""
echo "Spark Master launched. Verify at http://${IP}:8080"
echo "Next: run ./cluster/start_worker.sh"
