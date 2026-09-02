# ==============================================================================
# Start Spark Master & Worker — Local Standalone (Single Machine)
# ==============================================================================
# Path A setup: Master + Worker on the SAME machine using 127.0.0.1.
# No second laptop or network configuration required.
# ==============================================================================

$ip = "127.0.0.1"

# Auto-detect SPARK_HOME if not set
if (-not $env:SPARK_HOME) {
    $env:SPARK_HOME = (python -c "import pyspark, os; print(os.path.dirname(pyspark.__file__))")
}

$sparkBin = Join-Path $env:SPARK_HOME "bin"

Write-Host "============================================================" -ForegroundColor Green
Write-Host " SPARK LOCAL STANDALONE - SINGLE MACHINE                    " -ForegroundColor Green
Write-Host "============================================================" -ForegroundColor Green
Write-Host "SPARK_HOME             : $env:SPARK_HOME"                      -ForegroundColor Yellow
Write-Host "Bind Address           : $ip"                                  -ForegroundColor Yellow
Write-Host "Master Web UI          : http://${ip}:8080"                    -ForegroundColor Cyan
Write-Host "Spark Master URL       : spark://${ip}:7077"                   -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Green

# 1. Launch Spark Master
$masterCmd = "`"$sparkBin\spark-class.cmd`" org.apache.spark.deploy.master.Master --host $ip --port 7077"
Start-Process cmd.exe -ArgumentList "/k title Spark-Master & $masterCmd"

Start-Sleep -Seconds 4

# Ensure worker runtime directory exists
$workDir = ".spark-runtime/worker"
if (-not (Test-Path $workDir)) {
    New-Item -ItemType Directory -Path $workDir -Force | Out-Null
}

# 2. Launch Spark Worker on the same machine
$workerCores = if ($env:SPARK_WORKER_CORES) { $env:SPARK_WORKER_CORES } else { "4" }
$workerMem = if ($env:SPARK_WORKER_MEMORY) { $env:SPARK_WORKER_MEMORY } else { "4g" }
$workerCmd = "`"$sparkBin\spark-class.cmd`" org.apache.spark.deploy.worker.Worker spark://${ip}:7077 --cores $workerCores --memory $workerMem --work-dir `"$workDir`""
Start-Process cmd.exe -ArgumentList "/k title Spark-Worker-Local & $workerCmd"

Write-Host ""
Write-Host "Spark Master + Worker launched on $ip" -ForegroundColor Green
Write-Host "Verify Worker is ALIVE at http://${ip}:8080 before running the pipeline." -ForegroundColor Yellow
