# ==============================================================================
# Start Additional Spark Worker (Multi-Machine Only)
# ==============================================================================
# NOTE: This script is NOT required for the single-machine Path A scenario.
# start_master.ps1 already launches both Master AND Worker on 127.0.0.1.
#
# This script is kept for reference only — use it if you ever need to add
# a worker from a second machine in a multi-node cluster setup.
# ==============================================================================

param (
    [string]$MasterIP = "127.0.0.1"
)

# Auto-detect SPARK_HOME if not set
if (-not $env:SPARK_HOME) {
    $env:SPARK_HOME = (python -c "import pyspark, os; print(os.path.dirname(pyspark.__file__))")
}

$sparkBin = Join-Path $env:SPARK_HOME "bin"

Write-Host "============================================================" -ForegroundColor Green
Write-Host " STARTING ADDITIONAL SPARK WORKER                           " -ForegroundColor Green
Write-Host "============================================================" -ForegroundColor Green

if (-not $MasterIP) {
    $MasterIP = Read-Host "Enter Master IP (Default: 127.0.0.1)"
}
if (-not $MasterIP) {
    $MasterIP = "127.0.0.1"
}

Write-Host "Connecting to Spark Master at: spark://${MasterIP}:7077" -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Green

$workerCmd = "`"$sparkBin\spark-class.cmd`" org.apache.spark.deploy.worker.Worker spark://${MasterIP}:7077"
Start-Process cmd.exe -ArgumentList "/k title Spark-Worker-Extra & $workerCmd"

Write-Host "Spark Worker has been launched and connected to Master!" -ForegroundColor Green
