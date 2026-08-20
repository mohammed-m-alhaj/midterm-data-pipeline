# ==============================================================================
# Script 2: Start Spark Worker (Run on Laptop 2 - Secondary Laptop)
# ==============================================================================
Write-Host "============================================================" -ForegroundColor Green
Write-Host " STARTING SPARK WORKER (SECONDARY LAPTOP)                   " -ForegroundColor Green
Write-Host "============================================================" -ForegroundColor Green

param (
    [string]$MasterIP = "192.168.8.181"
)

if (-not $MasterIP) {
    $MasterIP = Read-Host "Enter Laptop 1 Master IP (Default: 192.168.8.181)"
}
if (-not $MasterIP) {
    $MasterIP = "192.168.8.181"
}

Write-Host "Connecting to Spark Master at: spark://${MasterIP}:7077" -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Green

$workerCmd = "%SPARK_HOME%\bin\spark-class.cmd org.apache.spark.deploy.worker.Worker spark://${MasterIP}:7077"
Start-Process cmd.exe -ArgumentList "/k title Spark-Worker-Secondary && $workerCmd"

Write-Host "Spark Worker has been launched and connected to Master!" -ForegroundColor Green
