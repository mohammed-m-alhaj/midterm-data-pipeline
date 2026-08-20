# ==============================================================================
# Script 1: Start Spark Master & Local Worker (Run on Laptop 1 - Main Laptop)
# ==============================================================================
Write-Host "============================================================" -ForegroundColor Green
Write-Host " STARTING SPARK MASTER & LOCAL WORKER (MAIN LAPTOP)         " -ForegroundColor Green
Write-Host "============================================================" -ForegroundColor Green

# 1. Automatically detect local Wi-Fi IPv4 address
$ip = (Get-NetIPAddress -AddressFamily IPv4 -InterfaceAlias "*Wi-Fi*" | Select-Object -ExpandProperty IPAddress -First 1)
if (-not $ip) {
    $ip = (Get-NetIPAddress -AddressFamily IPv4 | Where-Object { $_.IPAddress -notlike "127.*" -and $_.IPAddress -notlike "169.*" } | Select-Object -ExpandProperty IPAddress -First 1)
}

if (-not $ip) {
    $ip = "127.0.0.1"
}

Write-Host "Detected Local Wi-Fi IP : $ip" -ForegroundColor Yellow
Write-Host "Master Web UI Link     : http://${ip}:8080" -ForegroundColor Cyan
Write-Host "Spark Master Connection: spark://${ip}:7077" -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Green

# 2. Launch Spark Master in a background window
$masterCmd = "%SPARK_HOME%\bin\spark-class.cmd org.apache.spark.deploy.master.Master --host $ip --port 7077"
Start-Process cmd.exe -ArgumentList "/k title Spark-Master && $masterCmd"

Start-Sleep -Seconds 3

# 3. Launch Local Spark Worker in a background window
$workerCmd = "%SPARK_HOME%\bin\spark-class.cmd org.apache.spark.deploy.worker.Worker spark://${ip}:7077"
Start-Process cmd.exe -ArgumentList "/k title Spark-Worker-Local && $workerCmd"

Write-Host "Spark Master and Local Worker have been launched!" -ForegroundColor Green
Write-Host "Now run start_worker.ps1 on Laptop 2 pointing to: $ip" -ForegroundColor Yellow
