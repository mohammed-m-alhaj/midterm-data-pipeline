# ==============================================================================
# Script 3: Run Cluster Pipeline (Run on Laptop 1 after starting Master & Worker)
# ==============================================================================
Write-Host "============================================================" -ForegroundColor Green
Write-Host " LAUNCHING HYBRID DATA PIPELINE ON CLUSTER                  " -ForegroundColor Green
Write-Host "============================================================" -ForegroundColor Green

# Detect local Wi-Fi IP
$ip = (Get-NetIPAddress -AddressFamily IPv4 -InterfaceAlias "*Wi-Fi*" | Select-Object -ExpandProperty IPAddress -First 1)
if (-not $ip) {
    $ip = (Get-NetIPAddress -AddressFamily IPv4 | Where-Object { $_.IPAddress -notlike "127.*" -and $_.IPAddress -notlike "169.*" } | Select-Object -ExpandProperty IPAddress -First 1)
}
if (-not $ip) {
    $ip = "192.168.8.181"
}

$env:PIPELINE_SPARK_MASTER = "spark://${ip}:7077"

Write-Host "Configured PIPELINE_SPARK_MASTER = $env:PIPELINE_SPARK_MASTER" -ForegroundColor Cyan
Write-Host "Executing main.py with 250MB Spark Sample File..." -ForegroundColor Yellow
Write-Host "============================================================" -ForegroundColor Green

python src/main.py --file data/orders_spark_250mb.csv

Write-Host "Pipeline execution finished!" -ForegroundColor Green
