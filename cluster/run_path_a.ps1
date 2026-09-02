param(
    [string]$MasterIp = "127.0.0.1",
    [string]$InputFile = "data/orders_1m_sample.csv",
    [int]$MasterPort = 7077
)

if (-not $env:SPARK_HOME) {
    $pysparkPath = python -c "import pyspark, os; print(os.path.dirname(pyspark.__file__))"
    if ($pysparkPath) { $env:SPARK_HOME = $pysparkPath }
    else { throw "SPARK_HOME is not set." }
}

$sparkSubmit = Join-Path $env:SPARK_HOME "bin\spark-submit.cmd"
$masterUrl = "spark://${MasterIp}:${MasterPort}"

$env:PIPELINE_SPARK_MASTER = $masterUrl
$env:PIPELINE_INPUT_FILE = $InputFile
$env:PIPELINE_RUN_ELT_AFTER_RAW = "true"
$env:PIPELINE_ALLOW_FULL_LOCAL_ELT = "true"

# Path A Safety: disable silent fallback to local[*]
$env:PIPELINE_DISABLE_SPARK_FALLBACK = "true"

Write-Host "============================================================" -ForegroundColor Cyan
Write-Host " PATH A - Spark Standalone (Single Machine)                 " -ForegroundColor Cyan
Write-Host " Master         : $masterUrl"                                  -ForegroundColor Yellow
Write-Host " Input          : $InputFile"                                  -ForegroundColor Yellow
Write-Host " Fallback Guard : ENABLED (will fail if Master is down)"       -ForegroundColor Red
Write-Host "============================================================" -ForegroundColor Cyan

$ivyJars = Get-ChildItem "$HOME\.ivy2.5.2\jars\*.jar" -ErrorAction SilentlyContinue | Select-Object -ExpandProperty FullName
if ($ivyJars) {
    $jarsArg = $ivyJars -join ","
    $submitArgs = @(
        "--master", $masterUrl,
        "--driver-memory", "2g",
        "--executor-memory", "2g",
        "--conf", "spark.sql.adaptive.enabled=true",
        "--conf", "spark.sql.ansi.enabled=false",
        "--jars", "`"$jarsArg`"",
        "src\main.py", "--file", $InputFile
    )
} else {
    $submitArgs = @(
        "--master", $masterUrl,
        "--driver-memory", "2g",
        "--executor-memory", "2g",
        "--conf", "spark.sql.adaptive.enabled=true",
        "--conf", "spark.sql.ansi.enabled=false",
        "--packages", "org.mongodb.spark:mongo-spark-connector_2.13:11.1.0",
        "src\main.py", "--file", $InputFile
    )
}

& cmd.exe /c "`"$sparkSubmit`" $($submitArgs -join ' ')"
