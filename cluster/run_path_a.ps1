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

$env:PIPELINE_SPARK_MASTER = "spark://$MasterIp`:$MasterPort"
$env:PIPELINE_INPUT_FILE = $InputFile
$env:PIPELINE_RUN_ELT_AFTER_RAW = "true"
$env:PIPELINE_ALLOW_FULL_LOCAL_ELT = "true"

& "$env:SPARK_HOME\bin\spark-submit.cmd" `
    --master $env:PIPELINE_SPARK_MASTER `
    --driver-memory 4g `
    --executor-memory 4g `
    --conf "spark.sql.adaptive.enabled=true" `
    --conf "spark.sql.ansi.enabled=false" `
    --packages "org.mongodb.spark:mongo-spark-connector_2.13:11.1.0" `
    "src\main.py" --file $InputFile

