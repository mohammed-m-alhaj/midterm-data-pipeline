Write-Host "=== Python ==="
python --version
Write-Host "=== Java ==="
java -version
Write-Host "=== Spark ==="
spark-submit.cmd --version
Write-Host "=== PySpark ==="
python -c "import pyspark; print(pyspark.__version__)"
Write-Host "=== PyMongo ==="
python -c "import pymongo; print(pymongo.version)"
