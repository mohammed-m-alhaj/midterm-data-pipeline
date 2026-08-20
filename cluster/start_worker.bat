@echo off
pushd "%~dp0"
if not exist "src\main.py" (
    if exist "..\src\main.py" cd /d ".."
)
title Smart Spark Worker Launcher - Laptop 2

echo ============================================================
echo  STARTING SPARK WORKER (LAPTOP 2 - SECONDARY MACHINE)
echo ============================================================

for /f "tokens=*" %%i in ('python -c "import pyspark, os; print(os.path.join(os.path.dirname(pyspark.__file__), 'bin'))"') do set "SPARK_BIN=%%i"

set "DEFAULT_IP=10.183.237.106"
if exist "data\master_ip.txt" (
    for /f "tokens=*" %%i in (data\master_ip.txt) do set "DEFAULT_IP=%%i"
)

echo.
echo Laptop 1 Master IP Address is required to connect the cluster.
set "INPUT_IP="
set /p INPUT_IP="Enter Laptop 1 IP Address [Default: %DEFAULT_IP%]: "
if "%INPUT_IP%"=="" set "MASTER_IP=%DEFAULT_IP%"
if not "%INPUT_IP%"=="" set "MASTER_IP=%INPUT_IP%"

echo.
echo ============================================================
echo Connecting Worker to spark://%MASTER_IP%:7077 ...
echo ============================================================

start "Spark-Worker-Secondary" cmd /k ""%SPARK_BIN%\spark-class.cmd" org.apache.spark.deploy.worker.Worker spark://%MASTER_IP%:7077"

echo.
echo Worker has been launched! Check Master UI on Laptop 1 at: http://%MASTER_IP%:8080
pause
popd
