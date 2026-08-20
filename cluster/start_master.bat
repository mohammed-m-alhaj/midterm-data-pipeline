@echo off
pushd "%~dp0"
if not exist "src\main.py" (
    if exist "..\src\main.py" cd /d ".."
)
title Smart Spark Master Launcher

echo ============================================================
echo  1. INITIALIZING MONGODB SETUP (mongo_setup.py)
echo ============================================================
python src\mongo_setup.py

echo.
echo ============================================================
echo  2. AUTO-DETECTING NETWORK AND SPARK BINARIES
echo ============================================================

for /f "tokens=*" %%i in ('python -c "import pyspark, os; print(os.path.join(os.path.dirname(pyspark.__file__), 'bin'))"') do set "SPARK_BIN=%%i"
for /f "tokens=*" %%i in ('python -c "import socket; print(socket.gethostbyname(socket.gethostname()))"') do set "LOCAL_IP=%%i"

if "%LOCAL_IP%"=="" set "LOCAL_IP=10.183.237.106"

if not exist "data" mkdir "data"
echo %LOCAL_IP%> "data\master_ip.txt"

echo Master Laptop IP     : %LOCAL_IP%
echo Master Web UI Link   : http://%LOCAL_IP%:8080
echo Master Connection URI: spark://%LOCAL_IP%:7077
echo ============================================================

echo Opening Windows Firewall port 7077 for Spark...
netsh advfirewall firewall add rule name="Spark Master Port 7077" dir=in action=allow protocol=TCP localport=7077 >nul 2>&1

echo.
echo Launching Spark Master and Local Worker...
start "Spark-Master" cmd /k ""%SPARK_BIN%\spark-class.cmd" org.apache.spark.deploy.master.Master --host %LOCAL_IP% --port 7077"
ping 127.0.0.1 -n 4 >nul
start "Spark-Worker-Local" cmd /k ""%SPARK_BIN%\spark-class.cmd" org.apache.spark.deploy.worker.Worker spark://%LOCAL_IP%:7077"

echo.
echo ============================================================
echo  SUCCESS: Spark Master and Local Worker are Running!
echo  Web UI: http://%LOCAL_IP%:8080
echo ============================================================
timeout /t 2 >nul
popd
exit
