@echo off
pushd "%~dp0"
if not exist "src\main.py" (
    if exist "..\src\main.py" cd /d ".."
)
title RAZI UNIVERSITY - PIPELINE EXECUTOR

echo ============================================================
echo  1. INITIALIZING MONGODB SETUP (mongo_setup.py)
echo ============================================================
python src\mongo_setup.py

echo.
echo ============================================================
echo  2. STARTING DATA PIPELINE
echo  (Reading INPUT_FILE directly from config/settings.py)
echo ============================================================

for /f "tokens=*" %%i in ('python -c "import socket; print(socket.gethostbyname(socket.gethostname()))"') do set "LOCAL_IP=%%i"
if "%LOCAL_IP%"=="" set "LOCAL_IP=10.183.237.106"

for /f "tokens=*" %%i in ('python -c "import socket; s=socket.socket(); s.settimeout(0.5); res=s.connect_ex(('%LOCAL_IP%', 7077)); s.close(); print('ON' if res==0 else 'OFF')"') do set "MASTER_STATUS=%%i"

if "%MASTER_STATUS%"=="ON" (
    echo Spark Standalone Master Detected at: spark://%LOCAL_IP%:7077
    set "PIPELINE_SPARK_MASTER=spark://%LOCAL_IP%:7077"
) else (
    echo Standalone Master Offline. Running in GPU Accelerated Local Mode (local[*])
    set "PIPELINE_SPARK_MASTER=local[*]"
)

python src\main.py

echo.
echo ============================================================
echo  Pipeline Execution Finished Successfully!
echo ============================================================
pause
popd
