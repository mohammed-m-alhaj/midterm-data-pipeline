@echo off
pushd "%~dp0"
title RAZI UNIVERSITY - HYBRID BIG DATA PIPELINE CONTROLLER
color 0A

:MENU
cls
echo ==============================================================================
echo      RAZI UNIVERSITY - BIG DATA HYBRID DATA PIPELINE CONTROLLER
echo      Python Batch - Apache Spark - MongoDB - ELT - Idempotency - GPU
echo ==============================================================================
echo.
echo  [1] Pre-Flight System Health Check (Check Python, Spark, MongoDB, GPU)
echo  [2] Run MongoDB Setup AND Indexes (mongo_setup.py)
echo  [3] Start Spark Master AND Local Worker (Laptop 1 - Main Machine)
echo  [4] Start Spark Secondary Worker (Laptop 2 - Secondary Machine)
echo  [5] Execute Hybrid Data Pipeline (src/main.py)
echo  [6] Launch Interactive Web Dashboard (http://localhost:8000)
echo  [7] Run Automated Pytest Test Suite (26 Unit Tests)
echo  [8] Create Custom Data Sample (create_small_sample.py)
echo  [0] Exit
echo.
echo ==============================================================================
set /p choice="Select an option [0-8]: "

if "%choice%"=="1" goto HEALTH_CHECK
if "%choice%"=="2" goto MONGO_SETUP
if "%choice%"=="3" goto START_MASTER
if "%choice%"=="4" goto START_WORKER
if "%choice%"=="5" goto RUN_PIPELINE
if "%choice%"=="6" goto LAUNCH_DASHBOARD
if "%choice%"=="7" goto RUN_TESTS
if "%choice%"=="8" goto CREATE_SAMPLE
if "%choice%"=="0" exit
goto MENU

:HEALTH_CHECK
cls
echo ==============================================================================
echo                     SYSTEM PRE-FLIGHT HEALTH CHECK
echo ==============================================================================
python -c "import sys, pyspark, pymongo; print('[OK] Python Version     :', sys.version.split()[0]); print('[OK] PySpark Version    :', pyspark.__version__); print('[OK] PyMongo Version    :', pymongo.__version__)"
python -c "import subprocess; print('[OK] GPU Accelerator   :', subprocess.check_output('nvidia-smi --query-gpu=name,memory.total --format=csv,noheader', shell=True).decode().strip())" 2>nul || echo [INFO] GPU Status: CPU Mode / Standard Driver
python -c "from pymongo import MongoClient; c=MongoClient('mongodb://127.0.0.1:27017', serverSelectionTimeoutMS=2000); c.server_info(); print('[OK] MongoDB Connection  : ACTIVE (port 27017)')" 2>nul || echo [WARNING] MongoDB: Ensure MongoDB Service is running on port 27017
echo ==============================================================================
echo Pre-flight check complete.
pause
goto MENU

:MONGO_SETUP
cls
echo ==============================================================================
echo                    EXECUTING MONGODB SETUP AND INDEXES
echo ==============================================================================
if exist "%~dp0src\mongo_setup.py" (
    python "%~dp0src\mongo_setup.py"
) else (
    python "%~dp0..\src\mongo_setup.py"
)
echo ==============================================================================
pause
goto MENU

:START_MASTER
cls
if exist "%~dp0start_master.bat" (
    start "" "%~dp0start_master.bat"
) else (
    start "" "%~dp0..\start_master.bat"
)
goto MENU

:START_WORKER
cls
if exist "%~dp0start_worker.bat" (
    start "" "%~dp0start_worker.bat"
) else (
    start "" "%~dp0..\start_worker.bat"
)
goto MENU

:RUN_PIPELINE
cls
echo ==============================================================================
echo                   EXECUTING HYBRID DATA PIPELINE
echo ==============================================================================
if exist "%~dp0run_pipeline.bat" (
    call "%~dp0run_pipeline.bat"
) else (
    call "%~dp0..\run_pipeline.bat"
)
pause
goto MENU

:LAUNCH_DASHBOARD
cls
echo ==============================================================================
echo               LAUNCHING INTERACTIVE WEB DASHBOARD
echo ==============================================================================
echo Dashboard will open at: http://localhost:8000
start "" http://localhost:8000
if exist "%~dp0dashboard_server.py" (
    python "%~dp0dashboard_server.py"
) else (
    python "%~dp0..\dashboard_server.py"
)
pause
goto MENU

:RUN_TESTS
cls
echo ==============================================================================
echo                 RUNNING AUTOMATED PYTEST SUITE
echo ==============================================================================
python -m pytest "%~dp0tests" 2>nul || python -m pytest "%~dp0..\tests"
echo ==============================================================================
pause
goto MENU

:CREATE_SAMPLE
cls
echo ==============================================================================
echo                 CREATING CUSTOM SAMPLE CSV FILE
echo ==============================================================================
set /p rows="Enter number of rows (e.g. 100000 or 500000): "
if "%rows%"==" " set rows=100000
if exist "%~dp0src\create_small_sample.py" (
    python "%~dp0src\create_small_sample.py" --rows %rows%
) else (
    python "%~dp0..\src\create_small_sample.py" --rows %rows%
)
echo ==============================================================================
pause
goto MENU
