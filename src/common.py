from __future__ import annotations

import os
import subprocess
from pathlib import Path

# Enable ANSI escape sequences on Windows PowerShell & CMD
os.system("")

PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Terminal ANSI Color Palette
RESET = "\033[0m"
BOLD = "\033[1m"
GREEN = "\033[92m"
CYAN = "\033[96m"
YELLOW = "\033[93m"
MAGENTA = "\033[95m"
BLUE = "\033[94m"
WHITE = "\033[97m"
GREEN_BG = "\033[42m\033[30m"


def get_gpu_info() -> str:
    """Detect NVIDIA GPU if available. Returns a description string."""
    try:
        gpu_out = subprocess.check_output(
            "nvidia-smi --query-gpu=name,memory.total --format=csv,noheader",
            shell=True, stderr=subprocess.DEVNULL
        ).decode().strip()
        if gpu_out:
            return f"{gpu_out} [ACTIVE]"
    except Exception:
        pass
    return "N/A"


def silence_spark_warnings(spark) -> None:
    try:
        jvm = spark._jvm
        if jvm:
            log_config = jvm.org.apache.logging.log4j.core.config.Configurator
            level_off = jvm.org.apache.logging.log4j.Level.OFF
            for category in [
                "org.apache.spark.SparkEnv",
                "org.apache.spark.util.ShutdownHookManager",
                "org.apache.spark.SparkConf",
                "org.apache.spark.storage.BlockManager",
                "org.apache.spark.storage.DiskBlockManager",
                "org.apache.spark.storage",
                "org.apache.spark.util.Utils",
                "org.apache.spark.ContextCleaner",
                "org.apache.spark.SparkContext",
            ]:
                try:
                    log_config.setLevel(category, level_off)
                except Exception:
                    pass
    except Exception:
        pass
