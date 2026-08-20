"""Dashboard Server for Midterm Data Pipeline

Provides a zero-dependency HTTP server and REST API for the Interactive Pipeline Visualizer.
Serves index.html, queries MongoDB, executes main pipeline actions, and dynamically updates settings.
Full CORS support enabled for file:// and localhost origins.
"""
from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

PROJECT_ROOT = Path(__file__).resolve().parent
SRC_DIR = PROJECT_ROOT / "src"
CONFIG_FILE = PROJECT_ROOT / "config" / "settings.py"

for p in (str(PROJECT_ROOT), str(SRC_DIR)):
    if p not in sys.path:
        sys.path.insert(0, p)

from config.settings import (
    BATCH_SIZE,
    DATA_DIR,
    ENABLE_GPU_ACCELERATION,
    INPUT_FILE,
    LOCAL_ELT_MAX_MB,
    MONGO_DATABASE,
    MONGO_URI,
    QUARANTINE_COLLECTION,
    RAW_COLLECTION,
    REPORTS_DIR,
    RUN_ELT_AFTER_RAW,
    SMALL_FILE_THRESHOLD_MB,
    SMALL_SAMPLE_ROWS,
    SPARK_MASTER_URL,
    SPARK_PARTITIONS,
    VALIDATED_COLLECTION,
)
from pymongo import MongoClient

PORT = 8000
WEB_DIR = PROJECT_ROOT / "web"
RESULTS_FILE = REPORTS_DIR / "results.json"


def get_mongo_db():
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)
    return client, client[MONGO_DATABASE]


def list_available_files():
    files = []
    if DATA_DIR.exists():
        for p in DATA_DIR.glob("*.csv"):
            size_mb = p.stat().st_size / (1024 * 1024)
            files.append({
                "name": p.name,
                "path": str(p),
                "size_mb": round(size_mb, 2)
            })
    return files


class DashboardRequestHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass

    def send_json(self, data, status=200):
        try:
            body = json.dumps(data, default=str, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
            self.send_header("Access-Control-Allow-Headers", "*")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except Exception:
            pass

    def do_OPTIONS(self):
        try:
            self.send_response(200)
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
            self.send_header("Access-Control-Allow-Headers", "*")
            self.end_headers()
        except Exception:
            pass

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        query = parse_qs(parsed.query)

        if path == "/":
            self.serve_file(WEB_DIR / "index.html", "text/html; charset=utf-8")
            return

        if path == "/api/settings":
            input_path = Path(INPUT_FILE)
            size_mb = input_path.stat().st_size / (1024 * 1024) if input_path.is_file() else 0.0
            available_files = list_available_files()

            self.send_json({
                "INPUT_FILE": str(INPUT_FILE),
                "INPUT_FILE_NAME": input_path.name,
                "INPUT_FILE_SIZE_MB": round(size_mb, 2),
                "SMALL_FILE_THRESHOLD_MB": SMALL_FILE_THRESHOLD_MB,
                "SMALL_SAMPLE_ROWS": SMALL_SAMPLE_ROWS,
                "BATCH_SIZE": BATCH_SIZE,
                "SPARK_PARTITIONS": SPARK_PARTITIONS,
                "SPARK_MASTER_URL": SPARK_MASTER_URL,
                "ENABLE_GPU_ACCELERATION": ENABLE_GPU_ACCELERATION,
                "LOCAL_ELT_MAX_MB": LOCAL_ELT_MAX_MB,
                "RUN_ELT_AFTER_RAW": RUN_ELT_AFTER_RAW,
                "MONGO_DATABASE": MONGO_DATABASE,
                "MONGO_URI": MONGO_URI,
                "AVAILABLE_FILES": available_files,
            })
            return

        if path == "/api/results":
            if RESULTS_FILE.exists():
                try:
                    payload = json.loads(RESULTS_FILE.read_text(encoding="utf-8"))
                    self.send_json(payload)
                    return
                except Exception as e:
                    self.send_json({"error": str(e)}, 500)
                    return
            self.send_json([])
            return

        if path.startswith("/api/mongo/"):
            collection_name = path.replace("/api/mongo/", "")
            limit = int(query.get("limit", ["10"])[0])
            filter_status = query.get("quality_status", [None])[0]
            search_term = query.get("search", [None])[0]

            try:
                client, db = get_mongo_db()
                col = db[collection_name]
                q = {}
                if filter_status:
                    q["quality_status"] = filter_status
                if search_term:
                    pattern = {"$regex": search_term, "$options": "i"}
                    q["$or"] = [
                        {"order_id": pattern},
                        {"customer_id": pattern},
                        {"customer_name": pattern},
                        {"error_codes": pattern},
                        {"raw_record": pattern},
                        {"record_raw": pattern}
                    ]

                docs = list(col.find(q, {"_id": 0}).limit(limit))
                total_count = col.count_documents(q)
                client.close()

                self.send_json({
                    "collection": collection_name,
                    "total_count": total_count,
                    "returned": len(docs),
                    "documents": docs,
                    "search": search_term
                })
                return
            except Exception as e:
                self.send_json({"error": str(e), "collection": collection_name, "total_count": 0, "returned": 0, "documents": []}, 500)
                return

        file_path = WEB_DIR / path.lstrip("/")
        if file_path.is_file():
            mime = "text/html"
            if file_path.suffix == ".css":
                mime = "text/css"
            elif file_path.suffix == ".js":
                mime = "application/javascript"
            elif file_path.suffix == ".json":
                mime = "application/json"
            elif file_path.suffix == ".png":
                mime = "image/png"
            self.serve_file(file_path, mime)
            return

        self.send_json({"error": "Not Found"}, 404)

    def do_POST(self):
        parsed = urlparse(self.path)
        path = parsed.path
        content_length = int(self.headers.get("Content-Length", 0))
        body_str = self.rfile.read(content_length).decode("utf-8") if content_length > 0 else "{}"
        body = json.loads(body_str) if body_str else {}

        if path == "/api/save-settings":
            try:
                content = CONFIG_FILE.read_text(encoding="utf-8")
                
                if "file_name" in body:
                    target_file = body["file_name"]
                    content = re.sub(r'str\(DATA_DIR / "[^"]+"\)', f'str(DATA_DIR / "{target_file}")', content, count=1)
                
                if "sample_rows" in body:
                    val = int(body["sample_rows"])
                    content = re.sub(r'SMALL_SAMPLE_ROWS = int\(os\.getenv\("PIPELINE_SAMPLE_ROWS", "[^"]+"\)\)', f'SMALL_SAMPLE_ROWS = int(os.getenv("PIPELINE_SAMPLE_ROWS", "{val}"))', content)

                if "threshold_mb" in body:
                    val = int(body["threshold_mb"])
                    content = re.sub(r'SMALL_FILE_THRESHOLD_MB = \d+', f'SMALL_FILE_THRESHOLD_MB = {val}', content)

                if "batch_size" in body:
                    val = int(body["batch_size"])
                    content = re.sub(r'BATCH_SIZE = int\(os\.getenv\("PIPELINE_BATCH_SIZE", "[^"]+"\)\)', f'BATCH_SIZE = int(os.getenv("PIPELINE_BATCH_SIZE", "{val}"))', content)

                if "spark_partitions" in body:
                    val = int(body["spark_partitions"])
                    content = re.sub(r'SPARK_PARTITIONS = int\(os\.getenv\("PIPELINE_SPARK_PARTITIONS", "[^"]+"\)\)', f'SPARK_PARTITIONS = int(os.getenv("PIPELINE_SPARK_PARTITIONS", "{val}"))', content)

                if "spark_master" in body:
                    val = body["spark_master"]
                    content = re.sub(r'SPARK_MASTER_URL = os\.getenv\("PIPELINE_SPARK_MASTER", "[^"]+"\)', f'SPARK_MASTER_URL = os.getenv("PIPELINE_SPARK_MASTER", "{val}")', content)

                if "enable_gpu" in body:
                    val = "true" if body["enable_gpu"] else "false"
                    content = re.sub(r'ENABLE_GPU_ACCELERATION = os\.getenv\("PIPELINE_ENABLE_GPU", "[^"]+"\)\.lower\(\) == "true"', f'ENABLE_GPU_ACCELERATION = os.getenv("PIPELINE_ENABLE_GPU", "{val}").lower() == "true"', content)

                CONFIG_FILE.write_text(content, encoding="utf-8")

                self.send_json({"status": "Settings updated successfully in config/settings.py"})
                return
            except Exception as e:
                self.send_json({"error": str(e)}, 500)
                return

        if path == "/api/run-pipeline":
            env = os.environ.copy()
            if body.get("file"):
                env["PIPELINE_INPUT_FILE"] = str(DATA_DIR / body["file"])
            if body.get("batch_size"):
                env["PIPELINE_BATCH_SIZE"] = str(body["batch_size"])
            if body.get("spark_partitions"):
                env["PIPELINE_SPARK_PARTITIONS"] = str(body["spark_partitions"])
            if body.get("spark_master"):
                env["PIPELINE_SPARK_MASTER"] = str(body["spark_master"])

            cmd = [sys.executable, str(SRC_DIR / "main.py")]
            proc = subprocess.run(cmd, cwd=str(SRC_DIR), capture_output=True, text=True, encoding="utf-8", errors="replace", env=env)
            
            latest_metric = {}
            if RESULTS_FILE.exists():
                try:
                    payload = json.loads(RESULTS_FILE.read_text(encoding="utf-8"))
                    if payload and isinstance(payload, list):
                        latest_metric = payload[-1]
                except Exception:
                    pass

            self.send_json({
                "returncode": proc.returncode,
                "stdout": proc.stdout,
                "stderr": proc.stderr,
                "latest_metric": latest_metric,
            })
            return

        if path == "/api/create-sample":
            rows = body.get("rows", SMALL_SAMPLE_ROWS)
            output_file = body.get("output_file", "orders_small_sample.csv")
            source_file = body.get("source_file", "orders_huge_mixed_quality.csv")

            # Auto-fallback: if requested source doesn't exist, pick the largest available CSV
            source_path = DATA_DIR / source_file
            if not source_path.exists():
                available = sorted(DATA_DIR.glob("*.csv"), key=lambda p: p.stat().st_size, reverse=True)
                # Don't use the output file itself as source
                available = [p for p in available if p.name != output_file]
                if available:
                    source_path = available[0]
                    source_file = source_path.name

            cmd = [
                sys.executable, str(SRC_DIR / "create_small_sample.py"),
                "--rows", str(rows),
                "--input", str(source_path),
                "--output", str(DATA_DIR / output_file)
            ]

            proc = subprocess.run(cmd, cwd=str(SRC_DIR), capture_output=True, text=True, encoding="utf-8", errors="replace")
            
            out_path = DATA_DIR / output_file
            size_mb = round(out_path.stat().st_size / (1024 * 1024), 2) if out_path.exists() else 0.0

            # Automatically set INPUT_FILE in settings.py to the newly created sample if requested
            if body.get("set_as_input", True):
                try:
                    content = CONFIG_FILE.read_text(encoding="utf-8")
                    content = re.sub(r'str\(DATA_DIR / "[^"]+"\)', f'str(DATA_DIR / "{output_file}")', content, count=1)
                    CONFIG_FILE.write_text(content, encoding="utf-8")
                except Exception:
                    pass

            self.send_json({
                "returncode": proc.returncode,
                "stdout": proc.stdout,
                "stderr": proc.stderr,
                "rows_created": rows,
                "source_file": source_file,
                "output_file": out_path.name if out_path.exists() else output_file,
                "file_size_mb": size_mb,
                "note": f"Source auto-selected: {source_file}" if source_file != body.get("source_file") else ""
            })
            return

        if path == "/api/reset-db":
            try:
                client, db = get_mongo_db()
                db[RAW_COLLECTION].drop()
                db[VALIDATED_COLLECTION].drop()
                db[QUARANTINE_COLLECTION].drop()
                client.close()
                subprocess.run([sys.executable, str(SRC_DIR / "mongo_setup.py")], cwd=str(SRC_DIR), check=True)
                self.send_json({"status": "Database reset and re-initialized successfully"})
                return
            except Exception as e:
                self.send_json({"error": str(e)}, 500)
                return

        self.send_json({"error": "Not Found"}, 404)

    def serve_file(self, path: Path, content_type: str):
        if not path.is_file():
            self.send_json({"error": "File Not Found"}, 404)
            return
        try:
            content = path.read_bytes()
            self.send_response(200)
            self.send_header("Content-Type", content_type)
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Content-Length", str(len(content)))
            self.end_headers()
            self.wfile.write(content)
        except Exception:
            pass


def main():
    WEB_DIR.mkdir(parents=True, exist_ok=True)
    server = HTTPServer(("0.0.0.0", PORT), DashboardRequestHandler)
    print("=" * 70)
    print(f"MIDTERM DATA PIPELINE DASHBOARD SERVER")
    print(f"   URL: http://localhost:{PORT}")
    print("=" * 70)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down dashboard server.")
        server.server_close()


if __name__ == "__main__":
    main()
