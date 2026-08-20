# 🚀 Enterprise Hybrid Data Pipeline & Quality ELT Architecture
### *High-Throughput Order Processing Framework: Streaming Python Batch + Parallel Apache Spark + MongoDB + Automated Data Quality Engine*

[![Python](https://img.shields.io/badge/Python-3.11-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![PySpark](https://img.shields.io/badge/PySpark-4.2.0-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![MongoDB](https://img.shields.io/badge/MongoDB-8.0-47A248?style=for-the-badge&logo=mongodb&logoColor=white)](https://www.mongodb.com/)
[![Spark Cluster](https://img.shields.io/badge/Cluster-Spark_Standalone-007ACC?style=for-the-badge&logo=apache&logoColor=white)](https://spark.apache.org/docs/latest/spark-standalone.html)
[![Test Suite](https://img.shields.io/badge/Tests-33%20Passed%20%7C%20100%25-brightgreen?style=for-the-badge&logo=pytest&logoColor=white)](https://docs.pytest.org/)
[![Status](https://img.shields.io/badge/Status-Production_Ready-success?style=for-the-badge)](https://github.com/)

---

## 📌 Executive Summary

This repository delivers an **Enterprise-Grade Hybrid Data Pipeline** built to process large volumes of dirty, unformatted order data. The system dynamically routes datasets to the optimal ingestion engine based on file size thresholds:
- **Streaming Python Batch Engine:** Processes datasets $\le 200\text{ MB}$ using Python's streaming `csv.DictReader` and chunked MongoDB insertions without loading files into memory.
- **Distributed PySpark Engine:** Processes datasets $> 200\text{ MB}$ using PySpark DataFrame API, parallel partition rebalancing, and high-speed write tasks via the official MongoDB Spark Connector.

Following a strict **ELT (Extract, Load, Transform)** pattern, raw records are first ingested unmodified into MongoDB (`orders_raw`) to preserve complete source history and audit trails. Subsequently, PySpark executes automated data cleaning, standardization, and quality classification—separating records into **`orders_validated`** (with complete audit trails) or isolating unfixable records in **`orders_quarantine`** (with explicit error codes).

---

## 🏗️ System Architecture & Data Flow

```
                                  +-------------------+
                                  |  Unclean CSV File |
                                  +---------+---------+
                                            |
                                            v
                                 +---------------------+
                                 |   src/file_router   |
                                 +----------+----------+
                                            |
                    +-----------------------+-----------------------+
                    |                                               |
         [ Size <= 200 MB ]                                [ Size > 200 MB ]
                    |                                               |
                    v                                               v
        +-----------------------+                       +-----------------------+
        |   src/batch_loader    |                       |   src/spark_loader    |
        |  (Python Streaming)   |                       |    (PySpark Parallel) |
        +-----------+-----------+                       +-----------+-----------+
                    |                                               |
                    +-----------------------+-----------------------+
                                            |
                                            v
                                 +---------------------+
                                 | MongoDB: orders_raw | (Raw Source Retention)
                                 +----------+----------+
                                            |
                                            v
                                 +---------------------+
                                 |  src/elt_pipeline   | (PySpark Quality Transformation)
                                 +----------+----------+
                                            |
                    +-----------------------+-----------------------+
                    |                                               |
            [ Valid / Corrected ]                               [ Corrupt / Invalid ]
                    |                                               |
                    v                                               v
        +-----------------------+                       +-----------------------+
        | MongoDB: orders_valid |                       | MongoDB: orders_quar. |
        | (Unique Index + Hash) |                       | (Error Codes & Cause) |
        +-----------------------+                       +-----------------------+
```

---

## ✨ Key Technical Capabilities

### 1. Dynamic Engine Router (`src/file_router.py`)
- Central entry point via `src/main.py`.
- Evaluates file size against `SMALL_FILE_THRESHOLD_MB` (Default: 200 MB).
- Logs chosen engine, file size, threshold, and resolution reason prior to execution.

### 2. Zero-Loss Raw Ingestion Layer (`orders_raw`)
- Ingests raw CSV records without prior dropping, trimming, or filtering.
- Attaches source lineage metadata to every document:
  - `run_id`: Unique UUID generated for the pipeline run.
  - `source_file`: Absolute filesystem path of the source dataset.
  - `source_row_number`: Exact CSV line number.
  - `ingested_at`: ISO timestamp of ingestion.
  - `engine_used`: Processing engine used (`python_batch` or `pyspark`).
  - `raw_record`: Unmodified JSON payload representing original CSV values.

### 3. 9 Automated Quality Transformation Rules
Executed in PySpark (`src/elt_pipeline.py` & `src/quality_rules.py`) with deterministic, non-speculative transformation logic:
1. **Arabic Digits Conversion:** Normalizes `٠١٢٣٤٥٦٧٨٩` to standard ASCII `0123456789`.
2. **Currency Standardization:** Cleans textual currency symbols (`ريال`, `ر.ي`, `YER`) and normalizes valid codes to `YER`.
3. **Thousands Separators Removal:** Strips commas and Arabic decimal points (`125,000.00` $\rightarrow$ `125000.00`).
4. **Textual Word Prices:** Converts known word prices ("ألفان" $\rightarrow 2000$, "خمسة آلاف" $\rightarrow 5000$).
5. **Phone Number Standardization:** Normalizes Yemeni mobile phone formats to canonical `+9677XXXXXXXX`.
6. **Email Symbol Repair:** Repairs repeated symbols (`user@@mail..com` $\rightarrow$ `user@mail.com`) via regex rules.
7. **Date Format Standardization:** Parses diverse date formats (`yyyy-MM-dd`, `dd/MM/yyyy`, ISO) into standard ISO timestamps.
8. **Status Synonym Mapping:** Trims whitespace and maps synonyms ("مدفوع" / "دفع" $\rightarrow$ "تم الدفع").
9. **Total Recalculation:** Recomputes order total ($Total = \sum Items + Delivery$) when item unit prices and delivery costs are valid.

### 4. Comprehensive Audit Trail (`corrections`)
Every corrected record in `orders_validated` maintains an explicit `corrections` array detailing field alterations:
```json
{
  "quality_status": "corrected",
  "corrections": [
    {
      "field": "customer_email",
      "original_value": "user@@mail..com",
      "corrected_value": "user@mail.com",
      "rule_code": "EMAIL_REPEATED_SYMBOLS"
    }
  ]
}
```

### 5. Isolated Quarantine Layer (`orders_quarantine`)
Records with irreparable or ambiguous errors are routed to `orders_quarantine` with explicit diagnostic codes:

| Error Code | Trigger Condition & Description |
|---|---|
| `MISSING_ORDER_ID` | Order identifier is missing or null. |
| `MISSING_CUSTOMER_ID` | Customer identifier is missing or null. |
| `INVALID_IMPOSSIBLE_DATE` | Unparseable or non-existent date string. |
| `CORRUPTED_ITEMS_JSON` | Malformed or invalid JSON array in `items_json`. |
| `EMPTY_ITEMS` | Empty or missing order item list. |
| `UNKNOWN_PRICE` | Missing or uninferrable unit prices. |
| `AMBIGUOUS_NEGATIVE_VALUE` | Unexplainable negative quantity or amount. |
| `DUPLICATE_ORDER_ID` | Intra-batch duplicate business keys. |
| `MULTIPLE_CONFLICTING_ERRORS` | Multiple fundamental errors preventing safe recovery. |

### 6. Idempotency & Upsert Architecture
- **Stable Business Key:** `order_id` serves as the primary business identifier.
- **Database Uniqueness Enforcement:** Unique index `uq_validated_order_id` created on `orders_validated`.
- **Spark Connector Upsert:** Configured with `operationType=replace`, `upsertDocument=true`, and `idFieldList=order_id`.
- **SHA-256 Hash Auditing:** Generates `record_hash` per row to categorize database operations accurately into `inserted_count`, `updated_count`, and `unchanged_count`.

---

## 📂 Project Directory Structure

```text
midterm-data-pipeline/
├── config/
│   └── settings.py          # Centralized configuration & environment variables
├── src/
│   ├── main.py              # Main unified entry point & CLI pipeline runner
│   ├── file_router.py       # File size router & engine determination
│   ├── batch_loader.py      # Memory-efficient Python CSV streaming loader
│   ├── spark_loader.py      # PySpark parallel distributed CSV loader
│   ├── elt_pipeline.py       # PySpark data quality transformation & ELT engine
│   ├── quality_rules.py     # Deterministic data quality rules & regex patterns
│   ├── mongo_setup.py       # MongoDB collection setup, validators & indexes
│   └── metrics.py           # Execution metrics tracker & report logger
├── cluster/                 # Path A Spark Standalone Cluster scripts
├── data/                    # Sample datasets (Includes light demo sample)
├── reports/                 # Pipeline execution reports & metrics history
├── tests/                   # PyTest automated unit, quality & pipeline tests
├── web/                     # Interactive Web Dashboard frontend
├── dashboard_server.py      # Live Web Dashboard backend & API server
├── conftest.py              # PyTest configuration & test setup
├── requirements.txt         # Project Python dependencies
└── README.md                # System documentation
```

---

## ⚙️ Configuration Reference (`config/settings.py`)

All pipeline behaviors are controlled via centralized environment variables or default settings:

| Setting Parameter | Description | Default Value |
|---|---|---|
| `INPUT_FILE` | Default CSV file processed by the pipeline | `data/orders_small_sample.csv` |
| `SMALL_FILE_THRESHOLD_MB` | File size boundary (MB) for engine decision | `200` |
| `BATCH_SIZE` | Insert batch size for Python Batch Loader | `1000` |
| `SPARK_PARTITIONS` | Repartition count for PySpark parallel tasks | `8` |
| `MONGO_URI` | MongoDB connection string URI | `mongodb://127.0.0.1:27017` |
| `MONGO_DATABASE` | Target MongoDB database name | `midterm_pipeline` |

---

## 💻 Prerequisites & Setup Instructions

### 1. Prerequisites
- **Python:** 3.11 or higher
- **Java:** OpenJDK 17 (Required by PySpark)
- **MongoDB Server:** Version 8.x running locally or on custom URI

### 2. Installation
Clone the repository and install required packages:
```bash
git clone https://github.com/your-username/hybrid-data-pipeline.git
cd hybrid-data-pipeline
pip install -r requirements.txt
```

---

## 🚀 Step-by-Step Execution Guide

### 1. Initialize MongoDB Collections & Indexes
```bash
python src/mongo_setup.py
```

### 2. Execute Pipeline (Automatic Engine Selection)
```bash
# Process a small dataset (Triggers Python Batch Mode)
python src/main.py --file data/orders_test_5k.csv

# Process a large dataset (Triggers PySpark Engine Mode)
python src/main.py --file data/orders_spark_demo_250mb.csv
```

### 3. Idempotency Verification Test
Re-run the exact same file to verify that **zero duplicate records** are added to `orders_validated`:
```bash
python src/main.py --file data/orders_test_5k.csv
```

### 4. Launch Interactive Web Dashboard & Live Visualizer
```bash
python dashboard_server.py
```
Open **`http://localhost:8000`** in your web browser to access live KPI summary cards, interactive MongoDB document inspector, and data quality rule visualizations.

---

## 🧪 Automated Test Suite

Run unit tests, transformation checks, and pipeline integrity assertions via PyTest:
```bash
python -m pytest
```
*Current Status:* **33 Passed, 1 Skipped (100% Pass Rate)**.

---

## 📊 Benchmark Summary & Performance Analysis

Runtime metrics captured during production benchmarks:

| Engine / Phase | Input Dataset Size | Execution Time | Throughput | Key Metric / Result |
|---|---|---|---|---|
| **Python Batch (Raw)** | 5,000 Rows (2.09 MB) | 0.10 s | **48,037 rows/s** | 5 Batches @ 1000 rows/batch |
| **PySpark Raw Load** | 600,000 Rows (251.05 MB) | 17.11 s | **35,057 rows/s** | 8 Output Partitions Parallel Write |
| **ELT Transformation** | 600,000 Rows (251.05 MB) | 50.82 s | **11,807 rows/s** | 515,388 Validated / 84,612 Quarantine |
| **Idempotency Re-run** | 5,000 Rows (2.09 MB) | 23.89 s | N/A | **0 Inserted / 4,254 Unchanged (Zero Duplicates)** |

---

## 📄 License

Developed as an Enterprise-Grade Hybrid Data Pipeline Solution for high-throughput order ingestion, data quality enforcement, and fault-tolerant ELT operations.
