# Path A — Single-Machine Local Standalone Verification Report

## Environment

| Component | Version / Specification |
|---|---|
| OS | Windows 11 (amd64) |
| Python | 3.11.9 |
| Java | OpenJDK 17.0.11 (LTS) |
| PySpark | 4.2.0 |
| MongoDB | 8.0.4 Community Server |
| Spark Connector | `org.mongodb.spark:mongo-spark-connector_2.13:11.1.0` |
| Hardware Accelerator | NVIDIA GeForce RTX 5070 Ti Laptop GPU (12,227 MiB) |

---

## Test Configuration

- **Spark Mode:** Standalone Cluster (`spark://127.0.0.1:7077`)
- **Master + Worker:** Single Machine bound to `127.0.0.1`
- **Fallback Guard:** `PIPELINE_DISABLE_SPARK_FALLBACK=true` (Strict fail loudly, no silent fallback to `local[*]`)
- **Input File:** `data/orders_1m_sample.csv` (1,000,000 data rows, 418.55 MB)
- **Partitions:** 8 Output Partitions (`repartition(8)`)

---

## Commands Executed

```powershell
# 1. Initialize MongoDB Collections & Indexes
python src/mongo_setup.py

# 2. Launch Spark Standalone Master + Worker on 127.0.0.1
powershell -ExecutionPolicy Bypass -File cluster/start_master.ps1

# 3. Verify Master & Worker ALIVE
# -> http://127.0.0.1:8080 (Worker Status: ALIVE)

# 4. Run Path A on 1,000,000 Records
powershell -ExecutionPolicy Bypass -File cluster/run_path_a.ps1 -InputFile "data/orders_1m_sample.csv"

# 5. Execute Idempotency Run 2 on same 1M records
powershell -ExecutionPolicy Bypass -File cluster/run_path_a.ps1 -InputFile "data/orders_1m_sample.csv"

# 6. Execute Real In-Place Update Test
python src/run_update_test.py
```

---

## Verification Results

### 1. Spark Master UI
- **URL:** `http://127.0.0.1:8080`
- **Worker Status:** ALIVE
- **Registered Workers:** 1 Alive (`worker-20260824202202-192.168.8.181-57676`)
- **Master URL:** `spark://127.0.0.1:7077`
- **Evidence:** [`reports/screenshots/01_master_worker_alive.png`](screenshots/01_master_worker_alive.png)

### 2. Spark Application & Executors UI
- **URL:** `http://127.0.0.1:4040`
- **Application ID:** `app-20260824202945-0000` (Raw Load) and `app-20260824203021-0001` (ELT)
- **Executor ID:** 0 (4 Cores, 2.0 GiB RAM, Status: RUNNING)
- **Evidence:** [`reports/screenshots/02_spark_application.png`](screenshots/02_spark_application.png), [`reports/screenshots/03_executors.png`](screenshots/03_executors.png), [`reports/screenshots/04_jobs_stages_tasks.png`](screenshots/04_jobs_stages_tasks.png)

### 3. Repartition & Physical Plan (`explain(True)`)
- Physical execution plan confirms `RoundRobinPartitioning(8)`:
```text
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=true
+- == Final Plan ==
   ResultQueryStage 1
   +- ShuffleQueryStage 0
      +- Exchange RoundRobinPartitioning(8), REPARTITION_BY_NUM, [plan_id=9]
         +- FileScan csv [order_id#0, ...]
```
- **Evidence:** [`reports/screenshots/05_repartition_explain.png`](screenshots/05_repartition_explain.png)

### 4. 1M Run Performance & Metrics
| Phase / Metric | Run 1 (Initial Load) | Run 2 (Idempotency Re-run) |
|---|:---:|:---:|
| **Rows Read** | 1,000,000 | 1,000,000 |
| **Raw Load Time** | 32.19 s | 36.27 s |
| **Raw Throughput** | 31,065.67 rows/s | 27,574.42 rows/s |
| **ELT Process Time** | 106.80 s | 126.11 s |
| **ELT Throughput** | 9,363.24 rows/s | 7,929.43 rows/s |
| **Input Partitions** | 1 | 1 |
| **Output Partitions** | 8 | 8 |
| **Actual Master** | `spark://127.0.0.1:7077` | `spark://127.0.0.1:7077` |
| **Valid Count** | 0 | 0 |
| **Corrected Count** | 858,599 | 858,599 |
| **Quarantine Count** | 141,401 | 141,401 |
| **Inserted Count** | 858,599 | **0 (Zero Duplicates)** |
| **Updated Count** | 0 | 0 |
| **Unchanged Count** | 0 | **858,599** |

---

## Idempotency & In-Place Update Verification

1. **Idempotency Proof:** Re-running the exact same 1M records resulted in `inserted_count = 0` and `unchanged_count = 858,599`. The total unique count in `orders_validated` remained strictly 858,599.
   - **Evidence:** [`reports/screenshots/09_idempotency_run1.png`](screenshots/09_idempotency_run1.png), [`reports/screenshots/10_idempotency_run2.png`](screenshots/10_idempotency_run2.png)
2. **In-Place Update Proof:** Modifying order `1011692` updated the record in-place with `updated_count = 1` and `inserted_count = 0`.
   - **Evidence:** [`reports/screenshots/11_update_evidence.png`](screenshots/11_update_evidence.png)

---

## Conclusion

Path A execution has been fully verified on a single machine under Spark Standalone mode with:
- 1,000,000 data rows processed.
- Master and Worker running on `127.0.0.1`.
- Complete elimination of fallback to `local[*]`.
- 100% Idempotent Upsert and audit trail tracking in MongoDB.
- All 11 screenshots captured and verified.
