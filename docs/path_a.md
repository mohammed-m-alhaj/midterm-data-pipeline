# Path A — Spark Standalone (Single Machine)

## Overview

Path A demonstrates Spark running in **Standalone Cluster mode** on a single
machine.  Master and Worker both bind to `127.0.0.1`, and the PySpark job
connects via `spark://127.0.0.1:7077` — **not** `local[*]`.

This proves the pipeline can use Spark's cluster manager, task scheduler, and
executor lifecycle even when all processes share one host.

> **Note:** Running Master + Worker on the same machine is a valid
> demonstration of Spark Standalone mode, but it does **not** satisfy the
> official brief's requirement of two separate physical/virtual machines.
> This setup is used here with explicit approval from the instructor.

## Architecture

```text
Single Machine (127.0.0.1)
│
├── Spark Master         :7077
├── Spark Worker         (registered with Master)
├── Spark Master UI      :8080
├── Spark Application UI :4040  (during job execution)
├── PySpark Job           (spark-submit → spark://127.0.0.1:7077)
└── MongoDB              :27017
```

## Prerequisites

```text
Python   3.11.x
Java     17.x
Spark    4.0.0+  (installed via pip: pyspark)
MongoDB  (running on 127.0.0.1:27017)
MongoDB Spark Connector  11.1.0
```

Set `SPARK_HOME` or let the scripts auto-detect it via `pyspark`.

## Worker Resource Issue

By default, Spark Worker reports all available RAM on the machine minus 1 GB.
If `--executor-memory` in `spark-submit` exceeds the Worker's available
memory, jobs will hang with:

```text
WARN TaskSchedulerImpl: Initial job has not accepted any resources
```

**Fix:** Ensure `--executor-memory` + `--driver-memory` ≤ Worker available
memory. The scripts default to `2g` each. If your machine has limited RAM,
reduce these values.

You can also explicitly set Worker memory:

```bash
# Linux
SPARK_WORKER_MEMORY=4g ./cluster/start_worker.sh

# Windows — Worker memory is auto-detected by Spark
```

## Writable Work Directory

Spark Worker needs a writable work directory for shuffle files and executor
logs. The Linux scripts use `.spark-runtime/worker` (added to `.gitignore`).
On Windows, Spark uses `%TEMP%` by default.

If you see permission errors, set `SPARK_WORKER_DIR`:

```bash
export SPARK_WORKER_DIR=/tmp/spark-worker
```

---

## Running on Windows

### Step 1 — Start Master + Worker

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass -Force
.\cluster\start_master.ps1
```

This launches **both** Master and Worker on `127.0.0.1`.

Verify at `http://127.0.0.1:8080` → Worker = **ALIVE**.

> `cluster/start_worker.ps1` is **not needed** for single-machine setup.

### Step 2 — Run Path A

```powershell
.\cluster\run_path_a.ps1 -InputFile "data/orders_500k_sample.csv"
```

---

## Running on Linux

### Step 1 — Start Master

```bash
chmod +x cluster/*.sh
./cluster/start_master.sh
```

### Step 2 — Start Worker

```bash
./cluster/start_worker.sh
```

### Step 3 — Verify

Open `http://127.0.0.1:8080` → Worker = **ALIVE**.

### Step 4 — Run Path A

```bash
./cluster/run_path_a.sh 127.0.0.1 7077 data/orders_500k_sample.csv
```

---

## Fallback Safety

Both `run_path_a.ps1` and `run_path_a.sh` set:

```text
PIPELINE_DISABLE_SPARK_FALLBACK=true
```

If the Master is unreachable, the pipeline **fails immediately** instead of
silently falling back to `local[*]`.

The fallback decision chain in both `spark_loader.py` and `elt_pipeline.py`:

1. `DISABLE_SPARK_FALLBACK=true` → **fail loudly**
2. `ALLOW_SPARK_LOCAL_FALLBACK=false` (default) → **fail with clear error**
3. `ALLOW_SPARK_LOCAL_FALLBACK=true` → fall back to `local[*]` (dev only)

## Evidence: explain(True)

The pipeline prints `explain(True)` after `repartition()`. Look for:

```text
== Physical Plan ==
Exchange RoundRobinPartitioning(8), ...
+- FileScan csv [order_id#0, ...] ...
```

This proves Spark physically redistributes data across partitions.

## Collect Evidence

| Evidence | Source |
|---|---|
| Worker ALIVE | Master UI `http://127.0.0.1:8080` |
| Jobs / Stages / Tasks | Application UI `http://127.0.0.1:4040` |
| `explain(True)` plan | Console output during run |
| Metrics | `reports/results.json` |
| MongoDB data | `orders_raw`, `orders_validated`, `orders_quarantine` |
| Idempotency | Re-run same file → `inserted_count=0` |

## Required Evidence Checklist

```text
[ ] Master UI — Worker ALIVE
[ ] Application UI — Jobs, Stages, Tasks, Executors
[ ] explain(True) output showing Exchange / RepartitionByExpression
[ ] 500k+ rows processed (console + results.json)
[ ] elapsed_seconds, throughput, partitions in results.json
[ ] actual_spark_master = spark://127.0.0.1:7077 (NOT local[*])
[ ] MongoDB collections populated (raw, validated, quarantine)
[ ] Idempotency: insert → no-change → update
```
