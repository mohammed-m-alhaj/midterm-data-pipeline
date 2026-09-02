# 🎬 Official Live Defense & Presentation Checklist

To demonstrate the pipeline seamlessly in one single step, run the interactive defense script:
```powershell
.\demo.ps1
```

Or execute and show each required demonstration step individually:

| # | Demonstration Step | Required Action / Proof | Command / Location |
|---|---|---|---|
| 1 | **Cluster & Environment Health** | Master + Worker ALIVE on `127.0.0.1:8080` | `http://127.0.0.1:8080` |
| 2 | **Small Sample Ingestion** | Router selects `python_batch` ($\le 200\text{ MB}$) with 0-memory streaming | `python src/main.py --file data/orders_small_sample.csv` |
| 3 | **Raw Ingestion Layer** | Show `orders_raw` with uncleaned data and lineage metadata | MongoDB `orders_raw` / `src/check_raw.py` |
| 4 | **Quality Classification** | Show Valid record, Corrected record with `corrections`, and Quarantine record with `error_codes` | MongoDB collections / [`reports/screenshots/`](../reports/screenshots/) |
| 5 | **Large Dataset (1M Records)** | Router selects `pyspark` ($> 200\text{ MB}$) on Spark Standalone | `python src/main.py --file data/orders_1m_sample.csv` |
| 6 | **Spark Cluster UI & Parallelism** | Show Jobs, Stages, 8 Tasks, Executors, and `explain(True)` with `RoundRobinPartitioning(8)` | `http://127.0.0.1:4040` / [`05_repartition_explain.png`](../reports/screenshots/05_repartition_explain.png) |
| 7 | **Idempotency Proof** | Re-run same 1M records $\rightarrow$ `inserted_count = 0`, `unchanged_count = 858,599` | `python src/main.py --file data/orders_1m_sample.csv` |
| 8 | **In-Place Update (Mutation)** | Modify existing order $\rightarrow$ `updated_count = 1`, 0 duplicates created | `python src/run_update_test.py` |
| 9 | **Automated Test Suite** | 33 automated PyTest unit & quality tests passed | `python -m pytest` |
| 10 | **Interactive Web Dashboard** | Live KPI cards, document inspector & quality visualizer | `python dashboard_server.py` $\rightarrow$ `http://localhost:8000` |
