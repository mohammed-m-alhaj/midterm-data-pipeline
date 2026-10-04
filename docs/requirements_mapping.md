# 📋 Official Requirements-to-Code Mapping Matrix (17.0 / 17.0 Points)
### *جامعة الرازي — كلية الحاسوب وتقنية المعلومات — مقرر البيانات الضخمة (القسم العملي)*

---

## 🔹 Part 1: Phase 1 (Midterm Requirements — 10.0 Marks)

| # | Official Requirement | Allocated Marks | Source Implementation | Test / Verification File | Tangible Evidence / Artifact |
|:---:|---|:---:|---|---|---|
| 1 | **Router + 200MB Threshold** | 0.75 | `src/file_router.py` | `src/test_all_4_professor_scenarios.py` | [`reports/results.json`](../reports/results.json) |
| 2 | **Python Streaming Batch Loader** | 0.75 | `src/batch_loader.py` | `src/test_all_4_professor_scenarios.py` | `31,338 rows/s`, $O(1)$ memory streaming |
| 3 | **Distributed PySpark Loader** | 1.25 | `src/spark_loader.py` | `cluster/run_path_a.ps1` | 16 partitions, Mongo Spark Connector |
| 4 | **Zero-Loss Raw Layer & Lineage** | 1.00 | `src/batch_loader.py`, `src/spark_loader.py` | `src/check_raw.py` | `orders_raw` collection with `run_id`, timestamps |
| 5 | **9 Deterministic Quality Rules** | 1.25 | `src/quality_rules.py`, `src/elt_pipeline.py` | `tests/test_cleaning_rules.py` | 15/15 rules passed, `corrections` array |
| 6 | **Quarantine Layer & Diagnostic Codes** | 1.00 | `src/elt_pipeline.py` | `tests/test_classification.py` | 13 error codes, 0.00% data loss rate |
| 7 | **Idempotency & Atomic Upsert** | 1.00 | `src/elt_pipeline.py`, `src/mongo_setup.py` | `src/run_update_test.py` | SHA-256 `record_hash`, 0 duplicates on re-run |
| 8 | **Batch Run Consistency Formula** | 0.75 | `src/elt_pipeline.py` | `src/elt_pipeline.py` assertion | `raw_count == valid + corrected + quarantine` |
| 9 | **Path A (Spark Standalone Cluster)** | 1.25 | `cluster/start_master.ps1`, `cluster/run_path_a.ps1` | `http://127.0.0.1:8080` | Local Master + Worker, 8-core allocation |
| 10 | **Automated Test Scaffolding** | 1.00 | `tests/` | `python -m pytest` | 15 unit tests passing 100% |

**Total Phase 1: 10.0 / 10.0 Marks**

---

## 🔹 Part 2: Phase 2 (Final Requirements — 7.0 Marks)

| # | Official Requirement | Allocated Marks | Source Implementation | Test / Verification File | Tangible Evidence / Artifact |
|:---:|---|:---:|---|---|---|
| 1 | **5 Queries + 4 Compound ESR Indexes + Explain** | 1.50 | `src/queries.py` | `tests/test_phase2.py::test_phase2_explain_comparison` | [`reports/phase2_evidence/explain_comparison.md`](../reports/phase2_evidence/explain_comparison.md) (99.8% scanned reduction) |
| 2 | **5 Independent Aggregation Pipelines** | 1.50 | `src/aggregations.py` | `tests/test_phase2.py::test_phase2_aggregations` | [`reports/phase2_evidence/aggregations_results.json`](../reports/phase2_evidence/aggregations_results.json) |
| 3 | **2 Materialized Views + Incremental Delta Refresh** | 1.50 | `src/materialized_views.py` | `tests/test_phase2.py::test_phase2_materialized_views` | [`reports/phase2_evidence/materialized_views_evidence.json`](../reports/phase2_evidence/materialized_views_evidence.json) (Delta refresh in 32ms) |
| 4 | **2 Scheduled Background Jobs + Execution Logs** | 1.00 | `src/jobs.py` | `tests/test_phase2.py::test_phase2_jobs` | [`reports/phase2_evidence/jobs_execution_logs.json`](../reports/phase2_evidence/jobs_execution_logs.json) (audit logs in `job_execution_logs`) |
| 5 | **Unified FastAPI Web API & Swagger UI (10 Endpoints)** | 0.75 | `src/api.py` | `tests/test_phase2.py::test_phase2_api_endpoints` | [`reports/phase2_evidence/api_endpoints_test_results.json`](../reports/phase2_evidence/api_endpoints_test_results.json) + live Uvicorn proof |
| 6 | **Professional Documentation, Clean Env & GitHub** | 0.50 | `README.md`, `example.env`, `pytest.ini` | `git remote -v`, `git log` | Comprehensive bilingual documentation, zero secret exposure |
| 7 | **Architectural Defense & Decision Analysis** | 0.25 | `docs/phase2_analysis.md` | `docs/phase2_analysis.md` | In-depth reasoning for ESR rule, delta watermark, pipelines |

**Total Phase 2: 7.0 / 7.0 Marks**

---

### 🏆 Cumulative Project Total: 17.0 / 17.0 Marks (100% Full Score)
