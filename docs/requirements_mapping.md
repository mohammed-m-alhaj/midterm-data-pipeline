# Requirement-to-code map

| Brief requirement | Implementation |
|---|---|
| Threshold 200 MB | `config/settings.py`, `src/file_router.py` |
| One Router | `src/main.py` |
| Reproducible sample | `src/create_small_sample.py` |
| Python streaming batch | `src/batch_loader.py` |
| Batch metrics and errors | `src/batch_loader.py` |
| Spark DataFrame + fixed Schema | `src/spark_loader.py` |
| MongoDB Connector | `src/spark_loader.py`, `src/elt_pipeline.py` |
| Raw metadata | `src/batch_loader.py`, `src/spark_loader.py` |
| 8+ corrections | `src/elt_pipeline.py`, `src/quality_rules.py` |
| Trail Audit | `corrections` array in `src/elt_pipeline.py` |
| Quarantine + reasons | `orders_quarantine`, `error_codes`, `error_details` |
| Stable Business Key | `order_id` |
| Unique Index | `src/mongo_setup.py` |
| Upsert | `src/elt_pipeline.py` connector options |
| Idempotency | `record_hash` + unchanged/update/insert metrics |
| Run consistency | assertion in `src/elt_pipeline.py` |
| Metrics JSON | `src/metrics.py` |
| Tests | `tests/` |
| Path A | `cluster/` + `docs/path_a.md` |
