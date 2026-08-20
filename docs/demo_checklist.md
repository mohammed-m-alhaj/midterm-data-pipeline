# Demo checklist mapped to the brief

1. Small sample → Router chooses Python Batch.
2. Show `orders_raw` before cleaning.
3. Show one valid record, one corrected record with `corrections`, and one quarantine record with `error_codes`.
4. Large file → Router chooses PySpark.
5. Spark UI → Jobs / Stages / Tasks / Partitions / Executors.
6. Show `orders_validated`, `orders_quarantine`, and `reports/results.json`.
7. Show time / throughput / counts / error cases.
8. Re-run the same data → `orders_validated` count does not increase; unchanged count is reported.
9. Modify one existing order and run it again → update without duplicate.
10. Present Path A Master/Worker UI and local vs cluster comparison.
