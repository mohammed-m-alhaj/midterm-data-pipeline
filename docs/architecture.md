# Architecture

```text
Dirty CSV
   |
   v
File Router (<= 200 MB?)
   |----------------------|
   v                      v
Python Batch            PySpark
   |                      |
   +----------+-----------+
              v
         orders_raw
              |
              v
       Cleaning + Validation
          |          |
          |          +----------> orders_quarantine
          v
     Idempotent Upsert
          |
          v
   orders_validated
          |
          v
reports/results.json
```

## Raw layer contract

`orders_raw` is append-oriented history. No unique index or validator blocks raw ingestion. Every run receives a unique `run_id`. `record_raw` preserves the raw values as JSON; source metadata is stored alongside it.

## Final-state contract

`orders_validated` uses `order_id` as the stable business key. It has a unique index and uses the MongoDB Spark Connector's replace/upsert write semantics.

## Quarantine contract

A record that cannot be safely corrected is not dropped. It is written to `orders_quarantine` with `error_codes` and `error_details`.

## Consistency invariant

For every run:

`run_raw_count = run_valid_count + run_corrected_count + run_quarantine_count`
