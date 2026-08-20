# Path A — Spark Standalone Cluster

The official brief requires:

- Spark Master + at least one Worker on **two separate physical or virtual machines**.
- Application master URL `spark://MASTER_IP:7077`, not local mode.
- Same Java/Python/Spark/MongoDB Connector versions on nodes.
- Shared/network-accessible dataset path.
- Worker visible in Master UI and Executors/Tasks visible during execution.
- Real processing of at least 1,000,000 records.
- Local vs cluster timing comparison on the same input.

## Node layout

Example:

- Master: `192.168.1.10`
- Worker: `192.168.1.11`
- Spark Master URL: `spark://192.168.1.10:7077`

The full CSV must be accessible by both nodes through the **same path**, e.g. a shared network folder.

MongoDB must also be reachable from the nodes. If MongoDB is on the Master machine, configure MongoDB/network/firewall so the Worker can reach the MongoDB host and TCP 27017. For a production system, enable MongoDB authentication; the classroom brief does not require authentication.

## Required network ports

- Spark Master: TCP 7077
- Master UI: TCP 8080
- Worker UI: TCP 8081
- MongoDB: TCP 27017 (if MongoDB is remote)

## Version parity

Run the same versions on both nodes:

```text
Python 3.11.x
Java 17.x
Spark 4.2.0
MongoDB Spark Connector 11.1.0
PyMongo 4.17.x
```

## Master

Run `cluster/start_master.ps1` on the Master node.

Master UI: `http://MASTER_IP:8080`

## Worker

Run `cluster/start_worker.ps1 -MasterIp 192.168.1.10` on the Worker node.

Worker UI: `http://WORKER_IP:8081`

The Master UI must show the Worker as ALIVE before the graded run.

## One-million-row comparison

Create a reproducible 1,000,000-row sample from the same instructor CSV. Use exactly the same input for local and cluster runs.

1. Local: `PIPELINE_SPARK_MASTER=local[*]` and `PIPELINE_RUN_ELT_AFTER_RAW=false` for the benchmark pass.
2. Cluster: `spark://MASTER_IP:7077` through `cluster/run_path_a.ps1`.
3. Compare `elapsed_seconds`, `throughput`, and `partitions` in `reports/results.json`.

## Cluster run

From the project/shared-storage machine:

`cluster/run_path_a.ps1 -MasterIp 192.168.1.10 -InputFile \\\SERVER\share\orders_1m.csv`

For the final end-to-end run, point `-InputFile` to the instructor large CSV after the 1M acceptance evidence has been captured.

The script uses:

`spark://192.168.1.10:7077`

and the same connector coordinate:

`org.mongodb.spark:mongo-spark-connector_2.13:11.1.0`

## Required evidence

Capture:

1. Master UI with Worker ALIVE.
2. Spark UI showing Jobs, Stages, Tasks, Partitions and Executors.
3. Local 1M-record run time and throughput.
4. Cluster 1M-record run time and throughput.
5. `reports/results.json` entries for both runs.
6. Screenshot of `orders_validated` and `orders_quarantine`.
7. Idempotency rerun and one-record update evidence.
