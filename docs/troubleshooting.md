# Troubleshooting

## MongoDB Connector not found

The project uses:

`org.mongodb.spark:mongo-spark-connector_2.13:11.1.0`

Run the application through `spark-submit` for Path A. The same coordinate is also configured in the SparkSession for local IDE runs.

## HADOOP_HOME / winutils on Windows

Use the Windows Spark/Hadoop runtime already configured for the workstation. The assignment does not require Hadoop/HDFS/YARN for Path A; it requires Spark Standalone Cluster only.

## Java heap space during local quality/ELT

Do not run a full 12+ GB ELT pass on one laptop by accident. The project protects local execution using `PIPELINE_LOCAL_ELT_MAX_MB`. Use the 100k development sample or a 1M benchmark for local-vs-cluster comparison, and use Path A for the full distributed run.
