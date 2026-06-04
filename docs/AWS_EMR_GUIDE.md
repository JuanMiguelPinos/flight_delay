# AWS EMR Execution Guide

This document describes how to reproduce the cluster execution of the Flight Delay Big Data project using AWS Academy Learner Lab, Amazon S3, and Amazon EMR.

---

## 1. Architecture

The cluster execution follows this architecture:

```text
Local machine / Learner Lab terminal
        |
        v
Amazon S3 bucket
        |
        v
Amazon EMR cluster
        |
        v
Results written back to Amazon S3
```

The dataset and the project scripts are uploaded to Amazon S3. The EMR cluster reads the input data from S3, executes Spark and Hive jobs, and writes the generated results back to S3.

---

## 2. S3 Bucket Structure

The project was executed using the following S3 bucket:

```text
s3://flight-delay-project-juanmi-2026
```

The S3 structure was:

```text
s3://flight-delay-project-juanmi-2026/
├── data/
│   ├── raw/
│   │   └── flight_data_2024.csv
│   └── processed/
│       └── flights_cleaned.parquet/
├── src/
│   ├── data_preparation.py
│   ├── benchmark.py
│   ├── create_hive_tables_emr.sql
│   ├── analysis_hive_3_1_emr.sql
│   └── analysis_hive_3_2_emr.sql
├── scripts/
│   └── run_cluster.sh
└── results/
    ├── spark_sql/
    ├── spark_core/
    ├── hive/
    └── metrics/
```

For a new execution, replace the bucket name with your own S3 bucket.

---

## 3. Upload Files to S3

From the Learner Lab terminal or from a local machine with AWS CLI configured:

```bash
BUCKET=s3://flight-delay-project-juanmi-2026

aws s3 cp data/raw/flight_data_2024.csv $BUCKET/data/raw/

aws s3 cp src/data_preparation.py        $BUCKET/src/
aws s3 cp src/benchmark.py               $BUCKET/src/
aws s3 cp src/create_hive_tables_emr.sql $BUCKET/src/
aws s3 cp src/analysis_hive_3_1_emr.sql  $BUCKET/src/
aws s3 cp src/analysis_hive_3_2_emr.sql  $BUCKET/src/

aws s3 cp scripts/run_cluster.sh         $BUCKET/scripts/
```

Check the uploaded files:

```bash
aws s3 ls $BUCKET --recursive
```

---

## 4. EMR Cluster Configuration

The EMR cluster was created from the AWS Console with the following configuration:

| Parameter | Value |
|---|---|
| EMR release | emr-6.15.0 |
| Applications | Hadoop, Spark, Hive |
| Primary node | m4.large |
| Core node | m4.large |
| Number of core nodes | 1 |
| Cluster size | 1 primary + 1 core |
| EC2 key pair | vockey |
| Service role | EMR_DefaultRole |
| EC2 instance profile | EMR_EC2_DefaultRole |
| Region | us-east-1 |

Cluster-specific logs to S3 were disabled to avoid permission issues in the AWS Academy Learner Lab environment.

After creating the cluster, wait until the state becomes:

```text
Waiting
```

---

## 5. Connect to the EMR Primary Node

From the Learner Lab terminal:

```bash
ssh -i ~/.ssh/labsuser.pem hadoop@<PRIMARY_PUBLIC_IP>
```

The public IP can be found in:

```text
EMR Console → Clusters → flight-delay-cluster → EC2 instances → Primary node
```

If the SSH command hangs, open port 22 in the security group associated with the EMR primary node:

```text
EC2 → Security Groups → ElasticMapReduce-master → Inbound rules → Add SSH rule
```

The SSH rule was opened temporarily only to access the cluster during the experiment.

---

## 6. Run the Cluster Script

Once connected to the EMR primary node:

```bash
BUCKET=s3://flight-delay-project-juanmi-2026

aws s3 cp $BUCKET/scripts/run_cluster.sh /tmp/run_cluster.sh
sed -i 's/\r$//' /tmp/run_cluster.sh
chmod +x /tmp/run_cluster.sh

CLUSTER_SIZE="1-primary-1-core" bash /tmp/run_cluster.sh $BUCKET
```

The script performs the following steps:

1. Downloads the Python and SQL files from S3.
2. Runs `data_preparation.py` with Spark.
3. Converts the raw CSV dataset into Parquet format.
4. Runs `benchmark.py` with Spark SQL and Spark Core/RDD.
5. Writes Spark results and metrics to S3.
6. Creates the Hive external table over the Parquet dataset.
7. Runs the Hive analyses.
8. Writes Hive results and metrics to S3.

---

## 7. Hive Schema Fix

During the EMR execution, Hive initially failed when reading the Parquet column `fl_date`.

The issue was caused by a schema mismatch:

- Spark wrote `fl_date` as a Parquet `DATE`.
- The Hive external table originally declared `fl_date` as `STRING`.

The corrected Hive schema is:

```sql
fl_date DATE
```

The Hive view extracts the month using:

```sql
MONTH(fl_date) AS month
```

This avoids runtime errors related to `DateWritableV2`.

---

## 8. Collect Results from S3

From the Learner Lab terminal or from a local machine with AWS CLI configured:

```bash
BUCKET=s3://flight-delay-project-juanmi-2026

mkdir -p collected_results/metrics
mkdir -p collected_results/hive_3_1
mkdir -p collected_results/hive_3_2
mkdir -p collected_results/spark_sql_3_1_50pct
mkdir -p collected_results/spark_sql_3_1_100pct

aws s3 cp $BUCKET/results/metrics/cluster_execution_times.csv collected_results/metrics/
aws s3 cp $BUCKET/results/metrics/hive_cluster_execution_times.csv collected_results/metrics/

aws s3 cp $BUCKET/results/hive/3_1/fraction_100pct/ collected_results/hive_3_1/ --recursive
aws s3 cp $BUCKET/results/hive/3_2/fraction_100pct/ collected_results/hive_3_2/ --recursive

aws s3 cp $BUCKET/results/spark_sql/3_1/fraction_50pct/ collected_results/spark_sql_3_1_50pct/ --recursive
aws s3 cp $BUCKET/results/spark_sql/3_1/fraction_100pct/ collected_results/spark_sql_3_1_100pct/ --recursive
```

Check the downloaded files:

```bash
find collected_results -type f
```

Optional compression:

```bash
tar -czf collected_results.tar.gz collected_results
```

---

## 9. Terminate the Cluster

After collecting the results, terminate the EMR cluster to avoid unnecessary cost:

```text
EMR Console → Clusters → flight-delay-cluster → Terminate
```

Do not delete the S3 bucket until the report and repository are finalized.

---

## 10. Troubleshooting

| Problem | Cause | Solution |
|---|---|---|
| SSH command hangs | Port 22 is not open | Open an SSH inbound rule in the `ElasticMapReduce-master` security group |
| `Cannot find hadoop installation` | Hive was executed outside the EMR node | SSH into the EMR primary node and run Hive there |
| Hive fails with `DateWritableV2` | `fl_date` is stored as `DATE` in Parquet but declared as `STRING` in Hive | Declare `fl_date DATE` and use `MONTH(fl_date)` |
| S3 path is empty | Wrong bucket prefix | Use the correct bucket root: `s3://flight-delay-project-juanmi-2026` |
| Spark job succeeds but metrics are missing | Metrics file was not uploaded automatically | Upload or reconstruct the metrics CSV manually |
| EMR cluster keeps consuming credits | Cluster was not terminated | Terminate the EMR cluster after collecting the results |