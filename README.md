# Flight Delay Big Data Analysis

This repository contains a Big Data project based on the 2024 Flight Delay Dataset.  
The goal is to compare different Big Data technologies for analytical processing over a large real-world dataset.

The project uses three technologies:

- Spark SQL
- Spark Core / RDD
- Hive

The experiments were executed both locally and on an AWS EMR cluster.

---

## 1. Dataset

The project uses the 2024 Flight Delay Dataset from Kaggle.

The original dataset is not included in this repository because of its size.  
It must be downloaded manually and placed in:

```text
data/raw/flight_data_2024.csv
```

The dataset contains more than 7 million flight records and includes information about:

- airlines;
- departure and arrival airports;
- flight dates;
- departure and arrival delays;
- cancellations;
- cancellation causes;
- delay causes.

---

## 2. Implemented Analyses

Two analyses were implemented.

### 2.1 Analysis 3.1 — Airline Statistics

For each airline and departure airport or route, this analysis computes:

- number of flights;
- minimum arrival delay;
- maximum arrival delay;
- average arrival delay;
- cancellation rate;
- months in which the airline operates.

Implemented with:

- Spark SQL
- Spark Core / RDD
- Hive

---

### 2.2 Analysis 3.2 — Delay Report by Airport and Month

For each departure airport and month, this analysis computes:

- number of flights with low delay: less than 15 minutes;
- number of flights with medium delay: between 15 and 60 minutes;
- number of flights with high delay: more than 60 minutes;
- average departure delay;
- average arrival delay;
- most frequent delay or cancellation causes.

Implemented with:

- Spark SQL
- Spark Core / RDD
- Hive

---

## 3. Repository Structure

```text
flight_delay_project/
├── README.md
├── requirements.txt
├── docker-compose.yml
├── .gitignore
│
├── src/
│   ├── data_preparation.py
│   ├── benchmark.py
│   ├── analysis_spark.py
│   ├── analysis_spark_2.py
│   ├── analysis_rdd_3_1.py
│   ├── analysis_rdd_3_2.py
│   ├── analysis_hive.sql
│   ├── create_hive_tables.sql
│   ├── create_hive_tables_emr.sql
│   ├── analysis_hive_3_1_emr.sql
│   ├── analysis_hive_3_2_emr.sql
│   ├── prepare_hive_text_data.py
│   └── generate_charts.py
│
├── scripts/
│   ├── run_local.sh
│   └── run_cluster.sh
│
├── results/
│   ├── metrics/
│   │   ├── execution_times_local.csv
│   │   ├── cluster_execution_times.csv
│   │   ├── hive_cluster_execution_times_clean.csv
│   │   └── execution_times_all.csv
│   └── samples/
│       ├── hive_3_1_head.txt
│       ├── hive_3_2_head.txt
│       ├── spark_sql_3_1_50pct_head.csv
│       └── spark_sql_3_1_100pct_head.csv
|
├── tools/
│   ├── build_execution_times_all.py
│   └── extract_results_samples.py
│
├── report/
│   ├── figures/
│   └── paper.tex
│
└── docs/
    └── AWS_EMR_GUIDE.md
```

Large input files, processed Parquet files, temporary files, and full raw outputs are not tracked by Git.

---

## 4. Data Preparation

The data preparation step is implemented in:

```text
src/data_preparation.py
```

It performs the following operations:

- reads the original CSV file;
- selects the columns needed for the analyses;
- normalizes airline and airport codes;
- casts delay columns to numeric types;
- casts `cancelled` to integer;
- converts `fl_date` to date type;
- removes incomplete or non-informative records;
- writes the cleaned dataset in Parquet format.

Local output:

```text
data/processed/flights_cleaned.parquet/
```

Cluster output in S3:

```text
s3://flight-delay-project-juanmi-2026/data/processed/flights_cleaned.parquet/
```

---

## 5. Local Execution

### 5.1 Install Dependencies

```bash
pip install -r requirements.txt
```

Required software:

- Python 3.x
- PySpark
- Java runtime compatible with Spark
- Docker, only for local Hive execution

---

### 5.2 Prepare the Dataset

Place the raw CSV file in:

```text
data/raw/flight_data_2024.csv
```

Then run:

```bash
python src/data_preparation.py
```

This creates:

```text
data/processed/flights_cleaned.parquet/
```

---

### 5.3 Run the Local Benchmark

```bash
python src/benchmark.py
```

The benchmark executes Spark SQL and Spark Core/RDD analyses over different input fractions.

The local metrics are written to:

```text
data/results/metrics/execution_times.csv
```

For the final report, the local metrics are copied to:

```text
results/metrics/execution_times_local.csv
```

---

### 5.4 Run Individual Spark Jobs

Spark SQL analysis 3.1:

```bash
python src/analysis_spark.py
```

Spark SQL analysis 3.2:

```bash
python src/analysis_spark_2.py
```

Spark Core/RDD analysis 3.1:

```bash
python src/analysis_rdd_3_1.py 1.0
```

Spark Core/RDD analysis 3.2:

```bash
python src/analysis_rdd_3_2.py 1.0
```

The final argument represents the input fraction.

Examples:

```bash
python src/analysis_rdd_3_1.py 0.25
python src/analysis_rdd_3_1.py 0.50
python src/analysis_rdd_3_1.py 1.0
```

---

## 6. Local Hive Execution

Hive can be executed locally using Docker.

First prepare the Hive-compatible input:

```bash
python src/prepare_hive_text_data.py
```

Start the Docker environment:

```bash
docker compose up -d
```

Create the Hive tables:

```bash
docker exec -it hive-server hive -f /opt/hive/scripts/create_hive_tables.sql
```

Run the Hive analyses:

```bash
docker exec -it hive-server hive -f /opt/hive/scripts/analysis_hive.sql
```

The container name may vary depending on the Docker Compose configuration.

---

## 7. AWS EMR Cluster Execution

The cluster execution was performed with the following configuration:

| Component | Value |
|---|---|
| AWS service | Amazon EMR |
| EMR release | emr-6.15.0 |
| Spark version | Spark 3.4.1 |
| Hive version | Hive 3.1.3 |
| Hadoop version | Hadoop 3.3.6 |
| Primary node | m4.large |
| Core node | m4.large |
| Cluster size | 1 primary + 1 core |
| Storage | Amazon S3 |
| Region | us-east-1 |

The S3 bucket used in this experiment was:

```text
s3://flight-delay-project-juanmi-2026
```

The S3 structure was:

```text
s3://flight-delay-project-juanmi-2026/
├── data/raw/flight_data_2024.csv
├── data/processed/flights_cleaned.parquet/
├── src/
├── scripts/
└── results/
```

---

### 7.1 Run the Cluster Script

After connecting to the EMR primary node:

```bash
BUCKET=s3://flight-delay-project-juanmi-2026

aws s3 cp $BUCKET/scripts/run_cluster.sh /tmp/run_cluster.sh
sed -i 's/\r$//' /tmp/run_cluster.sh
chmod +x /tmp/run_cluster.sh

CLUSTER_SIZE="1-primary-1-core" bash /tmp/run_cluster.sh $BUCKET
```

The script:

1. downloads the source files from S3;
2. runs the Spark data preparation job;
3. converts the raw CSV dataset to Parquet;
4. runs Spark SQL and Spark Core/RDD benchmarks;
5. writes Spark results and metrics to S3;
6. creates the Hive external table over the Parquet dataset;
7. runs the Hive analyses;
8. writes Hive results and metrics to S3.

More detailed instructions are provided in:

```text
docs/AWS_EMR_GUIDE.md
```

---

## 8. Hive Schema Note

During the EMR execution, Hive required a schema correction for the `fl_date` column.

Spark wrote `fl_date` as a Parquet `DATE`, so the Hive external table must declare:

```sql
fl_date DATE
```

The Hive view extracts the month using:

```sql
MONTH(fl_date) AS month
```

This avoids runtime errors related to `DateWritableV2`.

---

## 9. Results

The repository contains selected metrics and sample outputs used in the final report.

Main metric files:

```text
results/metrics/execution_times_local.csv
results/metrics/cluster_execution_times.csv
results/metrics/hive_cluster_execution_times_clean.csv
results/metrics/execution_times_all.csv
```

Sample outputs are stored in:

```text
results/samples/
├── hive_3_1_head.txt
├── hive_3_2_head.txt
├── spark_sql_3_1_50pct_head.csv
└── spark_sql_3_1_100pct_head.csv
```

Full generated outputs, Parquet files, temporary files, and the original dataset are not committed to GitHub.

---

## 10. Generate Charts

After preparing the final metrics file:

```text
results/metrics/execution_times_all.csv
```

generate charts with:

```bash
python src/generate_charts.py
```

Charts are written to:

```text
report/figures/
```

---

## 11. Experimental Setup

The experiments compare local execution and EMR cluster execution.

Input sizes:

- 25% of the dataset;
- 50% of the dataset;
- 100% of the dataset, when available.

Technologies compared:

- Spark SQL
- Spark Core / RDD
- Hive

The comparison focuses on:

- execution time;
- expressiveness;
- ease of implementation;
- scalability;
- impact of shuffle and aggregation;
- differences between local and cluster execution.

---

## 12. Final Report

The final report is located in:

```text
report/paper.tex
```

The report includes:

- description of the data preparation process;
- implementation details for Spark SQL, Spark Core/RDD, and Hive;
- first 10 rows of the produced results;
- local vs cluster execution time comparison;
- charts;
- discussion of expressiveness, efficiency, scalability, shuffle, aggregation, and data preparation costs.

---

## 13. Reproducibility

To reproduce the project:

1. Download the 2024 Flight Delay Dataset from Kaggle.
2. Place the CSV file in `data/raw/flight_data_2024.csv`.
3. Install dependencies with `pip install -r requirements.txt`.
4. Run `src/data_preparation.py`.
5. Run the local benchmark with `src/benchmark.py`.
6. Optionally run Hive locally using Docker.
7. Upload the dataset and source files to S3.
8. Create an EMR cluster.
9. Run `scripts/run_cluster.sh` on the EMR primary node.
10. Collect the results from S3.
11. Generate charts.
12. Compile the final report.

---

## 14. Deliverables

The final submission consists of:

- final PDF report;
- GitHub repository;
- source code;
- execution scripts;
- selected metrics and sample outputs;
- documentation needed to reproduce the solution.