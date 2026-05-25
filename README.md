# Flight Delay Analysis — Big Data Project

**Roma Tre University · Big Data Course · Second Project**

This repository contains a Big Data analysis project based on the **2024 Flight Delay Dataset** from Kaggle.  
The goal of the project is to compare different Big Data technologies for processing and analysing a real dataset with more than 7 million flight records.

The implemented technologies are:

- Apache Spark SQL
- Apache Spark Core / RDD API
- Apache Hive 4.1.0 via Docker

The implemented analyses are:

| Analysis | Spark SQL | Spark Core / RDD | Hive |
|---|---:|---:|---:|
| 3.1 Airline statistics | ✅ | ✅ | ✅ |
| 3.2 Delay report by airport and month | ✅ | ✅ | ✅ |
| 3.3 Anomalous delay ranking | — | — | ✅ |

Analysis 3.3 is included as an additional Hive extension, but the main required analyses are 3.1 and 3.2.

---

## Dataset

The project uses the **2024 Flight Delay Dataset** from Kaggle.

Dataset page:

```text
https://www.kaggle.com/datasets/hrishitpatil/flight-data-2024

The original dataset contains more than 7 million records and 35 columns with information about:

flight dates
airlines
origin and destination airports
departure and arrival delays
cancellations
diversions
delay causes

Because the raw dataset is large, it is not included in this repository.
After downloading it from Kaggle, place it at:

data/raw/flight_data_2024.csv
Project structure
flight_delay_project/
│
├── data/
│   ├── raw/
│   │   └── flight_data_2024.csv              # Original Kaggle dataset, not tracked by Git
│   │
│   ├── processed/
│   │   └── flights_cleaned.parquet           # Cleaned dataset for Spark, generated locally
│   │
│   ├── hive/
│   │   └── flights_clean/
│   │       └── flights_clean_hive.psv        # Hive-ready text file, generated locally
│   │
│   └── results/
│       ├── spark_sql/                        # Spark SQL outputs
│       ├── spark_core/                       # Spark Core / RDD outputs
│       ├── hive/                             # Hive outputs
│       └── metrics/
│           └── execution_times.csv           # Benchmark results
│
├── scripts/
│   ├── run_local.sh                          # Local execution script
│   └── run_cluster.sh                        # Template for cluster execution
│
├── src/
│   ├── data_preparation.py                   # Cleans the original CSV and writes Parquet
│   ├── prepare_hive_text_data.py             # Converts cleaned data into Hive-compatible PSV
│   │
│   ├── analysis_spark.py                     # Spark SQL implementation of analysis 3.1
│   ├── analysis_spark_2.py                   # Spark SQL implementation of analysis 3.2
│   │
│   ├── analysis_rdd_3_1.py                   # Spark Core / RDD implementation of analysis 3.1
│   ├── analysis_rdd_3_2.py                   # Spark Core / RDD implementation of analysis 3.2
│   │
│   ├── create_hive_tables.sql                # Hive database and table creation script
│   ├── analysis_hive.sql                     # Hive implementations of analyses 3.1, 3.2 and 3.3
│   │
│   └── benchmark.py                          # Execution-time benchmark for Spark SQL and Spark Core
│
├── docker-compose.yml                        # Hive Docker environment
├── requirements.txt                          # Python dependencies
├── .gitignore
└── README.md
Technologies used
Apache Spark SQL

Spark SQL is used to implement the analyses through high-level DataFrame and SQL operations.

It is used for:

grouping flights by airline and departure airport
computing delay statistics
computing cancellation rates
grouping flights by airport, month and delay range
computing average departure and arrival delays
identifying the most frequent delay or cancellation causes
Apache Spark Core / RDD API

Spark Core is used to implement the same analyses with lower-level RDD transformations.

The RDD implementations use:

map
reduceByKey
leftOuterJoin
groupByKey
custom Python mapping and reduction functions

The RDD version is less concise than Spark SQL but allows a more explicit implementation of the MapReduce-style logic.

Apache Hive

Hive is used through a Docker-based environment.

Hive is used to implement:

analysis 3.1
analysis 3.2
an additional analysis 3.3

The input data for Hive is stored as a pipe-separated text file.

Implemented analyses
Analysis 3.1 — Airline statistics

This analysis generates statistics for each airline and departure airport.

For each pair:

airline, departure airport

the output contains:

airline code
departure airport
total number of flights
minimum arrival delay
maximum arrival delay
average arrival delay
cancellation rate
list of months in which the airline operated at that airport

Implemented in:

src/analysis_spark.py
src/analysis_rdd_3_1.py
src/analysis_hive.sql
Analysis 3.2 — Delay report by airport and month

This analysis generates a delay report for each departure airport and month.

Flights are classified into three departure-delay ranges:

Category	Definition
Low delay	departure delay < 15 minutes
Medium delay	15 <= departure delay <= 60 minutes
High delay	departure delay > 60 minutes

For each airport, month and delay category, the output contains:

departure airport
month
delay category
number of flights
average departure delay
average arrival delay
top 3 most frequent causes of delay or cancellation, when available

Implemented in:

src/analysis_spark_2.py
src/analysis_rdd_3_2.py
src/analysis_hive.sql
Analysis 3.3 — Anomalous delay ranking

This is an additional Hive analysis.

It compares the behaviour of each airline at each departure airport with the average behaviour of all airlines operating at the same airport.

For each airport-airline pair, the output includes:

number of flights
average departure delay
average arrival delay
cancellation rate
difference between airline average departure delay and airport average departure delay
ranking of airlines at each airport based on average departure delay

Implemented in:

src/analysis_hive.sql
Data preparation

Before running the analyses, the dataset is cleaned and transformed.

The preparation step includes:

reading the original CSV dataset
selecting the columns required by the implemented analyses
normalising delay-related numeric columns
handling missing values in delay and cancellation fields
preserving cancelled flights, since they are needed to compute cancellation rates
saving a cleaned Parquet version for Spark
generating a pipe-separated text file for Hive

Run:

python src/data_preparation.py

This creates:

data/processed/flights_cleaned.parquet

Then prepare the Hive input file:

python src/prepare_hive_text_data.py

This creates:

data/hive/flights_clean/flights_clean_hive.psv
Installation

Create and activate a Python environment.

Example with Conda:

conda create -n flight_project python=3.10 -y
conda activate flight_project

Install the required dependencies:

pip install -r requirements.txt

The recommended requirements.txt is:

pyspark==3.4.1
kaggle
psutil
matplotlib
pandas

On Windows, Hadoop compatibility files may be required.
The project assumes the following local configuration:

HADOOP_HOME=C:\hadoop
C:\hadoop\bin added to PATH
Running Spark SQL analyses

From the project root:

python src/analysis_spark.py
python src/analysis_spark_2.py

Generated outputs:

data/results/3_1_airline_statistics
data/results/3_2_delay_report
data/results/3_2_delay_causes_sql
Running Spark Core / RDD analyses

The RDD scripts accept an optional input fraction.

Examples:

python src/analysis_rdd_3_1.py 0.25
python src/analysis_rdd_3_1.py 0.50
python src/analysis_rdd_3_1.py 1.0
python src/analysis_rdd_3_2.py 0.25
python src/analysis_rdd_3_2.py 0.50
python src/analysis_rdd_3_2.py 1.0

If no fraction is provided, the scripts use a default sample fraction.

Outputs:

data/results/spark_core/3_1/
data/results/spark_core/3_2/
Running Hive analyses

The Hive environment is executed through Docker.

Start the Hive container:

docker compose up -d

Copy or mount the Hive input data so that the file is available at the path expected by the external Hive table:

/opt/hive/data/ext/flights_clean/

Create the Hive database and external table:

docker exec -it hive-server beeline -u jdbc:hive2://localhost:10000 -f /opt/hive/scripts/create_hive_tables.sql

Run the Hive analyses:

docker exec -it hive-server beeline -u jdbc:hive2://localhost:10000 -f /opt/hive/scripts/analysis_hive.sql

The exact container name may vary depending on the Docker Compose configuration.

Hive outputs are stored under:

data/hive/results/
Running the benchmark

The benchmark compares Spark SQL and Spark Core / RDD using increasing input sizes.

The tested input fractions are:

25%
50%
100%

Run:

python src/benchmark.py

The benchmark measures:

Technology	Analysis
Spark SQL	3.1
Spark SQL	3.2
Spark Core / RDD	3.1
Spark Core / RDD	3.2

The execution times are saved in:

data/results/metrics/execution_times.csv

Example output:

Technology,Analysis,Input_Size,Time_s
Spark SQL,3.1,25%,3.44
Spark SQL,3.2,25%,5.15
Spark Core,3.1,25%,17.39
Spark Core,3.2,25%,43.90
Spark SQL,3.1,50%,1.93
Spark SQL,3.2,50%,3.58
Spark Core,3.1,50%,27.22
Spark Core,3.2,50%,58.68
Spark SQL,3.1,100%,3.28
Spark SQL,3.2,100%,5.46
Spark Core,3.1,100%,44.70
Spark Core,3.2,100%,91.65

The results show that Spark SQL is significantly faster than Spark Core / RDD in the local environment, mainly because Spark SQL benefits from query optimisation, JVM execution and a more efficient physical plan, while PySpark RDD jobs require Python row-level functions and additional serialization overhead.

Local execution notes

All experiments were executed in local mode.

The Spark configuration used in the RDD and benchmark scripts includes:

local[2]
driver memory: 12g
executor memory: 4g
shuffle partitions: 8
default parallelism: 8

The experiments were performed by varying the input size through random sampling of the original dataset:

25%
50%
100%

This allows evaluating how each technology behaves as the amount of input data increases.

Cluster execution

The project includes placeholder scripts for possible cluster execution:

scripts/run_cluster.sh

The project specification encourages running experiments both locally and on a cluster when possible.
However, AWS or any specific cloud provider is not mandatory unless explicitly required by the instructor.

A possible cluster execution could be performed using:

AWS EMR
Google Dataproc
Databricks
a university cluster
a manually configured Spark cluster

The local benchmark results are included and discussed in the final report.
Cluster execution can be added later using the same Spark scripts and adapting the execution command to spark-submit.

Example cluster-style command:

spark-submit \
  --master yarn \
  --deploy-mode cluster \
  src/benchmark.py

The exact command depends on the cluster manager and deployment environment.

Reproducibility steps

To reproduce the full local workflow:

Clone the repository.
git clone <repository-url>
cd flight_delay_project
Create the Python environment.
conda create -n flight_project python=3.10 -y
conda activate flight_project
pip install -r requirements.txt
Download the dataset from Kaggle and place it at:
data/raw/flight_data_2024.csv
Prepare the dataset.
python src/data_preparation.py
python src/prepare_hive_text_data.py
Run Spark SQL analyses.
python src/analysis_spark.py
python src/analysis_spark_2.py
Run Spark Core / RDD analyses.
python src/analysis_rdd_3_1.py 1.0
python src/analysis_rdd_3_2.py 1.0
Run the benchmark.
python src/benchmark.py
Start Hive with Docker and run Hive scripts.
docker compose up -d

Then execute:

docker exec -it hive-server beeline -u jdbc:hive2://localhost:10000 -f /opt/hive/scripts/create_hive_tables.sql
docker exec -it hive-server beeline -u jdbc:hive2://localhost:10000 -f /opt/hive/scripts/analysis_hive.sql
Outputs

Main generated outputs:

data/results/spark_sql/3_1/
data/results/spark_sql/3_2/
data/results/spark_core/3_1/
data/results/spark_core/3_2/
data/hive/results/
data/results/metrics/execution_times.csv

Large generated files are ignored by Git and can be recreated by running the scripts.

Notes on Git tracking

The raw dataset and generated intermediate data are not tracked by Git because of their size.

Ignored paths include:

data/raw/
data/processed/
data/hive/flights_clean/
data/results/

If the benchmark CSV is included in the repository, the .gitignore should allow:

data/results/metrics/execution_times.csv
Author

Juan Miguel
