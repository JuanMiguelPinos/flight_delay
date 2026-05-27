# Flight Delay Analysis — Big Data Project

**Roma Tre University · Big Data Course · Second Project · 2024–2025**

---

## Overview

This project implements and compares three Big Data technologies for processing and analysing the
2024 Flight Delay Dataset from Kaggle, which contains more than 7 million flight records and 35 columns.

The main objective is to evaluate the expressiveness, ease of implementation, efficiency and
scalability of Apache Spark SQL, Apache Spark Core (RDD API) and Apache Hive when applied to
the same analytical tasks on a real large-scale dataset.

---

## Technologies

| Technology | Version | Execution environment |
|---|---|---|
| Apache Spark SQL | 3.4.1 | Local (PySpark) |
| Apache Spark Core / RDD | 3.4.1 | Local (PySpark) |
| Apache Hive | 4.1.0 | Docker (apache/hive:4.1.0) |

---

## Analyses implemented

| Analysis | Spark SQL | Spark Core / RDD | Hive |
|---|---|---|---|
| 3.1 — Airline statistics | ✅ | ✅ | ✅ |
| 3.2 — Delay report by airport and month | ✅ | ✅ | ✅ |
| 3.3 — Anomalous delay ranking | — | — | ✅ |

Analysis 3.3 is implemented as an additional Hive extension beyond the minimum required.

---

## Dataset

**Source:** [2024 Flight Delay Dataset — Kaggle](https://www.kaggle.com/datasets/hrishitpatil/flight-data-2024)

The dataset contains more than **7,079,081 records** with information about flight dates, airlines,
origin and destination airports, departure and arrival delays, cancellations, diversions and delay causes.

Because the raw file exceeds 1 GB, it is not tracked by Git.
After downloading from Kaggle, place it at:
data/raw/flight_data_2024.csv

---

## Project structure
flight_delay_project/
│
├── data/
│   ├── raw/                                      # Original CSV — not tracked by Git
│   ├── processed/                                # Cleaned Parquet — not tracked by Git
│   ├── hive/
│   │   ├── flights_clean/                        # PSV file for Hive — not tracked by Git
│   │   └── results/                              # Hive query outputs
│   │       ├── 3_1/
│   │       ├── 3_2_ranges/
│   │       ├── 3_2_causes/
│   │       └── 3_3/
│   └── results/
│       ├── spark_sql/                            # Spark SQL benchmark outputs
│       │   ├── 3_1/
│       │   └── 3_2/
│       ├── spark_core/                           # Spark Core benchmark outputs
│       │   ├── 3_1/
│       │   └── 3_2/
│       └── metrics/
│           └── execution_times.csv              # Benchmark results — tracked by Git
│
├── report/
│   └── figures/                                  # Generated performance charts
│       ├── execution_time_by_technology.png
│       ├── scalability_analysis_3.1.png
│       └── scalability_analysis_3.2.png
│
├── scripts/
│   ├── run_local.sh                              # Full local execution script
│   └── run_cluster.sh                            # Template for cluster execution
│
├── src/
│   ├── data_preparation.py                       # Cleans CSV and writes Parquet
│   ├── prepare_hive_text_data.py                 # Converts cleaned data to PSV for Hive
│   │
│   ├── analysis_spark.py                         # Spark SQL — Analysis 3.1
│   ├── analysis_spark_2.py                       # Spark SQL — Analysis 3.2
│   │
│   ├── analysis_rdd_3_1.py                       # Spark Core / RDD — Analysis 3.1
│   ├── analysis_rdd_3_2.py                       # Spark Core / RDD — Analysis 3.2
│   │
│   ├── create_hive_tables.sql                    # Hive database and table creation
│   ├── analysis_hive.sql                         # Hive — Analyses 3.1, 3.2 and 3.3
│   │
│   ├── benchmark.py                              # Execution time benchmark (Spark SQL + Core)
│   └── generate_charts.py                        # Performance chart generation
│
├── docker-compose.yml                            # Hive Metastore + HiveServer2
├── requirements.txt
├── .gitignore
└── README.md

---

## Installation

### 1. Create and activate the Python environment

```bash
conda create -n flight_project python=3.10 -y
conda activate flight_project
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Windows — Hadoop compatibility (required on Windows only)

PySpark on Windows requires Hadoop native binaries.
Download `winutils.exe` and `hadoop.dll` for Hadoop 3.x and place them at `C:\hadoop\bin\`.

Then set the following environment variables (already configured in the scripts):
HADOOP_HOME = C:\hadoop
PATH        = %PATH%;C:\hadoop\bin

---

## Execution

### Step 1 — Data preparation

```bash
python src/data_preparation.py
```

Reads `data/raw/flight_data_2024.csv`, selects and cleans the relevant columns,
and writes the cleaned dataset to `data/processed/flights_cleaned.parquet`.

```bash
python src/prepare_hive_text_data.py
```

Converts the cleaned dataset to a pipe-separated text file for Hive at
`data/hive/flights_clean/flights_clean_hive.psv`.

---

### Step 2 — Spark SQL analyses

```bash
python src/analysis_spark.py
python src/analysis_spark_2.py
```

Outputs written to:
- `data/results/3_1_airline_statistics/`
- `data/results/3_2_delay_report/`
- `data/results/3_2_delay_causes_sql/`

---

### Step 3 — Spark Core / RDD analyses

The RDD scripts accept an optional input fraction as an argument (default: 1.0).

```bash
python src/analysis_rdd_3_1.py 0.25
python src/analysis_rdd_3_1.py 0.50
python src/analysis_rdd_3_1.py 1.0

python src/analysis_rdd_3_2.py 0.25
python src/analysis_rdd_3_2.py 0.50
python src/analysis_rdd_3_2.py 1.0
```

Outputs written to:
- `data/results/spark_core/3_1/fraction_<pct>/`
- `data/results/spark_core/3_2/fraction_<pct>/`

---

### Step 4 — Hive analyses (requires Docker Desktop)

**Start the Hive environment:**

```bash
docker compose up -d
```

Wait approximately 30 seconds for the metastore to become healthy, then verify:

```bash
docker compose ps
```

Both `hive-metastore` (healthy) and `hive4` (up) should appear.

**Create the database and external table:**

```bash
docker exec -it hive4 beeline -u "jdbc:hive2://localhost:10000" \
  -f /opt/hive/user_scripts/create_hive_tables.sql
```

**Run all Hive analyses (3.1, 3.2 and 3.3):**

```bash
docker exec -it hive4 beeline -u "jdbc:hive2://localhost:10000" \
  -f /opt/hive/user_scripts/analysis_hive.sql
```

Hive results are written to `data/hive/results/` via the Docker volume mount.

---

### Step 5 — Benchmark and charts

```bash
python src/benchmark.py
```

Measures execution times for Spark SQL and Spark Core at 25%, 50% and 100% input size.
Results saved to `data/results/metrics/execution_times.csv`.

```bash
python src/generate_charts.py
```

Generates performance comparison charts saved to `report/figures/`.

---

## Benchmark results

All experiments were executed in **local mode** on a machine with **32 GB RAM** running Windows 10.
Scalability was evaluated by varying the input size through random sampling of the original dataset.

| Technology | Analysis | Input size | Time (s) |
|---|---|---|---|
| Spark SQL | 3.1 | 25% | 3.44 |
| Spark SQL | 3.1 | 50% | 1.93 |
| Spark SQL | 3.1 | 100% | 3.28 |
| Spark Core | 3.1 | 25% | 17.39 |
| Spark Core | 3.1 | 50% | 27.22 |
| Spark Core | 3.1 | 100% | 44.70 |
| Spark SQL | 3.2 | 25% | 5.15 |
| Spark SQL | 3.2 | 50% | 3.58 |
| Spark SQL | 3.2 | 100% | 5.46 |
| Spark Core | 3.2 | 25% | 43.90 |
| Spark Core | 3.2 | 50% | 58.68 |
| Spark Core | 3.2 | 100% | 91.65 |
| Hive | 3.1 | 100% | 11.27 |
| Hive | 3.2 | 100% | 25.85 |

Spark SQL is consistently faster than Spark Core in local mode, primarily because it benefits
from Catalyst query optimisation and JVM-native execution, while the RDD API requires
Python-level serialisation for every row-level operation.

---

## Data preparation summary

The following steps were applied to the raw dataset before analysis:

- **Column selection:** only the 12 columns required by the analyses were retained
  (`fl_date`, `op_unique_carrier`, `origin`, `dest`, `dep_delay`, `arr_delay`,
  `cancelled`, `cancellation_code`, `carrier_delay`, `weather_delay`, `nas_delay`,
  `security_delay`, `late_aircraft_delay`)
- **Missing value handling:** null values in delay columns filled with 0; cancelled flights
  are retained intentionally to allow accurate cancellation rate computation
- **Format:** cleaned data is stored as Parquet for Spark and as pipe-separated text for Hive
- **Total records after preparation:** 7,079,081

---

## Reproducibility

To reproduce the full local workflow from scratch:

```bash
# 1. Clone the repository
git clone <repository-url>
cd flight_delay_project

# 2. Create the environment
conda create -n flight_project python=3.10 -y
conda activate flight_project
pip install -r requirements.txt

# 3. Download the dataset from Kaggle and place it at data/raw/flight_data_2024.csv

# 4. Prepare the data
python src/data_preparation.py
python src/prepare_hive_text_data.py

# 5. Run Spark SQL
python src/analysis_spark.py
python src/analysis_spark_2.py

# 6. Run Spark Core
python src/analysis_rdd_3_1.py 1.0
python src/analysis_rdd_3_2.py 1.0

# 7. Run benchmark and generate charts
python src/benchmark.py
python src/generate_charts.py

# 8. Run Hive (requires Docker Desktop running)
docker compose up -d
# Wait ~30 seconds, then:
docker exec -it hive4 beeline -u "jdbc:hive2://localhost:10000" -f /opt/hive/user_scripts/create_hive_tables.sql
docker exec -it hive4 beeline -u "jdbc:hive2://localhost:10000" -f /opt/hive/user_scripts/analysis_hive.sql
```

---

## Notes on Git tracking

Large files and generated data are excluded from Git via `.gitignore`:

| Path | Reason |
|---|---|
| `data/raw/` | Original dataset (~1.3 GB) |
| `data/processed/` | Generated Parquet (~300 MB) |
| `data/hive/flights_clean/` | Generated PSV (~310 MB) |
| `data/results/*` | Generated outputs (except `metrics/`) |
| `tmp/` | Spark temporary shuffle files |

The benchmark results (`data/results/metrics/execution_times.csv`) are tracked by Git
so that the performance comparison is available without re-running the full benchmark.

---

## Author

Juan Miguel — Roma Tre University · Big Data Course · A.Y. 2024–2025
