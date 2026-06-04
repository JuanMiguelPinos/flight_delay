#!/bin/bash
# ─────────────────────────────────────────────────────────────────────────────
# run_cluster.sh  –  Execute the full benchmark on an EMR cluster.
#
# Usage:
#   bash scripts/run_cluster.sh s3://your-bucket/flight_delay_project
#
# The script expects the following layout inside the S3 prefix:
#   <PROJECT_S3>/
#     data/raw/flight_data_2024.csv          ← upload before running
#     scripts/data_preparation.py
#     scripts/benchmark.py
#     sql/create_hive_tables_emr.sql
#     sql/analysis_hive_3_1_emr.sql
#     sql/analysis_hive_3_2_emr.sql
#
# FIX 3: The original script defined:
#   SCRIPTS_DIR="$PROJECT_S3/scripts"
#   SQL_DIR="$PROJECT_S3/sql"
# but the repository keeps Python files under src/ and SQL files under src/ too.
# The variables below now match the ACTUAL repo layout so that the aws s3 cp
# commands succeed. Adjust if your S3 upload structure is different.
# ─────────────────────────────────────────────────────────────────────────────

set -euo pipefail

if [ "$#" -lt 1 ]; then
  echo "Usage: bash scripts/run_cluster.sh s3://your-bucket/flight_delay_project"
  exit 1
fi

PROJECT_S3="${1%/}"
CLUSTER_SIZE="${CLUSTER_SIZE:-1-primary-1-core}"
LOCAL_METRICS="/tmp/cluster_execution_times.csv"
HIVE_METRICS="/tmp/hive_cluster_execution_times.csv"

RAW_DATA_PATH="$PROJECT_S3/data/raw/flight_data_2024.csv"
PROCESSED_DATA_PATH="$PROJECT_S3/data/processed/flights_cleaned.parquet"
RESULTS_BASE="$PROJECT_S3/results"

# ── FIX 3: Use src/ as the canonical source directory (matches the repo) ──────
SCRIPTS_DIR="$PROJECT_S3/src"
SQL_DIR="$PROJECT_S3/src"

echo "Project S3 root    : $PROJECT_S3"
echo "Raw data path      : $RAW_DATA_PATH"
echo "Processed data path: $PROCESSED_DATA_PATH"
echo "Results base       : $RESULTS_BASE"
echo "Scripts dir (S3)   : $SCRIPTS_DIR"
echo "SQL dir (S3)       : $SQL_DIR"

# ─────────────────────────────────────────────────────────────────────────────
# Download scripts from S3 to the EMR master node's /tmp
# ─────────────────────────────────────────────────────────────────────────────
aws s3 cp "$SCRIPTS_DIR/data_preparation.py"      /tmp/data_preparation.py
aws s3 cp "$SCRIPTS_DIR/benchmark.py"             /tmp/benchmark.py
aws s3 cp "$SQL_DIR/create_hive_tables_emr.sql"   /tmp/create_hive_tables_emr.sql
aws s3 cp "$SQL_DIR/analysis_hive_3_1_emr.sql"    /tmp/analysis_hive_3_1_emr.sql
aws s3 cp "$SQL_DIR/analysis_hive_3_2_emr.sql"    /tmp/analysis_hive_3_2_emr.sql

# ─────────────────────────────────────────────────────────────────────────────
# Step 1 – Data preparation (CSV → Parquet in S3)
# ─────────────────────────────────────────────────────────────────────────────
echo ""
echo "=== Step 1: Preparing Parquet dataset ==="
RUN_MODE=cluster \
RAW_DATA_PATH="$RAW_DATA_PATH" \
PROCESSED_DATA_PATH="$PROCESSED_DATA_PATH" \
NUM_OUTPUT_PARTITIONS=8 \
spark-submit /tmp/data_preparation.py

# ─────────────────────────────────────────────────────────────────────────────
# Step 2 – Spark SQL + Spark Core benchmark
# ─────────────────────────────────────────────────────────────────────────────
echo ""
echo "=== Step 2: Running Spark SQL and Spark Core benchmark ==="
RUN_MODE=cluster \
ENVIRONMENT_LABEL=EMR \
CLUSTER_SIZE="$CLUSTER_SIZE" \
DATA_PATH="$PROCESSED_DATA_PATH" \
RESULTS_BASE="$RESULTS_BASE" \
OUTPUT_CSV="$LOCAL_METRICS" \
NUM_PARTITIONS=8 \
spark-submit /tmp/benchmark.py

aws s3 cp "$LOCAL_METRICS" "$RESULTS_BASE/metrics/cluster_execution_times.csv"

# ─────────────────────────────────────────────────────────────────────────────
# Step 3 – Hive: create external table over the Parquet data, then run queries
# ─────────────────────────────────────────────────────────────────────────────
echo ""
echo "=== Step 3: Creating Hive external table and view ==="
hive \
  --hiveconf processed_path="$PROCESSED_DATA_PATH" \
  -f /tmp/create_hive_tables_emr.sql

echo "Environment,Technology,Analysis,Input_Size,Cluster_Size,Time_s" > "$HIVE_METRICS"

run_hive_job() {
  local analysis="$1"
  local query_file="$2"
  local output_path="$3"

  echo ""
  echo "--- Running Hive analysis $analysis ---"

  local start_time end_time elapsed
  start_time=$(date +%s)

  hive \
    --hiveconf output_path="$output_path" \
    -f "$query_file"

  end_time=$(date +%s)
  elapsed=$((end_time - start_time))

  echo "EMR,Hive,$analysis,100%,$CLUSTER_SIZE,$elapsed" >> "$HIVE_METRICS"
  echo "    Hive $analysis finished in ${elapsed}s"
}

run_hive_job "3.1" /tmp/analysis_hive_3_1_emr.sql "$RESULTS_BASE/hive/3_1/fraction_100pct"
run_hive_job "3.2" /tmp/analysis_hive_3_2_emr.sql "$RESULTS_BASE/hive/3_2/fraction_100pct"

aws s3 cp "$HIVE_METRICS" "$RESULTS_BASE/metrics/hive_cluster_execution_times.csv"

echo ""
echo "=== Cluster execution completed ==="
echo "Spark metrics : $RESULTS_BASE/metrics/cluster_execution_times.csv"
echo "Hive metrics  : $RESULTS_BASE/metrics/hive_cluster_execution_times.csv"
