import os
import sys
from pathlib import Path

RUN_MODE = os.getenv("RUN_MODE", "local").lower()
PROJECT_ROOT = Path(__file__).resolve().parents[1]

DEFAULT_DATA_PATH = PROJECT_ROOT / "data" / "processed" / "flights_cleaned.parquet"
DEFAULT_OUTPUT_BASE = PROJECT_ROOT / "data" / "results" / "spark_core" / "3_2"
DEFAULT_SPARK_TMP = PROJECT_ROOT / "tmp" / "spark"

DATA_PATH = os.getenv("DATA_PATH", str(DEFAULT_DATA_PATH))
OUTPUT_BASE = os.getenv("OUTPUT_BASE", str(DEFAULT_OUTPUT_BASE))
SPARK_TMP = Path(os.getenv("SPARK_TMP", str(DEFAULT_SPARK_TMP)))
NUM_PARTITIONS = int(os.getenv("NUM_PARTITIONS", "8"))

if RUN_MODE == "local":
    SPARK_TMP.mkdir(parents=True, exist_ok=True)
    os.environ["HADOOP_HOME"] = os.getenv("HADOOP_HOME", "C:\\hadoop")
    os.environ["PATH"] += os.pathsep + os.path.join(os.environ["HADOOP_HOME"], "bin")
    os.environ["SPARK_LOCAL_DIRS"] = str(SPARK_TMP)

python_exe = sys.executable
os.environ["PYSPARK_PYTHON"] = python_exe
os.environ["PYSPARK_DRIVER_PYTHON"] = python_exe

from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.functions import month as sql_month
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)
import pyspark.sql.functions as F

DEFAULT_SAMPLE_FRACTION = 0.05

def get_sample_fraction():
    if len(sys.argv) >= 2:
        try:
            value = float(sys.argv[1])
            if value <= 0 or value > 1:
                raise ValueError
            return value
        except ValueError:
            raise ValueError(
                "The fraction must be a number [0,1]"
            )

    return DEFAULT_SAMPLE_FRACTION

def fraction_label(fraction):
    return f"{int(fraction * 100)}pct"

def make_output_path(base_path, fraction):
    return f"{base_path.rstrip('/')}/fraction_{fraction_label(fraction)}"

def build_spark():
    builder = SparkSession.builder.appName("DelayReport_SparkCore_RDD")

    if RUN_MODE == "local":
        builder = (
            builder
            .master("local[2]")
            .config("spark.driver.memory", "12g")
            .config("spark.executor.memory", "4g")
            .config("spark.sql.shuffle.partitions", str(NUM_PARTITIONS))
            .config("spark.default.parallelism", str(NUM_PARTITIONS))
            .config("spark.local.dir", str(SPARK_TMP))
            .config("spark.python.worker.reuse", "true")
        )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

def categorize_delay(delay):
    if delay < 15:
        return "1_Low (<15m)"
    elif delay <= 60:
        return "2_Medium (15-60m)"
    else:
        return "3_High (>60m)"

def safe_float(value):
    if value is None:
        return 0.0
    try:
        return float(value)
    except Exception:
        return 0.0

def safe_int(value):
    if value is None:
        return 0

    try:
        return int(value)
    except Exception:
        try:
            return int(float(value))
        except Exception:
            return 0

def get_delay_or_cancel_cause(row, dep_delay, cancelled):
    cancel_map = {
        "A": "CANCEL_CARRIER",
        "B": "CANCEL_WEATHER",
        "C": "CANCEL_NAS",
        "D": "CANCEL_SECURITY",
    }

    if cancelled == 1:
        code = row.cancellation_code or "UNKNOWN"
        return cancel_map.get(str(code).strip(), "CANCEL_UNKNOWN")

    if dep_delay < 15:
        return None

    delays = {
        "CARRIER": safe_float(row.carrier_delay),
        "WEATHER": safe_float(row.weather_delay),
        "NAS": safe_float(row.nas_delay),
        "SECURITY": safe_float(row.security_delay),
        "LATE_AIRCRAFT": safe_float(row.late_aircraft_delay),
    }

    best_cause = max(delays, key=delays.get)

    if delays[best_cause] > 0:
        return best_cause

    return None

def top3_causes_to_string(causes_iterable):
    causes = list(causes_iterable)

    causes_sorted = sorted(
        causes,
        key=lambda item: (-item[1], item[0])
    )

    top3 = causes_sorted[:3]

    return "; ".join(f"{cause}:{count}" for cause, count in top3)

def analyze_delay_report_rdd():
    sample_fraction = get_sample_fraction()
    output_path = make_output_path(OUTPUT_BASE, sample_fraction)

    spark = build_spark()

    print("Spark Session (Core/RDD) started:")
    print(f"Run mode: {RUN_MODE}")
    print(f"Reading data from: {DATA_PATH}")
    print(f"Sample fraction used: {sample_fraction}")
    print(f"Output path: {output_path}")

    if RUN_MODE == "local" and not Path(DATA_PATH).exists():
        raise FileNotFoundError(f"The processed dataset does not exist: {DATA_PATH}")

    df = spark.read.parquet(DATA_PATH)

    if sample_fraction < 1.0:
        df = df.sample(withReplacement=False, fraction=sample_fraction, seed=42)

    df_prep = (
        df
        .select(
            "origin",
            "fl_date",
            "dep_delay",
            "arr_delay",
            "cancelled",
            "cancellation_code",
            "carrier_delay",
            "weather_delay",
            "nas_delay",
            "security_delay",
            "late_aircraft_delay",
        )
        .withColumn("month_num", sql_month(F.col("fl_date")))
        .select(
            "origin",
            "month_num",
            "dep_delay",
            "arr_delay",
            "cancelled",
            "cancellation_code",
            "carrier_delay",
            "weather_delay",
            "nas_delay",
            "security_delay",
            "late_aircraft_delay",
        )
        .repartition(NUM_PARTITIONS)
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    total_rows = df_prep.count()
    print(f"Prepared rows: {total_rows}")

    if total_rows == 0:
        print("There are no rows to process with this fraction.")
        spark.stop()
        return

    base_rdd = df_prep.rdd

    print("Running Map/Reduce Part A: delay ranges")

    def map_stats(row):
        airport = row.origin if row.origin else "UNKNOWN"
        month = safe_int(row.month_num)

        dep_delay = safe_float(row.dep_delay)
        arr_delay = safe_float(row.arr_delay)

        delay_category = categorize_delay(dep_delay)

        key = (airport, month, delay_category)
        value = (
            1,
            dep_delay,
            arr_delay,
        )

        return key, value

    def reduce_stats(v1, v2):
        return (
            v1[0] + v2[0],
            v1[1] + v2[1],
            v1[2] + v2[2],
        )

    def format_stats(item):
        key, value = item
        airport, month, delay_category = key
        count, total_dep_delay, total_arr_delay = value

        avg_dep_delay = round(total_dep_delay / count, 2) if count > 0 else 0.0
        avg_arr_delay = round(total_arr_delay / count, 2) if count > 0 else 0.0

        return key, (
            int(count),
            float(avg_dep_delay),
            float(avg_arr_delay),
        )

    stats_rdd = (
        base_rdd
        .map(map_stats)
        .reduceByKey(reduce_stats, numPartitions=NUM_PARTITIONS)
        .map(format_stats)
    )

    print("Running Map/Reduce Part B: top 3 causes")

    def map_cause(row):
        airport = row.origin if row.origin else "UNKNOWN"
        month = safe_int(row.month_num)

        dep_delay = safe_float(row.dep_delay)
        cancelled = safe_int(row.cancelled)

        delay_category = categorize_delay(dep_delay)
        cause = get_delay_or_cancel_cause(row, dep_delay, cancelled)

        if cause is None:
            return None

        key = (airport, month, delay_category, cause)

        return key, 1

    def format_cause_count(item):
        key, count = item
        airport, month, delay_category, cause = key

        return (airport, month, delay_category), (cause, int(count))

    cause_counts_rdd = (
        base_rdd
        .map(map_cause)
        .filter(lambda x: x is not None)
        .reduceByKey(lambda a, b: a + b, numPartitions=NUM_PARTITIONS)
        .map(format_cause_count)
    )

    top_causes_rdd = (
        cause_counts_rdd
        .groupByKey(numPartitions=NUM_PARTITIONS)
        .mapValues(top3_causes_to_string)
    )

    print("Joining statistics with causes")

    final_rdd = (
        stats_rdd
        .leftOuterJoin(top_causes_rdd, numPartitions=NUM_PARTITIONS)
        .map(lambda item: (
            item[0][0],
            int(item[0][1]),
            item[0][2],
            int(item[1][0][0]),
            float(item[1][0][1]),
            float(item[1][0][2]),
            item[1][1] if item[1][1] else "",
        ))
    )

    schema = StructType([
        StructField("Departure_Airport", StringType(), True),
        StructField("Month", IntegerType(), True),
        StructField("Delay_Category", StringType(), True),
        StructField("Total_Flights", IntegerType(), True),
        StructField("Avg_Dep_Delay", DoubleType(), True),
        StructField("Avg_Arr_Delay", DoubleType(), True),
        StructField("Top_3_Causes", StringType(), True),
    ])

    result_df = (
        spark
        .createDataFrame(final_rdd, schema)
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    print("\nFIRST 10 ROWS OF RESULT 3.2 RDD:")
    result_df.show(10, truncate=False)

    print(f"Saving results to: {output_path}")

    (
        result_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(output_path)
    )

    result_df.unpersist()
    df_prep.unpersist()

    print("3.2 (RDD) completed:")
    spark.stop()

if __name__ == "__main__":
    analyze_delay_report_rdd()
