import os
import sys
from pathlib import Path

RUN_MODE = os.getenv("RUN_MODE", "local").lower()
PROJECT_ROOT = Path(__file__).resolve().parents[1]

DEFAULT_DATA_PATH = PROJECT_ROOT / "data" / "processed" / "flights_cleaned.parquet"
DEFAULT_OUTPUT_BASE = PROJECT_ROOT / "data" / "results" / "spark_core" / "3_1"
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
    builder = SparkSession.builder.appName("AirlineStatistics_SparkCore_RDD")

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

def month_to_mask(month_value):
    if month_value is None:
        return 0

    try:
        month_int = int(month_value)
    except ValueError:
        return 0

    if month_int < 1 or month_int > 12:
        return 0

    return 1 << month_int

def mask_to_months_string(mask):
    months = []

    for month in range(1, 13):
        if mask & (1 << month):
            months.append(str(month))

    return ",".join(months)

def analyze_airline_statistics_rdd():
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
        .select("op_unique_carrier", "origin", "arr_delay", "cancelled", "fl_date")
        .withColumn("month_num", sql_month(F.col("fl_date")))
        .select("op_unique_carrier", "origin", "arr_delay", "cancelled", "month_num")
        .repartition(NUM_PARTITIONS)
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    total_rows = df_prep.count()
    print(f"Prepared rows: {total_rows}")

    if total_rows == 0:
        print("There are no rows to process with this fraction.")
        spark.stop()
        return

    print("Running Map/Reduce with Spark Core")

    def map_row(row):
        airline = row.op_unique_carrier if row.op_unique_carrier else "UNKNOWN"
        airport = row.origin if row.origin else "UNKNOWN"

        arr_delay = float(row.arr_delay) if row.arr_delay is not None else 0.0
        cancelled = int(row.cancelled) if row.cancelled is not None else 0

        month_mask = month_to_mask(row.month_num)

        return (
            (airline, airport),
            (
                1,
                arr_delay,
                arr_delay,
                arr_delay,
                cancelled,
                month_mask,
            ),
        )

    def reduce_func(v1, v2):
        return (
            v1[0] + v2[0],
            v1[1] + v2[1],
            min(v1[2], v2[2]),
            max(v1[3], v2[3]),
            v1[4] + v2[4],
            v1[5] | v2[5],
        )

    def format_result(item):
        airline, airport = item[0]
        count, sum_arr, min_arr, max_arr, cancel_sum, month_mask = item[1]

        avg_arr = round(sum_arr / count, 2) if count > 0 else 0.0
        cancel_rate = round((cancel_sum / count) * 100, 2) if count > 0 else 0.0
        months_str = mask_to_months_string(month_mask)

        return (
            airline,
            airport,
            int(count),
            float(min_arr),
            float(max_arr),
            float(avg_arr),
            float(cancel_rate),
            months_str,
        )

    result_rdd = (
        df_prep.rdd
        .map(map_row)
        .reduceByKey(reduce_func, numPartitions=NUM_PARTITIONS)
        .map(format_result)
    )

    schema = StructType([
        StructField("Airline_Code", StringType(), True),
        StructField("Departure_Airport", StringType(), True),
        StructField("Total_Flights", IntegerType(), True),
        StructField("Min_Arrival_Delay", DoubleType(), True),
        StructField("Max_Arrival_Delay", DoubleType(), True),
        StructField("Avg_Arrival_Delay", DoubleType(), True),
        StructField("Cancellation_Rate_Pct", DoubleType(), True),
        StructField("Operating_Months", StringType(), True),
    ])

    result_df = spark.createDataFrame(result_rdd, schema)

    print("\nFirst 10 rows:")
    result_df.show(10, truncate=False)

    print(f"Saving results: {output_path}")

    (
        result_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(output_path)
    )

    df_prep.unpersist()

    print("3.1 (RDD) completed")
    spark.stop()

if __name__ == "__main__":
    analyze_airline_statistics_rdd()
