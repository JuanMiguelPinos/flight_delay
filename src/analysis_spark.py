import os
import sys
from pathlib import Path

RUN_MODE = os.getenv("RUN_MODE", "local").lower()
PROJECT_ROOT = Path(__file__).resolve().parents[1]

DEFAULT_DATA_PATH = PROJECT_ROOT / "data" / "processed" / "flights_cleaned.parquet"
DEFAULT_OUTPUT_BASE = PROJECT_ROOT / "data" / "results" / "spark_sql" / "3_1"
DEFAULT_SPARK_TMP = PROJECT_ROOT / "tmp" / "spark"

DATA_PATH = os.getenv("DATA_PATH", str(DEFAULT_DATA_PATH))
OUTPUT_BASE = os.getenv("OUTPUT_BASE", str(DEFAULT_OUTPUT_BASE))
SPARK_TMP = Path(os.getenv("SPARK_TMP", str(DEFAULT_SPARK_TMP)))
NUM_PARTITIONS = int(os.getenv("NUM_PARTITIONS", "8"))
DEFAULT_SAMPLE_FRACTION = 1.0

if RUN_MODE == "local":
    SPARK_TMP.mkdir(parents=True, exist_ok=True)
    os.environ["HADOOP_HOME"] = os.getenv("HADOOP_HOME", "C:\\hadoop")
    os.environ["PATH"] += os.pathsep + os.path.join(os.environ["HADOOP_HOME"], "bin")
    os.environ["SPARK_LOCAL_DIRS"] = str(SPARK_TMP)

from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws

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
    builder = SparkSession.builder.appName("AirlineStatistics_SparkSQL")

    if RUN_MODE == "local":
        builder = (
            builder
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", str(NUM_PARTITIONS))
            .config("spark.local.dir", str(SPARK_TMP))
        )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

def analyze_airline_statistics():
    sample_fraction = get_sample_fraction()
    output_path = make_output_path(OUTPUT_BASE, sample_fraction)

    spark = build_spark()

    print("Spark Session started:")
    print(f"Run mode: {RUN_MODE}")
    print(f"Reading data from: {DATA_PATH}")
    print(f"Sample fraction used: {sample_fraction}")

    if RUN_MODE == "local" and not Path(DATA_PATH).exists():
        raise FileNotFoundError(f"The processed dataset does not exist: {DATA_PATH}")

    df = spark.read.parquet(DATA_PATH)

    if sample_fraction < 1.0:
        df = df.sample(withReplacement=False, fraction=sample_fraction, seed=42)

    df.createOrReplaceTempView("flights")

    query = """
        SELECT 
            op_unique_carrier AS Airline_Code,
            origin AS Departure_Airport,
            COUNT(*) AS Total_Flights,
            MIN(arr_delay) AS Min_Arrival_Delay,
            MAX(arr_delay) AS Max_Arrival_Delay,
            ROUND(AVG(arr_delay), 2) AS Avg_Arrival_Delay,
            ROUND((SUM(cancelled) / COUNT(*)) * 100, 2) AS Cancellation_Rate_Pct,
            array_sort(collect_set(month(CAST(fl_date AS TIMESTAMP)))) AS Operating_Months
        FROM flights
        GROUP BY op_unique_carrier, origin
        ORDER BY Airline_Code, Departure_Airport
    """

    print("Running Analysis 3.1 with Spark SQL")
    result_df = spark.sql(query)

    print("\nFIRST 10 ROWS OF THE RESULT:")
    result_df.show(10, truncate=False)

    print(f"Saving final report to: {output_path}")

    result_df.withColumn("Operating_Months", concat_ws(", ", "Operating_Months")) \
             .repartition(1).write.mode("overwrite").csv(output_path, header=True)

    print("3.1 completed:")
    spark.stop()

if __name__ == "__main__":
    analyze_airline_statistics()
