import os
from pathlib import Path

RUN_MODE = os.getenv("RUN_MODE", "local").lower()
PROJECT_ROOT = Path(__file__).resolve().parents[1]

DEFAULT_RAW_DATA_PATH = PROJECT_ROOT / "data" / "raw" / "flight_data_2024.csv"
DEFAULT_PROCESSED_DATA_PATH = PROJECT_ROOT / "data" / "processed" / "flights_cleaned.parquet"
DEFAULT_SPARK_TMP = PROJECT_ROOT / "tmp" / "spark"

RAW_DATA_PATH = os.getenv("RAW_DATA_PATH", str(DEFAULT_RAW_DATA_PATH))
PROCESSED_DATA_PATH = os.getenv("PROCESSED_DATA_PATH", str(DEFAULT_PROCESSED_DATA_PATH))
SPARK_TMP = Path(os.getenv("SPARK_TMP", str(DEFAULT_SPARK_TMP)))
NUM_OUTPUT_PARTITIONS = int(os.getenv("NUM_OUTPUT_PARTITIONS", "4"))

if RUN_MODE == "local":
    SPARK_TMP.mkdir(parents=True, exist_ok=True)
    os.environ["HADOOP_HOME"] = os.getenv("HADOOP_HOME", "C:\\hadoop")
    os.environ["PATH"] += os.pathsep + os.path.join(os.environ["HADOOP_HOME"], "bin")
    os.environ["SPARK_LOCAL_DIRS"] = str(SPARK_TMP)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType

def build_spark():
    builder = SparkSession.builder.appName("FlightDataPreparation")
    if RUN_MODE == "local":
        builder = (
            builder
            .master("local[*]")
            .config("spark.local.dir", str(SPARK_TMP))
        )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

def prepare_data():
    spark = build_spark()

    print(f"Spark Session started – run mode: {RUN_MODE}")

    if RUN_MODE == "local" and not Path(RAW_DATA_PATH).exists():
        raise FileNotFoundError(f"Raw dataset not found: {RAW_DATA_PATH}")

    print(f"Loading data from: {RAW_DATA_PATH}")
    df_raw = spark.read.csv(RAW_DATA_PATH, header=True, inferSchema=True)
    print(f"Total original records: {df_raw.count()}")

    key_columns = [
        "fl_date",
        "op_unique_carrier",
        "origin",
        "dest",
        "dep_delay",
        "arr_delay",
        "cancelled",
        "cancellation_code",
        "carrier_delay",
        "weather_delay",
        "nas_delay",
        "security_delay",
        "late_aircraft_delay",
    ]

    existing_columns = [c for c in key_columns if c in df_raw.columns]
    df_cleaned = df_raw.select(existing_columns)

    df_cleaned = df_cleaned.fillna(
        0,
        subset=[
            "dep_delay", "arr_delay",
            "carrier_delay", "weather_delay", "nas_delay",
            "security_delay", "late_aircraft_delay",
        ],
    )

    if "cancelled" in df_cleaned.columns:
        df_cleaned = df_cleaned.withColumn("cancelled", col("cancelled").cast(IntegerType()))

    print(f"Total records after cleaning: {df_cleaned.count()}")
    print(f"Saving processed data to: {PROCESSED_DATA_PATH}")

    df_cleaned.repartition(NUM_OUTPUT_PARTITIONS).write.mode("overwrite").parquet(PROCESSED_DATA_PATH)

    print("Data preparation completed successfully.")
    spark.stop()

if __name__ == "__main__":
    prepare_data()
