import os
import sys
from pathlib import Path

RUN_MODE = os.getenv("RUN_MODE", "local").lower()
PROJECT_ROOT = Path(__file__).resolve().parents[1]

DEFAULT_DATA_PATH = PROJECT_ROOT / "data" / "processed" / "flights_cleaned.parquet"
DEFAULT_OUTPUT_BASE = PROJECT_ROOT / "data" / "results" / "spark_sql" / "3_2"
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
from pyspark.sql.functions import (
    col,
    month,
    when,
    round,
    count,
    avg,
    greatest,
    lit,
    row_number,
    expr,
    concat_ws,
)
from pyspark.sql.window import Window
import pyspark.sql.functions as F

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
    builder = SparkSession.builder.appName("DelayReport_SparkSQL")

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

def analyze_delay_report_sql():
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

    df_transformed = (
        df
        .withColumn("Month", month(col("fl_date")))
        .withColumn(
            "Delay_Category",
            when(col("dep_delay") < 15, "1_Low (<15m)")
            .when((col("dep_delay") >= 15) & (col("dep_delay") <= 60), "2_Medium (15-60m)")
            .otherwise("3_High (>60m)")
        )
    )

    stats_df = (
        df_transformed
        .groupBy("origin", "Month", "Delay_Category")
        .agg(
            count("*").alias("Total_Flights"),
            round(avg("dep_delay"), 2).alias("Avg_Dep_Delay"),
            round(avg("arr_delay"), 2).alias("Avg_Arr_Delay")
        )
        .withColumnRenamed("origin", "Departure_Airport")
    )

    carrier_delay = F.coalesce(col("carrier_delay"), lit(0.0))
    weather_delay = F.coalesce(col("weather_delay"), lit(0.0))
    nas_delay = F.coalesce(col("nas_delay"), lit(0.0))
    security_delay = F.coalesce(col("security_delay"), lit(0.0))
    late_aircraft_delay = F.coalesce(col("late_aircraft_delay"), lit(0.0))

    max_delay = greatest(
        carrier_delay,
        weather_delay,
        nas_delay,
        security_delay,
        late_aircraft_delay,
    )

    df_with_cause = (
        df_transformed
        .withColumn(
            "dominant_cause",
            when(
                col("cancelled") == 1,
                when(col("cancellation_code") == "A", lit("CANCEL_CARRIER"))
                .when(col("cancellation_code") == "B", lit("CANCEL_WEATHER"))
                .when(col("cancellation_code") == "C", lit("CANCEL_NAS"))
                .when(col("cancellation_code") == "D", lit("CANCEL_SECURITY"))
                .otherwise(lit("CANCEL_UNKNOWN"))
            )
            .when((col("cancelled") == 0) & (col("dep_delay") < 15), lit(None).cast("string"))
            .when((carrier_delay == max_delay) & (carrier_delay > 0), lit("CARRIER"))
            .when((weather_delay == max_delay) & (weather_delay > 0), lit("WEATHER"))
            .when((nas_delay == max_delay) & (nas_delay > 0), lit("NAS"))
            .when((security_delay == max_delay) & (security_delay > 0), lit("SECURITY"))
            .when((late_aircraft_delay == max_delay) & (late_aircraft_delay > 0), lit("LATE_AIRCRAFT"))
            .otherwise(lit(None).cast("string"))
        )
    )

    cause_counts = (
        df_with_cause
        .filter(col("dominant_cause").isNotNull())
        .groupBy("origin", "Month", "Delay_Category", "dominant_cause")
        .agg(count("*").alias("Cause_Count"))
    )

    window_spec = (
        Window
        .partitionBy("origin", "Month", "Delay_Category")
        .orderBy(col("Cause_Count").desc(), col("dominant_cause").asc())
    )

    top_causes = (
        cause_counts
        .withColumn("rk", row_number().over(window_spec))
        .filter(col("rk") <= 3)
        .withColumn(
            "cause_string",
            F.concat(col("dominant_cause"), lit(":"), col("Cause_Count").cast("string"))
        )
        .groupBy("origin", "Month", "Delay_Category")
        .agg(
            concat_ws(
                "; ",
                expr(
                    "transform("
                    "array_sort("
                    "collect_list(named_struct('rk', rk, 's', cause_string))"
                    "), x -> x.s)"
                )
            ).alias("Top_3_Causes")
        )
        .withColumnRenamed("origin", "Departure_Airport")
    )

    final_df = (
        stats_df
        .join(top_causes, on=["Departure_Airport", "Month", "Delay_Category"], how="left")
        .fillna({"Top_3_Causes": ""})
        .orderBy("Departure_Airport", "Month", "Delay_Category")
    )

    print("\nFIRST 10 ROWS OF RESULT 3.2 SPARK SQL:")
    final_df.show(10, truncate=False)

    print(f"Saving final report to: {output_path}")

    final_df.repartition(1).write.mode("overwrite").csv(output_path, header=True)

    print("3.2 (Spark SQL) completed:")
    spark.stop()

if __name__ == "__main__":
    analyze_delay_report_sql()
