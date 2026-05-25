import os
import sys
import time
import csv
import gc
from pathlib import Path

# -------------------------------------------------------------------
# Configuración de rutas
# -------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[1]

DATA_PATH = PROJECT_ROOT / "data" / "processed" / "flights_cleaned.parquet"
OUTPUT_CSV = PROJECT_ROOT / "data" / "results" / "metrics" / "execution_times.csv"
SPARK_TMP = PROJECT_ROOT / "tmp" / "spark"

SPARK_TMP.mkdir(parents=True, exist_ok=True)
OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)

# -------------------------------------------------------------------
# Configuración Windows / Hadoop / Spark
# -------------------------------------------------------------------
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] += os.pathsep + "C:\\hadoop\\bin"
os.environ["SPARK_LOCAL_DIRS"] = str(SPARK_TMP)

# Fuerza a PySpark a usar exactamente el Python actual
python_exe = sys.executable
os.environ["PYSPARK_PYTHON"] = python_exe
os.environ["PYSPARK_DRIVER_PYTHON"] = python_exe

from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    month,
    when,
    count,
    avg,
    collect_set,
    array_sort,
    concat_ws,
    greatest,
    lit,
    row_number,
    expr,
    round as spark_round,
    sum as spark_sum,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)
from pyspark.sql.window import Window
import pyspark.sql.functions as F


NUM_PARTITIONS = 8


# -------------------------------------------------------------------
# Spark
# -------------------------------------------------------------------
def get_spark(app_name):
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[2]")
        .config("spark.driver.memory", "12g")
        .config("spark.executor.memory", "4g")
        .config("spark.sql.shuffle.partitions", str(NUM_PARTITIONS))
        .config("spark.default.parallelism", str(NUM_PARTITIONS))
        .config("spark.local.dir", str(SPARK_TMP))
        .config("spark.python.worker.reuse", "true")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark


def get_fraction(spark, fraction):
    df = spark.read.parquet(str(DATA_PATH))

    if fraction < 1.0:
        df = df.sample(
            withReplacement=False,
            fraction=fraction,
            seed=42
        )

    return df


def fraction_label(fraction):
    return f"{int(fraction * 100)}pct"


# -------------------------------------------------------------------
# Funciones auxiliares RDD
# -------------------------------------------------------------------
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


def month_to_mask(month_value):
    if month_value is None:
        return 0

    try:
        month_int = int(month_value)
    except Exception:
        return 0

    if month_int < 1 or month_int > 12:
        return 0

    return 1 << month_int


def mask_to_months_string(mask):
    months = []

    for month_value in range(1, 13):
        if mask & (1 << month_value):
            months.append(str(month_value))

    return ",".join(months)


def categorize_delay(delay):
    if delay < 15:
        return "1_Low (<15m)"
    elif delay <= 60:
        return "2_Medium (15-60m)"
    else:
        return "3_High (>60m)"


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

    return "; ".join(f"{cause}:{count_value}" for cause, count_value in top3)


# -------------------------------------------------------------------
# Análisis 3.1 Spark SQL
# -------------------------------------------------------------------
def run_3_1_sql(spark, df, fraction, results):
    df.createOrReplaceTempView("flights")

    output_path = (
        PROJECT_ROOT
        / "data"
        / "results"
        / "spark_sql"
        / "3_1"
        / f"fraction_{fraction_label(fraction)}"
    )

    t0 = time.time()

    result_df = spark.sql("""
        SELECT 
            op_unique_carrier AS Airline_Code,
            origin AS Departure_Airport,
            COUNT(*) AS Total_Flights,
            MIN(arr_delay) AS Min_Arrival_Delay,
            MAX(arr_delay) AS Max_Arrival_Delay,
            ROUND(AVG(arr_delay), 2) AS Avg_Arrival_Delay,
            ROUND((SUM(cancelled) / COUNT(*)) * 100, 2) AS Cancellation_Rate_Pct,
            array_sort(collect_set(month(fl_date))) AS Operating_Months
        FROM flights
        GROUP BY op_unique_carrier, origin
    """)

    result_df = result_df.withColumn(
        "Operating_Months",
        concat_ws(",", col("Operating_Months"))
    )

    result_df.write.mode("overwrite").parquet(str(output_path))

    elapsed = round(time.time() - t0, 2)

    results.append(("Spark SQL", "3.1", f"{int(fraction * 100)}%", elapsed))
    print(f"  [Spark SQL 3.1 {int(fraction * 100)}%] {elapsed}s")


# -------------------------------------------------------------------
# Análisis 3.2 Spark SQL
# -------------------------------------------------------------------
def run_3_2_sql(spark, df, fraction, results):
    output_path = (
        PROJECT_ROOT
        / "data"
        / "results"
        / "spark_sql"
        / "3_2"
        / f"fraction_{fraction_label(fraction)}"
    )

    t0 = time.time()

    df_t = (
        df
        .withColumn("Month", month(col("fl_date")))
        .withColumn(
            "Delay_Category",
            when(col("dep_delay") < 15, "1_Low (<15m)")
            .when(
                (col("dep_delay") >= 15) & (col("dep_delay") <= 60),
                "2_Medium (15-60m)"
            )
            .otherwise("3_High (>60m)")
        )
    )

    stats_df = (
        df_t
        .groupBy("origin", "Month", "Delay_Category")
        .agg(
            count("*").alias("Total_Flights"),
            spark_round(avg("dep_delay"), 2).alias("Avg_Dep_Delay"),
            spark_round(avg("arr_delay"), 2).alias("Avg_Arr_Delay"),
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

    df_causes = (
        df_t
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
            .when(
                (col("cancelled") == 0) & (col("dep_delay") < 15),
                lit(None).cast("string")
            )
            .when((carrier_delay == max_delay) & (carrier_delay > 0), lit("CARRIER"))
            .when((weather_delay == max_delay) & (weather_delay > 0), lit("WEATHER"))
            .when((nas_delay == max_delay) & (nas_delay > 0), lit("NAS"))
            .when((security_delay == max_delay) & (security_delay > 0), lit("SECURITY"))
            .when(
                (late_aircraft_delay == max_delay) & (late_aircraft_delay > 0),
                lit("LATE_AIRCRAFT")
            )
            .otherwise(lit(None).cast("string"))
        )
    )

    cause_counts = (
        df_causes
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
            F.concat(
                col("dominant_cause"),
                lit(":"),
                col("Cause_Count").cast("string")
            )
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
        .join(
            top_causes,
            on=["Departure_Airport", "Month", "Delay_Category"],
            how="left"
        )
        .fillna({"Top_3_Causes": ""})
    )

    final_df.write.mode("overwrite").parquet(str(output_path))

    elapsed = round(time.time() - t0, 2)

    results.append(("Spark SQL", "3.2", f"{int(fraction * 100)}%", elapsed))
    print(f"  [Spark SQL 3.2 {int(fraction * 100)}%] {elapsed}s")


# -------------------------------------------------------------------
# Análisis 3.1 Spark Core / RDD
# -------------------------------------------------------------------
def run_3_1_rdd(spark, df, fraction, results):
    output_path = (
        PROJECT_ROOT
        / "data"
        / "results"
        / "spark_core"
        / "3_1"
        / f"fraction_{fraction_label(fraction)}"
    )

    df_prep = (
        df
        .select("op_unique_carrier", "origin", "arr_delay", "cancelled", "fl_date")
        .withColumn("month_num", month(col("fl_date")))
        .select("op_unique_carrier", "origin", "arr_delay", "cancelled", "month_num")
        .repartition(NUM_PARTITIONS)
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    df_prep.count()

    def map_row(row):
        airline = row.op_unique_carrier if row.op_unique_carrier else "UNKNOWN"
        airport = row.origin if row.origin else "UNKNOWN"

        arr_delay = safe_float(row.arr_delay)
        cancelled = safe_int(row.cancelled)
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
        count_value, sum_arr, min_arr, max_arr, cancel_sum, month_mask = item[1]

        avg_arr = round(sum_arr / count_value, 2) if count_value > 0 else 0.0
        cancel_rate = round((cancel_sum / count_value) * 100, 2) if count_value > 0 else 0.0

        return (
            airline,
            airport,
            int(count_value),
            float(min_arr),
            float(max_arr),
            float(avg_arr),
            float(cancel_rate),
            mask_to_months_string(month_mask),
        )

    t0 = time.time()

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

    result_df.write.mode("overwrite").parquet(str(output_path))

    elapsed = round(time.time() - t0, 2)

    df_prep.unpersist()

    results.append(("Spark Core", "3.1", f"{int(fraction * 100)}%", elapsed))
    print(f"  [Spark Core 3.1 {int(fraction * 100)}%] {elapsed}s")


# -------------------------------------------------------------------
# Análisis 3.2 Spark Core / RDD
# -------------------------------------------------------------------
def run_3_2_rdd(spark, df, fraction, results):
    output_path = (
        PROJECT_ROOT
        / "data"
        / "results"
        / "spark_core"
        / "3_2"
        / f"fraction_{fraction_label(fraction)}"
    )

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
        .withColumn("month_num", month(col("fl_date")))
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

    df_prep.count()
    base_rdd = df_prep.rdd

    def map_stats(row):
        airport = row.origin if row.origin else "UNKNOWN"
        month_value = safe_int(row.month_num)

        dep_delay = safe_float(row.dep_delay)
        arr_delay = safe_float(row.arr_delay)

        delay_category = categorize_delay(dep_delay)

        return (
            (airport, month_value, delay_category),
            (
                1,
                dep_delay,
                arr_delay,
            ),
        )

    def reduce_stats(v1, v2):
        return (
            v1[0] + v2[0],
            v1[1] + v2[1],
            v1[2] + v2[2],
        )

    def format_stats(item):
        key, value = item
        count_value, total_dep_delay, total_arr_delay = value

        avg_dep_delay = round(total_dep_delay / count_value, 2) if count_value > 0 else 0.0
        avg_arr_delay = round(total_arr_delay / count_value, 2) if count_value > 0 else 0.0

        return key, (
            int(count_value),
            float(avg_dep_delay),
            float(avg_arr_delay),
        )

    def map_cause(row):
        airport = row.origin if row.origin else "UNKNOWN"
        month_value = safe_int(row.month_num)

        dep_delay = safe_float(row.dep_delay)
        cancelled = safe_int(row.cancelled)

        delay_category = categorize_delay(dep_delay)
        cause = get_delay_or_cancel_cause(row, dep_delay, cancelled)

        if cause is None:
            return None

        return (
            (airport, month_value, delay_category, cause),
            1,
        )

    def format_cause_count(item):
        key, count_value = item
        airport, month_value, delay_category, cause = key

        return (
            (airport, month_value, delay_category),
            (cause, int(count_value)),
        )

    t0 = time.time()

    stats_rdd = (
        base_rdd
        .map(map_stats)
        .reduceByKey(reduce_stats, numPartitions=NUM_PARTITIONS)
        .map(format_stats)
    )

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

    result_df = spark.createDataFrame(final_rdd, schema)

    result_df.write.mode("overwrite").parquet(str(output_path))

    elapsed = round(time.time() - t0, 2)

    df_prep.unpersist()

    results.append(("Spark Core", "3.2", f"{int(fraction * 100)}%", elapsed))
    print(f"  [Spark Core 3.2 {int(fraction * 100)}%] {elapsed}s")


# -------------------------------------------------------------------
# Main
# -------------------------------------------------------------------
def main():
    if not DATA_PATH.exists():
        raise FileNotFoundError(f"No existe el dataset procesado: {DATA_PATH}")

    fractions = [0.25, 0.50, 1.0]
    results = []

    spark = get_spark("Benchmark_Flight_Delay")

    for fraction in fractions:
        print(f"\n=== Fracción {int(fraction * 100)}% ===")

        df = get_fraction(spark, fraction)
        df = df.persist(StorageLevel.MEMORY_AND_DISK)

        total_rows = df.count()
        print(f"  Filas usadas: {total_rows}")

        run_3_1_sql(spark, df, fraction, results)
        run_3_2_sql(spark, df, fraction, results)
        run_3_1_rdd(spark, df, fraction, results)
        run_3_2_rdd(spark, df, fraction, results)

        df.unpersist()
        spark.catalog.clearCache()
        gc.collect()

    spark.stop()

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(["Technology", "Analysis", "Input_Size", "Time_s"])
        writer.writerows(results)

    print(f"\nTiempos guardados en: {OUTPUT_CSV}")

    print("\nResultados:")
    print(f"{'Technology':<12} {'Analysis':<10} {'Input':<8} {'Time(s)'}")
    print("-" * 45)

    for row in results:
        print(f"{row[0]:<12} {row[1]:<10} {row[2]:<8} {row[3]}")


if __name__ == "__main__":
    main()