import os
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] += os.pathsep + "C:\\hadoop\\bin"

import time
import csv
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, month, when, count, avg,
    collect_set, array_sort, concat_ws,
    coalesce, greatest, lit,
    round as spark_round,
    sum as spark_sum
)
from pyspark.sql.types import (StructType, StructField, StringType,
                                IntegerType, DoubleType)
from pyspark.sql.window import Window
import pyspark.sql.functions as F

DATA_PATH  = "../data/processed/flights_cleaned.parquet"
OUTPUT_CSV = "../data/results/metrics/execution_times.csv"

def get_spark(app_name):
    return SparkSession.builder \
        .appName(app_name) \
        .master("local[1]") \
        .config("spark.driver.memory", "12g") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()

def get_fraction(spark, fraction):
    """Devuelve una fracción del dataset."""
    df = spark.read.parquet(DATA_PATH)
    if fraction < 1.0:
        df = df.sample(fraction=fraction, seed=42)
    return df

# ── Análisis 3.1 Spark SQL ─────────────────────────────────────────────────
def run_3_1_sql(spark, df, fraction, results):
    df.createOrReplaceTempView("flights")
    t0 = time.time()
    spark.sql("""
        SELECT op_unique_carrier AS Airline_Code, origin AS Departure_Airport,
               COUNT(*) AS Total_Flights,
               MIN(arr_delay) AS Min_Arrival_Delay,
               MAX(arr_delay) AS Max_Arrival_Delay,
               ROUND(AVG(arr_delay), 2) AS Avg_Arrival_Delay,
               ROUND((SUM(cancelled)/COUNT(*))*100, 2) AS Cancellation_Rate_Pct,
               array_sort(collect_set(month(CAST(fl_date AS TIMESTAMP)))) AS Months
        FROM flights
        GROUP BY op_unique_carrier, origin
        ORDER BY Airline_Code, Departure_Airport
    """).write.mode("overwrite").parquet(
        f"../data/results/spark_sql/3_1/fraction_{int(fraction*100)}"
    )
    elapsed = round(time.time() - t0, 2)
    results.append(("Spark SQL", "3.1", f"{int(fraction*100)}%", elapsed))
    print(f"  [Spark SQL 3.1 {int(fraction*100)}%] {elapsed}s")

# ── Análisis 3.2 Spark SQL ─────────────────────────────────────────────────
def run_3_2_sql(spark, df, fraction, results):
    df_t = df.withColumn("Month", month(col("fl_date"))) \
             .withColumn("Delay_Category",
                when(col("dep_delay") < 15, "1_Low (<15m)")
                .when((col("dep_delay") >= 15) & (col("dep_delay") <= 60), "2_Medium (15-60m)")
                .otherwise("3_High (>60m)"))
    t0 = time.time()
    df_t.groupBy("origin", "Month", "Delay_Category") \
        .agg(count("*").alias("Total_Flights"),
             spark_round(avg("dep_delay"), 2).alias("Avg_Dep_Delay"),
             spark_round(avg("arr_delay"), 2).alias("Avg_Arr_Delay")) \
        .write.mode("overwrite").parquet(
            f"../data/results/spark_sql/3_2/fraction_{int(fraction*100)}"
        )
    elapsed = round(time.time() - t0, 2)
    results.append(("Spark SQL", "3.2", f"{int(fraction*100)}%", elapsed))
    print(f"  [Spark SQL 3.2 {int(fraction*100)}%] {elapsed}s")

# ── Análisis 3.1 Spark Core ────────────────────────────────────────────────
def run_3_1_rdd(spark, df, fraction, results):
    from pyspark.sql.functions import month as sql_month
    df_prep = df.withColumn("month_num", sql_month(col("fl_date"))) \
                .select("op_unique_carrier","origin","arr_delay","cancelled","month_num")
    df_prep.persist(); df_prep.count()

    def map_row(row):
        a = row.op_unique_carrier or "UNKNOWN"
        o = row.origin or "UNKNOWN"
        d = float(row.arr_delay or 0)
        c = int(row.cancelled or 0)
        m = row.month_num or 0
        return ((a, o), (1, d, d, d, c, frozenset([m])))

    def reduce_func(v1, v2):
        return (v1[0]+v2[0], v1[1]+v2[1], min(v1[2],v2[2]),
                max(v1[3],v2[3]), v1[4]+v2[4], v1[5]|v2[5])

    t0 = time.time()
    collected = df_prep.rdd.map(map_row) \
        .reduceByKey(reduce_func, numPartitions=4) \
        .map(lambda x: (
            x[0][0], x[0][1], x[1][0],
            round(x[1][1]/x[1][0], 2) if x[1][0]>0 else 0.0,
            x[1][2], x[1][3],
            round((x[1][4]/x[1][0])*100, 2) if x[1][0]>0 else 0.0,
            ",".join(str(m) for m in sorted(x[1][5]))
        )).collect()
    elapsed = round(time.time() - t0, 2)

    schema = StructType([
        StructField("Airline_Code",StringType(),True),
        StructField("Departure_Airport",StringType(),True),
        StructField("Total_Flights",IntegerType(),True),
        StructField("Avg_Arrival_Delay",DoubleType(),True),
        StructField("Min_Arrival_Delay",DoubleType(),True),
        StructField("Max_Arrival_Delay",DoubleType(),True),
        StructField("Cancellation_Rate_Pct",DoubleType(),True),
        StructField("Operating_Months",StringType(),True),
    ])
    spark.createDataFrame(collected, schema) \
         .write.mode("overwrite").parquet(
             f"../data/results/spark_core/3_1/fraction_{int(fraction*100)}"
         )
    df_prep.unpersist()
    results.append(("Spark Core", "3.1", f"{int(fraction*100)}%", elapsed))
    print(f"  [Spark Core 3.1 {int(fraction*100)}%] {elapsed}s")

# ── Análisis 3.2 Spark Core ────────────────────────────────────────────────
def run_3_2_rdd(spark, df, fraction, results):
    from pyspark.sql.functions import month as sql_month

    def categorize(d):
        if d < 15:   return "1_Low (<15m)"
        elif d <= 60: return "2_Medium (15-60m)"
        else:         return "3_High (>60m)"

    df_prep = df.withColumn("month_num", sql_month(col("fl_date"))) \
                .select("origin","month_num","dep_delay","arr_delay")
    df_prep.persist(); df_prep.count()

    t0 = time.time()
    collected = df_prep.rdd \
        .map(lambda r: (
            (r.origin or "UNKNOWN", r.month_num or 0,
             categorize(float(r.dep_delay or 0))),
            (1, float(r.dep_delay or 0), float(r.arr_delay or 0))
        )) \
        .reduceByKey(lambda a,b: (a[0]+b[0], a[1]+b[1], a[2]+b[2]),
                     numPartitions=4) \
        .map(lambda x: (
            x[0][0], x[0][1], x[0][2], x[1][0],
            round(x[1][1]/x[1][0], 2) if x[1][0]>0 else 0.0,
            round(x[1][2]/x[1][0], 2) if x[1][0]>0 else 0.0
        )).collect()
    elapsed = round(time.time() - t0, 2)

    schema = StructType([
        StructField("Departure_Airport",StringType(),True),
        StructField("Month",IntegerType(),True),
        StructField("Delay_Category",StringType(),True),
        StructField("Total_Flights",IntegerType(),True),
        StructField("Avg_Dep_Delay",DoubleType(),True),
        StructField("Avg_Arr_Delay",DoubleType(),True),
    ])
    spark.createDataFrame(collected, schema) \
         .write.mode("overwrite").parquet(
             f"../data/results/spark_core/3_2/fraction_{int(fraction*100)}"
         )
    df_prep.unpersist()
    results.append(("Spark Core", "3.2", f"{int(fraction*100)}%", elapsed))
    print(f"  [Spark Core 3.2 {int(fraction*100)}%] {elapsed}s")

# ── Main ───────────────────────────────────────────────────────────────────
def main():
    fractions = [0.25, 0.50, 1.0]
    results   = []  # lista de (technology, analysis, input_size, time_s)

    spark = get_spark("Benchmark")

    for frac in fractions:
        print(f"\n=== Fracción {int(frac*100)}% ===")
        df = get_fraction(spark, frac)
        df.persist(); df.count()

        run_3_1_sql(spark, df, frac, results)
        run_3_2_sql(spark, df, frac, results)
        run_3_1_rdd(spark, df, frac, results)
        run_3_2_rdd(spark, df, frac, results)

        df.unpersist()
        spark.catalog.clearCache()

    spark.stop()

    # Guardar CSV de tiempos
    os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)
    with open(OUTPUT_CSV, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["Technology", "Analysis", "Input_Size", "Time_s"])
        w.writerows(results)

    print(f"\nTiempos guardados en: {OUTPUT_CSV}")
    print("\nResultados:")
    print(f"{'Technology':<12} {'Analysis':<10} {'Input':<8} {'Time(s)'}")
    print("-" * 45)
    for r in results:
        print(f"{r[0]:<12} {r[1]:<10} {r[2]:<8} {r[3]}")

if __name__ == "__main__":
    main()