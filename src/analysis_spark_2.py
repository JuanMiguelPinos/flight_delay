import os
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] += os.pathsep + "C:\\hadoop\\bin"

from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, month, when, round, count, avg,
                                    coalesce, greatest, lit)
from pyspark.sql.window import Window
import pyspark.sql.functions as F

def analyze_delay_report_sql():
    spark = SparkSession.builder \
        .appName("DelayReport_SparkSQL") \
        .master("local[*]") \
        .getOrCreate()

    print("--- Spark Session iniciada ---")

    data_path = "../data/processed/flights_cleaned.parquet"
    df = spark.read.parquet(data_path)

    # ── PARTE A: rangos de retraso ──────────────────────────────────────────
    df_transformed = df.withColumn("Month", month(col("fl_date"))) \
        .withColumn("Delay_Category",
            when(col("dep_delay") < 15, "1_Low (<15m)")
            .when((col("dep_delay") >= 15) & (col("dep_delay") <= 60), "2_Medium (15-60m)")
            .otherwise("3_High (>60m)")
        )

    result_df = df_transformed.groupBy("origin", "Month", "Delay_Category") \
        .agg(
            count("*").alias("Total_Flights"),
            round(avg("dep_delay"), 2).alias("Avg_Dep_Delay"),
            round(avg("arr_delay"), 2).alias("Avg_Arr_Delay")
        ) \
        .withColumnRenamed("origin", "Departure_Airport") \
        .orderBy("Departure_Airport", "Month", "Delay_Category")

    print("\n--- PRIMERAS 10 FILAS DEL RESULTADO PARTE A ---")
    result_df.show(10, truncate=False)

    result_df.repartition(1).write.mode("overwrite").csv(
        "../data/results/3_2_delay_report", header=True
    )

    # ── PARTE B: top 3 causas por (aeropuerto, mes) ─────────────────────────
    df_with_cause = df.withColumn("dominant_cause",
        when(col("cancelled") == 1,
            when(col("cancellation_code") == "A", lit("CANCEL_CARRIER"))
            .when(col("cancellation_code") == "B", lit("CANCEL_WEATHER"))
            .when(col("cancellation_code") == "C", lit("CANCEL_NAS"))
            .when(col("cancellation_code") == "D", lit("CANCEL_SECURITY"))
            .otherwise(lit("CANCEL_UNKNOWN")))
        .when((col("dep_delay") < 15) & (col("cancelled") == 0),
              lit(None).cast("string"))
        .when((col("carrier_delay") == greatest(
                "carrier_delay","weather_delay","nas_delay",
                "security_delay","late_aircraft_delay"))
              & (col("carrier_delay") > 0),       lit("CARRIER"))
        .when((col("weather_delay") == greatest(
                "carrier_delay","weather_delay","nas_delay",
                "security_delay","late_aircraft_delay"))
              & (col("weather_delay") > 0),        lit("WEATHER"))
        .when((col("nas_delay") == greatest(
                "carrier_delay","weather_delay","nas_delay",
                "security_delay","late_aircraft_delay"))
              & (col("nas_delay") > 0),            lit("NAS"))
        .when((col("security_delay") == greatest(
                "carrier_delay","weather_delay","nas_delay",
                "security_delay","late_aircraft_delay"))
              & (col("security_delay") > 0),       lit("SECURITY"))
        .when(col("late_aircraft_delay") > 0,      lit("LATE_AIRCRAFT"))
        .otherwise(lit("UNKNOWN"))
    )

    df_with_cause.createOrReplaceTempView("flights_with_cause")
    top_causes = spark.sql("""
        SELECT origin, Month, dominant_cause, cause_count,
               ROW_NUMBER() OVER (
                   PARTITION BY origin, Month
                   ORDER BY cause_count DESC
               ) AS rk
        FROM (
            SELECT
                origin,
                month(fl_date) AS Month,
                dominant_cause,
                count(*)       AS cause_count
            FROM flights_with_cause
            WHERE dominant_cause IS NOT NULL
              AND dominant_cause != 'UNKNOWN'
            GROUP BY origin, month(fl_date), dominant_cause
        ) t
    """).filter("rk <= 3")

    print("\n--- PRIMERAS 10 FILAS DEL RESULTADO PARTE B (TOP 3 CAUSAS) ---")
    top_causes.show(10, truncate=False)

    top_causes.repartition(1).write.mode("overwrite").csv(
        "../data/results/3_2_delay_causes_sql", header=True
    )

    print("--- ¡Análisis 3.2 (Spark SQL) completado! ---")
    spark.stop()

if __name__ == "__main__":
    analyze_delay_report_sql()