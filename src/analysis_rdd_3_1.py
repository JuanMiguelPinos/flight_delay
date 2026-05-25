import os
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] += os.pathsep + "C:\\hadoop\\bin"

from pyspark.sql import SparkSession
from pyspark.sql.types import (StructType, StructField, StringType,
                                IntegerType, DoubleType, ArrayType)
from pyspark.sql.functions import month as sql_month
import pyspark.sql.functions as F

def analyze_airline_statistics_rdd():
    spark = SparkSession.builder \
        .appName("AirlineStatistics_SparkCore") \
        .master("local[1]") \
        .config("spark.driver.memory", "12g") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()

    print("--- Spark Session (Core/RDD) iniciada ---")
    data_path = "../data/processed/flights_cleaned.parquet"

    # Preprocesar mes en SQL antes de pasar a RDD (evita crash en Windows)
    df_prep = spark.read.parquet(data_path) \
        .select("op_unique_carrier", "origin", "arr_delay", "cancelled", "fl_date") \
        .withColumn("month_num", sql_month(F.col("fl_date"))) \
        .select("op_unique_carrier", "origin", "arr_delay", "cancelled", "month_num")

    df_prep.persist()
    df_prep.count()
    print("DataFrame persistido. Ejecutando Map/Reduce...")

    rdd = df_prep.rdd

    # FASE MAP
    # Clave: (airline, airport)
    # Valor: (count, arr_delay, arr_delay, arr_delay, cancelled, {month})
    #         para calcular: total, sum, min, max, cancel_sum, months_set
    def map_row(row):
        airline   = row.op_unique_carrier if row.op_unique_carrier else "UNKNOWN"
        airport   = row.origin            if row.origin            else "UNKNOWN"
        arr_delay = float(row.arr_delay)  if row.arr_delay is not None else 0.0
        cancelled = int(row.cancelled)    if row.cancelled is not None else 0
        month     = row.month_num         if row.month_num          else 0

        # Valor: (count, sum_arr, min_arr, max_arr, cancel_sum, months_frozenset)
        return ((airline, airport), (1, arr_delay, arr_delay, arr_delay,
                                     cancelled, frozenset([month])))

    # FASE REDUCE
    def reduce_func(v1, v2):
        return (
            v1[0] + v2[0],                      # count
            v1[1] + v2[1],                      # sum_arr
            min(v1[2], v2[2]),                  # min_arr
            max(v1[3], v2[3]),                  # max_arr
            v1[4] + v2[4],                      # cancel_sum
            v1[5] | v2[5],                      # months union
        )

    # FASE FORMAT
    def format_result(item):
        airline, airport = item[0]
        count, sum_arr, min_arr, max_arr, cancel_sum, months = item[1]
        avg_arr      = round(sum_arr / count, 2) if count > 0 else 0.0
        cancel_rate  = round((cancel_sum / count) * 100, 2) if count > 0 else 0.0
        months_str   = ",".join(str(m) for m in sorted(months))
        return (airline, airport, count, min_arr, max_arr, avg_arr,
                cancel_rate, months_str)

    print("Ejecutando Map -> Reduce -> collect()...")
    collected = rdd \
        .map(map_row) \
        .reduceByKey(reduce_func, numPartitions=4) \
        .map(format_result) \
        .collect()

    print(f"Pares (aerolínea, aeropuerto) únicos: {len(collected)}")

    schema = StructType([
        StructField("Airline_Code",          StringType(),  True),
        StructField("Departure_Airport",     StringType(),  True),
        StructField("Total_Flights",         IntegerType(), True),
        StructField("Min_Arrival_Delay",     DoubleType(),  True),
        StructField("Max_Arrival_Delay",     DoubleType(),  True),
        StructField("Avg_Arrival_Delay",     DoubleType(),  True),
        StructField("Cancellation_Rate_Pct", DoubleType(),  True),
        StructField("Operating_Months",      StringType(),  True),
    ])

    result_df = spark.createDataFrame(collected, schema) \
                     .orderBy("Airline_Code", "Departure_Airport")

    print("\n--- PRIMERAS 10 FILAS ---")
    result_df.show(10, truncate=False)

    result_df.repartition(1).write.mode("overwrite").csv(
        "../data/results/spark_core/3_1", header=True
    )

    df_prep.unpersist()
    print("--- ¡Análisis 3.1 (RDD) completado! ---")
    spark.stop()

if __name__ == "__main__":
    analyze_airline_statistics_rdd()