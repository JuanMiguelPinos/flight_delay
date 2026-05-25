import os
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] += os.pathsep + "C:\\hadoop\\bin"

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.window import Window
from pyspark.sql.functions import rank as spark_rank
import pyspark.sql.functions as F

def categorize_delay(delay):
    if delay < 15:
        return "1_Low (<15m)"
    elif delay <= 60:
        return "2_Medium (15-60m)"
    else:
        return "3_High (>60m)"

def analyze_delay_report_rdd():
    spark = SparkSession.builder \
        .appName("DelayReport_SparkCore") \
        .master("local[1]") \
        .config("spark.driver.memory", "12g") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()

    print("--- Spark Session (Core/RDD) iniciada ---")
    data_path = "../data/processed/flights_cleaned.parquet"

    # ── PARTE A: rangos de retraso ──────────────────────────────────────────
    # Preprocesamos en SQL para evitar serializar objetos Date a Python
    from pyspark.sql.functions import month as sql_month
    df_prep = spark.read.parquet(data_path) \
        .select("origin", "fl_date", "dep_delay", "arr_delay") \
        .withColumn("month_num", sql_month(F.col("fl_date"))) \
        .select("origin", "month_num", "dep_delay", "arr_delay")

    df_prep.persist()
    df_prep.count()
    print("DataFrame persistido. Ejecutando Map/Reduce...")

    rdd = df_prep.rdd

    def map_row(row):
        airport   = row.origin    if row.origin    else "UNKNOWN"
        month     = row.month_num if row.month_num else 0
        dep_delay = float(row.dep_delay) if row.dep_delay is not None else 0.0
        arr_delay = float(row.arr_delay) if row.arr_delay is not None else 0.0
        category  = categorize_delay(dep_delay)
        return ((airport, month, category), (1, dep_delay, arr_delay))

    def reduce_func(v1, v2):
        return (v1[0] + v2[0], v1[1] + v2[1], v1[2] + v2[2])

    def format_result(item):
        airport, month, category = item[0]
        cnt, total_dep, total_arr = item[1]
        avg_dep = round(total_dep / cnt, 2) if cnt > 0 else 0.0
        avg_arr = round(total_arr / cnt, 2) if cnt > 0 else 0.0
        return (airport, month, category, cnt, avg_dep, avg_arr)

    # ✅ CLAVE: collect() en el driver — evita el crash del worker en Windows
    # El RDD reducido tiene ~24K filas, perfectamente manejable en memoria
    print("Ejecutando Map -> Reduce -> collect()...")
    collected_a = rdd \
        .map(map_row) \
        .reduceByKey(reduce_func, numPartitions=4) \
        .map(format_result) \
        .collect()

    print(f"Registros reducidos: {len(collected_a)}")

    schema_a = StructType([
        StructField("Departure_Airport", StringType(),  True),
        StructField("Month",             IntegerType(), True),
        StructField("Delay_Category",    StringType(),  True),
        StructField("Total_Flights",     IntegerType(), True),
        StructField("Avg_Dep_Delay",     DoubleType(),  True),
        StructField("Avg_Arr_Delay",     DoubleType(),  True),
    ])

    # Creamos el DataFrame desde lista local — sin re-ejecutar el pipeline Python
    result_df = spark.createDataFrame(collected_a, schema_a) \
                     .orderBy("Departure_Airport", "Month", "Delay_Category")

    print("\n--- PRIMERAS 10 FILAS PARTE A ---")
    result_df.show(10, truncate=False)

    result_df.repartition(1).write.mode("overwrite").csv(
        "../data/results/3_2_delay_report", header=True
    )
    df_prep.unpersist()

    # ── PARTE B: top 3 causas por (aeropuerto, mes) ─────────────────────────
    df2_prep = spark.read.parquet(data_path) \
        .select("origin", "fl_date", "dep_delay", "cancelled",
                "cancellation_code", "carrier_delay", "weather_delay",
                "nas_delay", "security_delay", "late_aircraft_delay") \
        .withColumn("month_num", sql_month(F.col("fl_date"))) \
        .select("origin", "month_num", "dep_delay", "cancelled",
                "cancellation_code", "carrier_delay", "weather_delay",
                "nas_delay", "security_delay", "late_aircraft_delay")

    df2_prep.persist()
    df2_prep.count()
    print("DataFrame causas persistido. Ejecutando Map/Reduce causas...")

    CANCEL_MAP = {"A": "CANCEL_CARRIER", "B": "CANCEL_WEATHER",
                  "C": "CANCEL_NAS",     "D": "CANCEL_SECURITY"}

    def map_cause(row):
        airport   = row.origin    if row.origin    else "UNKNOWN"
        month_val = row.month_num if row.month_num else 0
        dep       = float(row.dep_delay) if row.dep_delay is not None else 0.0
        cancelled = int(row.cancelled)   if row.cancelled is not None else 0

        if dep < 15 and cancelled == 0:
            return None

        if cancelled == 1:
            code  = row.cancellation_code or "UNKNOWN"
            cause = CANCEL_MAP.get(code, "CANCEL_UNKNOWN")
        else:
            delays = {
                "CARRIER":       float(row.carrier_delay       or 0),
                "WEATHER":       float(row.weather_delay       or 0),
                "NAS":           float(row.nas_delay           or 0),
                "SECURITY":      float(row.security_delay      or 0),
                "LATE_AIRCRAFT": float(row.late_aircraft_delay or 0),
            }
            best  = max(delays, key=delays.get)
            cause = best if delays[best] > 0 else "UNKNOWN"

        return ((airport, month_val, cause), 1)

    # ✅ Mismo patrón: collect() antes de pasar a DataFrame
    collected_b = df2_prep.rdd \
        .map(map_cause) \
        .filter(lambda x: x is not None) \
        .reduceByKey(lambda a, b: a + b) \
        .map(lambda x: (x[0][0], x[0][1], x[0][2], x[1])) \
        .collect()

    print(f"Registros causas reducidos: {len(collected_b)}")

    schema_b = StructType([
        StructField("Departure_Airport", StringType(),  True),
        StructField("Month",             IntegerType(), True),
        StructField("Cause",             StringType(),  True),
        StructField("Count",             IntegerType(), True),
    ])

    causes_df = spark.createDataFrame(collected_b, schema_b)

    w = Window.partitionBy("Departure_Airport", "Month").orderBy(F.col("Count").desc())
    top3 = causes_df.withColumn("rk", spark_rank().over(w)).filter("rk <= 3")

    print("\n--- PRIMERAS 10 FILAS PARTE B (TOP 3 CAUSAS) ---")
    top3.show(10, truncate=False)

    top3.repartition(1).write.mode("overwrite").csv(
        "../data/results/3_2_delay_causes_rdd", header=True
    )

    df2_prep.unpersist()
    print("--- ¡Análisis 3.2 (RDD) completado! ---")
    spark.stop()

if __name__ == "__main__":
    analyze_delay_report_rdd()