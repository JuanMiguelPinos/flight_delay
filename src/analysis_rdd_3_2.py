import os
import sys
from pathlib import Path

# -------------------------------------------------------------------
# Configuración de rutas
# -------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[1]

DATA_PATH = PROJECT_ROOT / "data" / "processed" / "flights_cleaned.parquet"
OUTPUT_BASE = PROJECT_ROOT / "data" / "results" / "spark_core" / "3_2"
SPARK_TMP = PROJECT_ROOT / "tmp" / "spark"

SPARK_TMP.mkdir(parents=True, exist_ok=True)

# -------------------------------------------------------------------
# Configuración Windows / Hadoop / Spark
# -------------------------------------------------------------------
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] += os.pathsep + "C:\\hadoop\\bin"
os.environ["SPARK_LOCAL_DIRS"] = str(SPARK_TMP)

# Usar exactamente el Python con el que se ejecuta este script
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


# -------------------------------------------------------------------
# Fracción de datos
# -------------------------------------------------------------------
# Por defecto ejecuta el 5% del dataset.
# Puedes cambiarlo al ejecutar:
#   python analysis_rdd.py 0.10
#   python analysis_rdd.py 0.25
#   python analysis_rdd.py 1.0
# -------------------------------------------------------------------
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
                "La fracción debe ser un número entre 0 y 1. "
                "Ejemplo: python analysis_rdd.py 0.05"
            )

    return DEFAULT_SAMPLE_FRACTION


def fraction_label(fraction):
    return f"{int(fraction * 100)}pct"


def build_spark():
    spark = (
        SparkSession.builder
        .appName("DelayReport_SparkCore_RDD")
        .master("local[2]")
        .config("spark.driver.memory", "12g")
        .config("spark.executor.memory", "4g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.default.parallelism", "8")
        .config("spark.local.dir", str(SPARK_TMP))
        .config("spark.python.worker.reuse", "true")
        .getOrCreate()
    )

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

    # Si no está cancelado y no tiene retraso relevante, no contamos causa
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
    output_path = OUTPUT_BASE / f"fraction_{fraction_label(sample_fraction)}"

    spark = build_spark()

    print("--- Spark Session (Core/RDD) iniciada ---")
    print(f"Leyendo datos desde: {DATA_PATH}")
    print(f"Fracción utilizada: {sample_fraction}")
    print(f"Salida: {output_path}")

    if not DATA_PATH.exists():
        raise FileNotFoundError(f"No existe el dataset procesado: {DATA_PATH}")

    # -------------------------------------------------------------------
    # Lectura y muestreo
    # -------------------------------------------------------------------
    df = spark.read.parquet(str(DATA_PATH))

    if sample_fraction < 1.0:
        df = df.sample(withReplacement=False, fraction=sample_fraction, seed=42)

    # -------------------------------------------------------------------
    # Preparación previa con DataFrame
    # -------------------------------------------------------------------
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
        .repartition(8)
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    total_rows = df_prep.count()
    print(f"Filas preparadas: {total_rows}")

    if total_rows == 0:
        print("No hay filas para procesar con esta fracción.")
        spark.stop()
        return

    base_rdd = df_prep.rdd

    # ===================================================================
    # PARTE A:
    # Estadísticas por aeropuerto, mes y rango de retraso
    # ===================================================================

    print("Ejecutando Map/Reduce Parte A: rangos de retraso...")

    def map_stats(row):
        airport = row.origin if row.origin else "UNKNOWN"
        month = safe_int(row.month_num)

        dep_delay = safe_float(row.dep_delay)
        arr_delay = safe_float(row.arr_delay)

        delay_category = categorize_delay(dep_delay)

        key = (airport, month, delay_category)
        value = (
            1,          # count
            dep_delay,  # sum dep delay
            arr_delay,  # sum arr delay
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
        .reduceByKey(reduce_stats, numPartitions=8)
        .map(format_stats)
    )

    # ===================================================================
    # PARTE B:
    # Top 3 causas por aeropuerto, mes y rango de retraso
    # ===================================================================

    print("Ejecutando Map/Reduce Parte B: top 3 causas...")

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
        .reduceByKey(lambda a, b: a + b, numPartitions=8)
        .map(format_cause_count)
    )

    top_causes_rdd = (
        cause_counts_rdd
        .groupByKey(numPartitions=8)
        .mapValues(top3_causes_to_string)
    )

    # ===================================================================
    # JOIN final:
    # Unimos estadísticas y top 3 causas
    # ===================================================================

    print("Uniendo estadísticas con causas...")

    final_rdd = (
        stats_rdd
        .leftOuterJoin(top_causes_rdd, numPartitions=8)
        .map(lambda item: (
            item[0][0],                         # Departure_Airport
            int(item[0][1]),                    # Month
            item[0][2],                         # Delay_Category
            int(item[1][0][0]),                 # Total_Flights
            float(item[1][0][1]),               # Avg_Dep_Delay
            float(item[1][0][2]),               # Avg_Arr_Delay
            item[1][1] if item[1][1] else "",   # Top_3_Causes
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

    print("\n--- PRIMERAS 10 FILAS RESULTADO 3.2 RDD ---")
    result_df.show(10, truncate=False)

    print(f"Guardando resultados en: {output_path}")

    (
        result_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(str(output_path))
    )

    result_df.unpersist()
    df_prep.unpersist()

    print("--- ¡Análisis 3.2 (RDD) completado! ---")
    spark.stop()


if __name__ == "__main__":
    analyze_delay_report_rdd()