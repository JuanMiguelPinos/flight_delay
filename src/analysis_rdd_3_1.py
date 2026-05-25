import os
import sys
from pathlib import Path

python_exe = sys.executable
os.environ["PYSPARK_PYTHON"] = python_exe
os.environ["PYSPARK_DRIVER_PYTHON"] = python_exe

# -------------------------------------------------------------------
# Configuración de rutas
# -------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[1]

DATA_PATH = PROJECT_ROOT / "data" / "processed" / "flights_cleaned.parquet"
OUTPUT_BASE = PROJECT_ROOT / "data" / "results" / "spark_core" / "3_1"
SPARK_TMP = PROJECT_ROOT / "tmp" / "spark"

SPARK_TMP.mkdir(parents=True, exist_ok=True)

# -------------------------------------------------------------------
# Configuración Windows / Hadoop / Spark
# -------------------------------------------------------------------
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] += os.pathsep + "C:\\hadoop\\bin"
os.environ["SPARK_LOCAL_DIRS"] = str(SPARK_TMP)

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
#   python analysis_rdd_3_1.py 0.10
#   python analysis_rdd_3_1.py 0.25
#   python analysis_rdd_3_1.py 1.0
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
                "Ejemplo: python analysis_rdd_3_1.py 0.05"
            )

    return DEFAULT_SAMPLE_FRACTION


def fraction_label(fraction):
    return f"{int(fraction * 100)}pct"


def build_spark():
    spark = (
        SparkSession.builder
        .appName("AirlineStatistics_SparkCore_RDD")
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


# -------------------------------------------------------------------
# Máscara de bits para los meses
# -------------------------------------------------------------------
# En vez de guardar frozenset([month]) por cada fila, usamos un entero.
#
# Ejemplo:
#   enero  -> bit 1
#   febrero -> bit 2
#   ...
#   diciembre -> bit 12
#
# Para unir meses de dos registros:
#   mask1 | mask2
# -------------------------------------------------------------------
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
        .select("op_unique_carrier", "origin", "arr_delay", "cancelled", "fl_date")
        .withColumn("month_num", sql_month(F.col("fl_date")))
        .select("op_unique_carrier", "origin", "arr_delay", "cancelled", "month_num")
        .repartition(8)
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    total_rows = df_prep.count()
    print(f"Filas preparadas: {total_rows}")

    if total_rows == 0:
        print("No hay filas para procesar con esta fracción.")
        spark.stop()
        return

    print("Ejecutando Map/Reduce con Spark Core...")

    # -------------------------------------------------------------------
    # MAP
    # -------------------------------------------------------------------
    def map_row(row):
        airline = row.op_unique_carrier if row.op_unique_carrier else "UNKNOWN"
        airport = row.origin if row.origin else "UNKNOWN"

        arr_delay = float(row.arr_delay) if row.arr_delay is not None else 0.0
        cancelled = int(row.cancelled) if row.cancelled is not None else 0

        month_mask = month_to_mask(row.month_num)

        return (
            (airline, airport),
            (
                1,              # count
                arr_delay,      # sum_arr
                arr_delay,      # min_arr
                arr_delay,      # max_arr
                cancelled,      # cancel_sum
                month_mask,     # months mask
            ),
        )

    # -------------------------------------------------------------------
    # REDUCE
    # -------------------------------------------------------------------
    def reduce_func(v1, v2):
        return (
            v1[0] + v2[0],          # count
            v1[1] + v2[1],          # sum_arr
            min(v1[2], v2[2]),      # min_arr
            max(v1[3], v2[3]),      # max_arr
            v1[4] + v2[4],          # cancel_sum
            v1[5] | v2[5],          # union meses con OR binario
        )

    # -------------------------------------------------------------------
    # FORMAT
    # -------------------------------------------------------------------
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
        .reduceByKey(reduce_func, numPartitions=8)
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

    # -------------------------------------------------------------------
    # Crear DataFrame directamente desde RDD
    # Sin collect()
    # Sin orderBy() global
    # -------------------------------------------------------------------
    result_df = spark.createDataFrame(result_rdd, schema)

    print("\n--- PRIMERAS 10 FILAS ---")
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

    df_prep.unpersist()

    print("--- ¡Análisis 3.1 (RDD) completado! ---")
    spark.stop()


if __name__ == "__main__":
    analyze_airline_statistics_rdd()