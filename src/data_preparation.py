import os
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] += os.pathsep + "C:\\hadoop\\bin"

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def prepare_data():
    # 1. Inicializar la sesión de Spark
    spark = SparkSession.builder \
        .appName("FlightDataPreparation") \
        .master("local[*]") \
        .getOrCreate()

    print("--- Spark Session iniciada con éxito ---")

    # 2. Cargar el dataset crudo
    raw_data_path = "../data/raw/flight_data_2024.csv" 
    
    print(f"Cargando datos desde: {raw_data_path}")
    df_raw = spark.read.csv(raw_data_path, header=True, inferSchema=True)
    print(f"Total de registros originales: {df_raw.count()}")

    # 3. Limpieza y Selección de Datos usando LOS NOMBRES CORRECTOS DEL CSV
    columnas_clave = [
    "fl_date", "op_unique_carrier", "origin", "dest",
    "dep_delay", "arr_delay", "cancelled",
    "cancellation_code",    # para causas de cancelación
    "carrier_delay",        # causa: aerolínea
    "weather_delay",        # causa: meteorología
    "nas_delay",            # causa: sistema aéreo nacional
    "security_delay",       # causa: seguridad
    "late_aircraft_delay",  # causa: avión con retraso previo
    ]
    
    # Filtramos para quedarnos solo con estas columnas clave
    columnas_existentes = [c for c in columnas_clave if c in df_raw.columns]
    df_cleaned = df_raw.select(columnas_existentes)

    # Limpieza: 
    # ✅ Lo correcto es MANTENER los cancelados (los necesitas para calcular
    # la tasa de cancelación en 3.1 y 3.3) y actualizar el comentario:
    df_cleaned = df_cleaned.fillna(0, subset=["dep_delay", "arr_delay",
        "carrier_delay", "weather_delay", "nas_delay",
        "security_delay", "late_aircraft_delay"])
    # Nota: los vuelos cancelados SE MANTIENEN intencionalmente para calcular
    # cancellation_rate en los análisis 3.1 y 3.3

    print(f"Total de registros tras la limpieza (sin cancelados): {df_cleaned.count()}")

    # 4. Guardar los datos en formato optimizado (Parquet)
    processed_data_path = "../data/processed/flights_cleaned.parquet"
    print(f"Guardando datos procesados en: {processed_data_path}")
    
    df_cleaned.repartition(4).write.mode("overwrite").parquet(processed_data_path)

    print("--- ¡Preparación de datos completada con éxito! ---")
    spark.stop()

if __name__ == "__main__":
    prepare_data()