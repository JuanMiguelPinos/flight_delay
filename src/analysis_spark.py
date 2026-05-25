import os
# Mantenemos las variables de entorno para que Windows no falle al guardar
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] += os.pathsep + "C:\\hadoop\\bin"

from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws

def analyze_airline_statistics():
    # Inicializamos Spark
    spark = SparkSession.builder \
        .appName("AirlineStatistics_SparkSQL") \
        .master("local[*]") \
        .getOrCreate()

    print("--- Spark Session iniciada ---")
    
    # 1. Cargar los datos procesados en formato Parquet
    data_path = "../data/processed/flights_cleaned.parquet"
    df = spark.read.parquet(data_path)
    
    # 2. Registrar el DataFrame como una vista SQL temporal
    df.createOrReplaceTempView("flights")

    # 3. La consulta SQL que cumple el 100% de los requisitos del apartado 3.1
    query = """
        SELECT 
            op_unique_carrier AS Airline_Code,
            origin AS Departure_Airport,
            COUNT(*) AS Total_Flights,
            MIN(arr_delay) AS Min_Arrival_Delay,
            MAX(arr_delay) AS Max_Arrival_Delay,
            ROUND(AVG(arr_delay), 2) AS Avg_Arrival_Delay,
            ROUND((SUM(cancelled) / COUNT(*)) * 100, 2) AS Cancellation_Rate_Pct,
            array_sort(collect_set(month(CAST(fl_date AS TIMESTAMP)))) AS Operating_Months
        FROM flights
        GROUP BY op_unique_carrier, origin
        ORDER BY Airline_Code, Departure_Airport
    """
    
    print("Ejecutando Análisis 3.1 con Spark SQL...")
    result_df = spark.sql(query)
    
    # Mostrar las primeras 10 filas (Obligatorio incluirlo en tu reporte final)
    print("\n--- PRIMERAS 10 FILAS DEL RESULTADO (Cópialas para tu informe PDF) ---")
    result_df.show(10, truncate=False)

    # 4. Guardar los resultados en un CSV para que los tengas a mano
    output_path = "../data/results/3_1_airline_statistics"
    print(f"Guardando reporte final en: {output_path}")
    
    # repartition(1) fuerza a que se guarde como un único archivo CSV en lugar de varios
    result_df.withColumn("Operating_Months", concat_ws(", ", "Operating_Months")) \
             .repartition(1).write.mode("overwrite").csv(output_path, header=True)
    
    print("--- ¡Análisis 3.1 completado! ---")
    spark.stop()

if __name__ == "__main__":
    analyze_airline_statistics()