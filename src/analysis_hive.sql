-- =================================================================
-- PROYECTO BIG DATA - ANÁLISIS HIVE
-- =================================================================

USE flights_db;

-- =================================================================
-- ANÁLISIS 3.1: Estadísticas por aerolínea y aeropuerto
-- =================================================================

-- Primeras 10 filas para el informe
SELECT
    airline                                        AS Airline_Code,
    origin                                         AS Departure_Airport,
    COUNT(*)                                       AS Total_Flights,
    MIN(arr_delay)                                 AS Min_Arrival_Delay,
    MAX(arr_delay)                                 AS Max_Arrival_Delay,
    ROUND(AVG(arr_delay), 2)                       AS Avg_Arrival_Delay,
    ROUND(SUM(cancelled) / COUNT(*) * 100, 2)      AS Cancellation_Rate_Pct,
    COLLECT_SET(month)                             AS Operating_Months
FROM flights_clean
GROUP BY airline, origin
ORDER BY airline, origin
LIMIT 10;

-- Resultado completo guardado en fichero (aparecerá en data/hive/results/3_1/)
INSERT OVERWRITE LOCAL DIRECTORY '/opt/hive/data/ext/results/3_1'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT
    airline,
    origin,
    COUNT(*)                                       AS Total_Flights,
    MIN(arr_delay)                                 AS Min_Arrival_Delay,
    MAX(arr_delay)                                 AS Max_Arrival_Delay,
    ROUND(AVG(arr_delay), 2)                       AS Avg_Arrival_Delay,
    ROUND(SUM(cancelled) / COUNT(*) * 100, 2)      AS Cancellation_Rate_Pct,
    CONCAT_WS('-', SORT_ARRAY(COLLECT_SET(CAST(month AS STRING)))) AS Operating_Months
FROM flights_clean
GROUP BY airline, origin
ORDER BY airline, origin;


-- =================================================================
-- ANÁLISIS 3.2: Reporte de retrasos por aeropuerto y mes
-- =================================================================

-- Paso 1: contar vuelos por (aeropuerto, mes, categoría) con medias
-- Primeras 10 filas para el informe
SELECT
    origin                                                  AS Departure_Airport,
    month                                                   AS Month,
    CASE
        WHEN COALESCE(dep_delay, 0) < 15            THEN '1_Low (<15m)'
        WHEN COALESCE(dep_delay, 0) <= 60           THEN '2_Medium (15-60m)'
        ELSE                                             '3_High (>60m)'
    END                                                     AS Delay_Category,
    COUNT(*)                                                AS Total_Flights,
    ROUND(AVG(COALESCE(dep_delay, 0)), 2)                   AS Avg_Dep_Delay,
    ROUND(AVG(COALESCE(arr_delay, 0)), 2)                   AS Avg_Arr_Delay
FROM flights_clean
GROUP BY
    origin,
    month,
    CASE
        WHEN COALESCE(dep_delay, 0) < 15  THEN '1_Low (<15m)'
        WHEN COALESCE(dep_delay, 0) <= 60 THEN '2_Medium (15-60m)'
        ELSE                                   '3_High (>60m)'
    END
ORDER BY origin, month, Delay_Category
LIMIT 10;

-- Paso 2: top 3 causas por (aeropuerto, mes)
-- Primeras 10 filas para el informe
SELECT origin, month, cause, cause_count
FROM (
    SELECT
        origin,
        month,
        cause,
        COUNT(*) AS cause_count,
        ROW_NUMBER() OVER (
            PARTITION BY origin, month
            ORDER BY COUNT(*) DESC
        ) AS rk
    FROM flights_clean
    WHERE cause IS NOT NULL AND cause != '' AND cause != 'UNKNOWN'
    GROUP BY origin, month, cause
) ranked
WHERE rk <= 3
ORDER BY origin, month, rk
LIMIT 10;

-- Resultado completo 3.2 parte A (rangos de retraso)
INSERT OVERWRITE LOCAL DIRECTORY '/opt/hive/data/ext/results/3_2_ranges'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT
    origin,
    month,
    CASE
        WHEN COALESCE(dep_delay, 0) < 15  THEN '1_Low (<15m)'
        WHEN COALESCE(dep_delay, 0) <= 60 THEN '2_Medium (15-60m)'
        ELSE                                   '3_High (>60m)'
    END                                        AS Delay_Category,
    COUNT(*)                                   AS Total_Flights,
    ROUND(AVG(COALESCE(dep_delay, 0)), 2)      AS Avg_Dep_Delay,
    ROUND(AVG(COALESCE(arr_delay, 0)), 2)      AS Avg_Arr_Delay
FROM flights_clean
GROUP BY
    origin, month,
    CASE
        WHEN COALESCE(dep_delay, 0) < 15  THEN '1_Low (<15m)'
        WHEN COALESCE(dep_delay, 0) <= 60 THEN '2_Medium (15-60m)'
        ELSE                                   '3_High (>60m)'
    END
ORDER BY origin, month, Delay_Category;

-- Resultado completo 3.2 parte B (top 3 causas)
INSERT OVERWRITE LOCAL DIRECTORY '/opt/hive/data/ext/results/3_2_causes'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT origin, month, cause, cause_count, rk
FROM (
    SELECT
        origin,
        month,
        cause,
        COUNT(*) AS cause_count,
        ROW_NUMBER() OVER (
            PARTITION BY origin, month
            ORDER BY COUNT(*) DESC
        ) AS rk
    FROM flights_clean
    WHERE cause IS NOT NULL AND cause != ''
    GROUP BY origin, month, cause
) ranked
WHERE rk <= 3
ORDER BY origin, month, rk;


-- =================================================================
-- ANÁLISIS 3.3: Ranking aerolínea-aeropuerto con comportamiento anómalo
-- =================================================================

-- Primeras 10 filas para el informe
SELECT
    f.origin                                                  AS Departure_Airport,
    f.airline                                                 AS Airline,
    COUNT(*)                                                  AS Total_Flights,
    ROUND(AVG(COALESCE(f.dep_delay, 0)), 2)                   AS Avg_Dep_Delay,
    ROUND(AVG(COALESCE(f.arr_delay, 0)), 2)                   AS Avg_Arr_Delay,
    ROUND(SUM(f.cancelled) / COUNT(*) * 100, 2)               AS Cancellation_Rate_Pct,
    ROUND(AVG(COALESCE(f.dep_delay, 0)) - ap.avg_airport_dep, 2)  AS Diff_Vs_Airport_Avg,
    RANK() OVER (
        PARTITION BY f.origin
        ORDER BY AVG(COALESCE(f.dep_delay, 0)) ASC
    )                                                         AS Ranking_At_Airport
FROM flights_clean f
JOIN (
    SELECT
        origin,
        ROUND(AVG(COALESCE(dep_delay, 0)), 2) AS avg_airport_dep
    FROM flights_clean
    GROUP BY origin
) ap ON f.origin = ap.origin
GROUP BY f.origin, f.airline, ap.avg_airport_dep
ORDER BY f.origin, Ranking_At_Airport
LIMIT 10;

-- Resultado completo 3.3
INSERT OVERWRITE LOCAL DIRECTORY '/opt/hive/data/ext/results/3_3'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT
    f.origin,
    f.airline,
    COUNT(*)                                                      AS Total_Flights,
    ROUND(AVG(COALESCE(f.dep_delay, 0)), 2)                       AS Avg_Dep_Delay,
    ROUND(AVG(COALESCE(f.arr_delay, 0)), 2)                       AS Avg_Arr_Delay,
    ROUND(SUM(f.cancelled) / COUNT(*) * 100, 2)                   AS Cancellation_Rate_Pct,
    ROUND(AVG(COALESCE(f.dep_delay, 0)) - ap.avg_airport_dep, 2)  AS Diff_Vs_Airport_Avg,
    RANK() OVER (
        PARTITION BY f.origin
        ORDER BY AVG(COALESCE(f.dep_delay, 0)) ASC
    )                                                             AS Ranking_At_Airport
FROM flights_clean f
JOIN (
    SELECT
        origin,
        ROUND(AVG(COALESCE(dep_delay, 0)), 2) AS avg_airport_dep
    FROM flights_clean
    GROUP BY origin
) ap ON f.origin = ap.origin
GROUP BY f.origin, f.airline, ap.avg_airport_dep
ORDER BY f.origin, Ranking_At_Airport;