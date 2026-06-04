USE flights_db;

SELECT
    airline                                        AS Airline_Code,
    origin                                         AS Departure_Airport,
    COUNT(*)                                       AS Total_Flights,
    MIN(arr_delay)                                 AS Min_Arrival_Delay,
    MAX(arr_delay)                                 AS Max_Arrival_Delay,
    ROUND(AVG(arr_delay), 2)                       AS Avg_Arrival_Delay,
    ROUND(SUM(cancelled) / COUNT(*) * 100, 2)      AS Cancellation_Rate_Pct,
    CONCAT_WS('-', SORT_ARRAY(COLLECT_SET(CAST(month AS STRING)))) AS Operating_Months
FROM flights_clean
GROUP BY airline, origin
ORDER BY airline, origin
LIMIT 10;

INSERT OVERWRITE DIRECTORY '${hiveconf:output_path}'
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
