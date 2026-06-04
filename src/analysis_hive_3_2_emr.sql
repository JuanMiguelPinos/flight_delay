USE flights_db;

WITH stats AS (
    SELECT
        origin AS Departure_Airport,
        month AS Month,
        CASE
            WHEN COALESCE(dep_delay, 0) < 15  THEN '1_Low (<15m)'
            WHEN COALESCE(dep_delay, 0) <= 60 THEN '2_Medium (15-60m)'
            ELSE                                   '3_High (>60m)'
        END AS Delay_Category,
        COUNT(*) AS Total_Flights,
        ROUND(AVG(COALESCE(dep_delay, 0)), 2) AS Avg_Dep_Delay,
        ROUND(AVG(COALESCE(arr_delay, 0)), 2) AS Avg_Arr_Delay
    FROM flights_clean
    GROUP BY
        origin,
        month,
        CASE
            WHEN COALESCE(dep_delay, 0) < 15  THEN '1_Low (<15m)'
            WHEN COALESCE(dep_delay, 0) <= 60 THEN '2_Medium (15-60m)'
            ELSE                                   '3_High (>60m)'
        END
),
cause_counts AS (
    SELECT
        origin AS Departure_Airport,
        month AS Month,
        CASE
            WHEN COALESCE(dep_delay, 0) < 15  THEN '1_Low (<15m)'
            WHEN COALESCE(dep_delay, 0) <= 60 THEN '2_Medium (15-60m)'
            ELSE                                   '3_High (>60m)'
        END AS Delay_Category,
        cause,
        COUNT(*) AS Cause_Count
    FROM flights_clean
    WHERE cause IS NOT NULL AND cause != '' AND cause != 'UNKNOWN'
    GROUP BY
        origin,
        month,
        CASE
            WHEN COALESCE(dep_delay, 0) < 15  THEN '1_Low (<15m)'
            WHEN COALESCE(dep_delay, 0) <= 60 THEN '2_Medium (15-60m)'
            ELSE                                   '3_High (>60m)'
        END,
        cause
),
ranked_causes AS (
    SELECT
        Departure_Airport,
        Month,
        Delay_Category,
        cause,
        Cause_Count,
        ROW_NUMBER() OVER (
            PARTITION BY Departure_Airport, Month, Delay_Category
            ORDER BY Cause_Count DESC, cause ASC
        ) AS rk
    FROM cause_counts
),
top_causes AS (
    SELECT
        Departure_Airport,
        Month,
        Delay_Category,
        CONCAT_WS(
            '; ',
            SORT_ARRAY(COLLECT_LIST(CONCAT(CAST(rk AS STRING), ':', cause, ':', CAST(Cause_Count AS STRING))))
        ) AS Top_3_Causes
    FROM ranked_causes
    WHERE rk <= 3
    GROUP BY Departure_Airport, Month, Delay_Category
),
final_result AS (
    SELECT
        s.Departure_Airport,
        s.Month,
        s.Delay_Category,
        s.Total_Flights,
        s.Avg_Dep_Delay,
        s.Avg_Arr_Delay,
        COALESCE(t.Top_3_Causes, '') AS Top_3_Causes
    FROM stats s
    LEFT JOIN top_causes t
        ON s.Departure_Airport = t.Departure_Airport
       AND s.Month = t.Month
       AND s.Delay_Category = t.Delay_Category
)
SELECT *
FROM final_result
ORDER BY Departure_Airport, Month, Delay_Category
LIMIT 10;

WITH stats AS (
    SELECT
        origin AS Departure_Airport,
        month AS Month,
        CASE
            WHEN COALESCE(dep_delay, 0) < 15  THEN '1_Low (<15m)'
            WHEN COALESCE(dep_delay, 0) <= 60 THEN '2_Medium (15-60m)'
            ELSE                                   '3_High (>60m)'
        END AS Delay_Category,
        COUNT(*) AS Total_Flights,
        ROUND(AVG(COALESCE(dep_delay, 0)), 2) AS Avg_Dep_Delay,
        ROUND(AVG(COALESCE(arr_delay, 0)), 2) AS Avg_Arr_Delay
    FROM flights_clean
    GROUP BY
        origin,
        month,
        CASE
            WHEN COALESCE(dep_delay, 0) < 15  THEN '1_Low (<15m)'
            WHEN COALESCE(dep_delay, 0) <= 60 THEN '2_Medium (15-60m)'
            ELSE                                   '3_High (>60m)'
        END
),
cause_counts AS (
    SELECT
        origin AS Departure_Airport,
        month AS Month,
        CASE
            WHEN COALESCE(dep_delay, 0) < 15  THEN '1_Low (<15m)'
            WHEN COALESCE(dep_delay, 0) <= 60 THEN '2_Medium (15-60m)'
            ELSE                                   '3_High (>60m)'
        END AS Delay_Category,
        cause,
        COUNT(*) AS Cause_Count
    FROM flights_clean
    WHERE cause IS NOT NULL AND cause != '' AND cause != 'UNKNOWN'
    GROUP BY
        origin,
        month,
        CASE
            WHEN COALESCE(dep_delay, 0) < 15  THEN '1_Low (<15m)'
            WHEN COALESCE(dep_delay, 0) <= 60 THEN '2_Medium (15-60m)'
            ELSE                                   '3_High (>60m)'
        END,
        cause
),
ranked_causes AS (
    SELECT
        Departure_Airport,
        Month,
        Delay_Category,
        cause,
        Cause_Count,
        ROW_NUMBER() OVER (
            PARTITION BY Departure_Airport, Month, Delay_Category
            ORDER BY Cause_Count DESC, cause ASC
        ) AS rk
    FROM cause_counts
),
top_causes AS (
    SELECT
        Departure_Airport,
        Month,
        Delay_Category,
        CONCAT_WS(
            '; ',
            SORT_ARRAY(COLLECT_LIST(CONCAT(CAST(rk AS STRING), ':', cause, ':', CAST(Cause_Count AS STRING))))
        ) AS Top_3_Causes
    FROM ranked_causes
    WHERE rk <= 3
    GROUP BY Departure_Airport, Month, Delay_Category
),
final_result AS (
    SELECT
        s.Departure_Airport,
        s.Month,
        s.Delay_Category,
        s.Total_Flights,
        s.Avg_Dep_Delay,
        s.Avg_Arr_Delay,
        COALESCE(t.Top_3_Causes, '') AS Top_3_Causes
    FROM stats s
    LEFT JOIN top_causes t
        ON s.Departure_Airport = t.Departure_Airport
       AND s.Month = t.Month
       AND s.Delay_Category = t.Delay_Category
)
INSERT OVERWRITE DIRECTORY '${hiveconf:output_path}'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT *
FROM final_result
ORDER BY Departure_Airport, Month, Delay_Category;
