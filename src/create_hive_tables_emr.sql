CREATE DATABASE IF NOT EXISTS flights_db;
USE flights_db;

DROP VIEW  IF EXISTS flights_clean;
DROP TABLE IF EXISTS flights_clean_parquet;

CREATE EXTERNAL TABLE flights_clean_parquet (
    fl_date               DATE,
    op_unique_carrier     STRING,
    origin                STRING,
    dest                  STRING,
    dep_delay             DOUBLE,
    arr_delay             DOUBLE,
    cancelled             INT,
    cancellation_code     STRING,
    carrier_delay         DOUBLE,
    weather_delay         DOUBLE,
    nas_delay             DOUBLE,
    security_delay        DOUBLE,
    late_aircraft_delay   DOUBLE
)
STORED AS PARQUET
LOCATION '${hiveconf:processed_path}';

CREATE VIEW flights_clean AS
SELECT
    UPPER(op_unique_carrier)                          AS airline,
    UPPER(origin)                                     AS origin,
    UPPER(dest)                                       AS dest,
    CONCAT(UPPER(origin), '-', UPPER(dest))           AS route,
    MONTH(fl_date)                                    AS month,
    COALESCE(dep_delay, 0.0)                          AS dep_delay,
    COALESCE(arr_delay, 0.0)                          AS arr_delay,
    COALESCE(cancelled, 0)                            AS cancelled,
    UPPER(COALESCE(cancellation_code, ''))            AS cancellation_code,
    CASE
        WHEN COALESCE(cancelled, 0) = 1 AND UPPER(COALESCE(cancellation_code,'')) = 'A'
            THEN 'CANCEL_CARRIER'
        WHEN COALESCE(cancelled, 0) = 1 AND UPPER(COALESCE(cancellation_code,'')) = 'B'
            THEN 'CANCEL_WEATHER'
        WHEN COALESCE(cancelled, 0) = 1 AND UPPER(COALESCE(cancellation_code,'')) = 'C'
            THEN 'CANCEL_NAS'
        WHEN COALESCE(cancelled, 0) = 1 AND UPPER(COALESCE(cancellation_code,'')) = 'D'
            THEN 'CANCEL_SECURITY'
        WHEN COALESCE(cancelled, 0) = 1
            THEN 'CANCEL_UNKNOWN'
        WHEN COALESCE(dep_delay, 0.0) < 15
            THEN 'UNKNOWN'
        WHEN COALESCE(carrier_delay, 0.0) >= GREATEST(
                COALESCE(weather_delay, 0.0), COALESCE(nas_delay, 0.0),
                COALESCE(security_delay, 0.0), COALESCE(late_aircraft_delay, 0.0))
             AND COALESCE(carrier_delay, 0.0) > 0
            THEN 'CARRIER'
        WHEN COALESCE(weather_delay, 0.0) >= GREATEST(
                COALESCE(carrier_delay, 0.0), COALESCE(nas_delay, 0.0),
                COALESCE(security_delay, 0.0), COALESCE(late_aircraft_delay, 0.0))
             AND COALESCE(weather_delay, 0.0) > 0
            THEN 'WEATHER'
        WHEN COALESCE(nas_delay, 0.0) >= GREATEST(
                COALESCE(carrier_delay, 0.0), COALESCE(weather_delay, 0.0),
                COALESCE(security_delay, 0.0), COALESCE(late_aircraft_delay, 0.0))
             AND COALESCE(nas_delay, 0.0) > 0
            THEN 'NAS'
        WHEN COALESCE(security_delay, 0.0) >= GREATEST(
                COALESCE(carrier_delay, 0.0), COALESCE(weather_delay, 0.0),
                COALESCE(nas_delay, 0.0), COALESCE(late_aircraft_delay, 0.0))
             AND COALESCE(security_delay, 0.0) > 0
            THEN 'SECURITY'
        WHEN COALESCE(late_aircraft_delay, 0.0) > 0
            THEN 'LATE_AIRCRAFT'
        ELSE 'UNKNOWN'
    END AS cause
FROM flights_clean_parquet;

SELECT COUNT(*) AS total_records FROM flights_clean;
