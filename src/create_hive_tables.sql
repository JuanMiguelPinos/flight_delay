-- =============================================================
-- SCRIPT DE CREACIÓN DE BASE DE DATOS Y TABLA EXTERNA EN HIVE
-- Ejecutar con:
--   docker exec -it hive4 beeline -u "jdbc:hive2://localhost:10000"
--   -f /opt/hive/user_scripts/create_hive_tables.sql
-- =============================================================

CREATE DATABASE IF NOT EXISTS flights_db;
USE flights_db;

DROP TABLE IF EXISTS flights_clean;

CREATE EXTERNAL TABLE IF NOT EXISTS flights_clean (
    airline             STRING,
    origin              STRING,
    dest                STRING,
    route               STRING,
    month               INT,
    dep_delay           FLOAT,
    arr_delay           FLOAT,
    cancelled           INT,
    cancellation_code   STRING,
    cause               STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION '/opt/hive/data/ext/flights_clean'
TBLPROPERTIES ("skip.header.line.count"="1");

-- Verificar carga
SELECT COUNT(*) AS total_records FROM flights_clean;