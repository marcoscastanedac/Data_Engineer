-- Creación de un script en hive
-- Autor: Marcos Castañeda
-- Date: 16/08/2024

-- Creamos la base de datos tripdata_bde
CREATE DATABASE tripdata_bde;

-- Creamos la tabla payments
CREATE TABLE IF NOT EXISTS payments (
    VendorID int, 
    tpep_pickup_datetetime date, 
    payment_type int, 
    total_amount double)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

-- Creamos la tabla passengers
CREATE TABLE IF NOT EXISTS passengers (
    tpep_pickup_datetetime date, 
    passenger_count int, 
    total_amount double)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

-- Creamos la tabla tolls
CREATE TABLE IF NOT EXISTS tolls (
    tpep_pickup_datetetime date, 
    passenger_count int,
    tolls_amount double, 
    total_amount double)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

-- Creamos la tabla congestion
CREATE TABLE IF NOT EXISTS congestion (
    tpep_pickup_datetetime date, 
    passenger_count int,
    congestion_surcharge double,
    total_amount double)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

-- Creamos la tabla distance
CREATE TABLE IF NOT EXISTS distance (
    tpep_pickup_datetetime date, 
    passenger_count int,
    trip_distance double, 
    total_amount double)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

