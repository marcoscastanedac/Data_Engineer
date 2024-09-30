-- Creación de un script en hive
-- Autor: Marcos Castañeda
-- Date: 23/09/2024

-- Creamos la base de datos car_rental_db
CREATE DATABASE car_rental_db;

-- Creamos la tabla car_rental_analytics,
CREATE EXTERNAL TABLE car_rental_analytics (
    fuelType string,
    rating int, 
    renterTripsTaken int, 
    reviewCount int,
    city string,
    state_name string,
    owner_id int,
    rate_daily int,
    make string,
    model string,
    year int)
COMMENT 'Car_Rental Tables'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/tables/external/car_rental/car_rental_analytics';