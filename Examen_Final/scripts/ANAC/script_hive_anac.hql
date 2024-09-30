-- Creación de un script en hive
-- Autor: Marcos Castañeda
-- Date: 17/09/2024

-- Creamos la base de datos anac_db
CREATE DATABASE anac_db;

-- Creamos la tabla flights
CREATE EXTERNAL TABLE flights (
    fecha date,
    horaUTC string, 
    clase_de_vuelo string, 
    clasificacion_de_vuelo string,
    tipo_de_movimiento string,
    aeropuerto string,
    origen_destino string,
    aerolinea_nombre string,
    aeronave string,
    pasajeros int)
COMMENT 'Flights Tables'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/tables/external/anac/flights';

-- Creamos la tabla airport_details
CREATE EXTERNAL TABLE airport_details (
    aeropuerto  string, 
    oac string, 
    iata string, 
    tipo string, 
    denominacion string, 
    coordenadas string, 
    latitud string, 
    longitud string,
    elev float, 
    uom_elev string,
    ref string,
    distancia_ref float,
    direccion_ref string,
    condicion string,
    control string,
    region string,
    uso string,
    trafico string,
    sna string,
    concesionado string,
    provincia string)     
COMMENT 'Airport details Tables'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/tables/external/anac/airport_details';
