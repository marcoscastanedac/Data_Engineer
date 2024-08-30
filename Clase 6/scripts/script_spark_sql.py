
# Creación de un script en spark sql
# Autor: Marcos Castañeda
# Date: 22/08/2024

# Creamos la sesion para SPARK
from pyspark.sql import SparkSession

# Resulve el punto 5
#Creamos un df a partir de la ingesta del archivo yellow_tripdata_2021-01.csv 
df = spark.read.option("header", "true").csv("/ingest/yellow_tripdata_2021-01.csv")

# Creamos una vista "vdf" para poner usar sentencias SQL
df_cast = spark.sql("select cast(tpep_pickup_datetime as date), cast(VendorID as int), cast(payment_type as int), cast(total_amount as double), cast(passenger_count as int), cast(tolls_amount as double), cast(congestion_surcharge as double), cast(trip_distance as float) from vdf")

# Creamos una vista ya casteada
df_cast.createOrReplaceTempView("v_cast")

#Creamos un df de filtro para los datos que se deben de cargar 
df_filter = spark.sql("select VendorID, tpep_pickup_datetime, payment_type, total_amount from v_cast where payment_type = 1") 

# Creamos una vista del filtro para que se cargue
df_filter.createOrReplaceTempView("v_load_payments")

#Load de datos
spark.sql("insert into tripdata_bde.payments select * from v_load_payments")

# Resulve el punto 6
#Creamos un df de filtro para los datos que se deben de cargar 
df_filter = spark.sql("select tpep_pickup_datetime, passenger_count,total_amount from v_cast where passenger_count > 2 and total_amount > 8")

# Creamos una vista del filtro para cargar
df_filter.createOrReplaceTempView("v_load_passengers")

#Load de datos
spark.sql("insert into tripdata_bde.passengers select * from v_load_passengers")

# Resulve el punto 7
#Creamos un df de filtro para los datos que se deben de cargar 
df_filter = spark.sql("select tpep_pickup_datetime, passenger_count, tolls_amount, total_amount from v_cast where passenger_count > 1 and tolls_amount > 0.1")

# Creamos una vista del filtro para cargar
df_filter.createOrReplaceTempView("v_load_tolls")

#Load de datos
spark.sql("insert into tripdata_bde.tolls select * from v_load_tolls")

# Resulve el punto 8
#Creamos un df de filtro para los datos que se deben de cargar 
df_filter = spark.sql("select tpep_pickup_datetime, passenger_count, congestion_surcharge, total_amount from v_cast where tpep_pickup_datetime = '2021-01-18' and congestion_surcharge > 0")

# Creamos una vista del filtro para cargar
df_filter.createOrReplaceTempView("v_load_congestion")

#Load de datos
spark.sql("insert into tripdata_bde.congestion select * from v_load_congestion")

# Resulve el punto 9
#Creamos un df de filtro para los datos que se deben de cargar 
df_filter = spark.sql("select tpep_pickup_datetime, passenger_count, trip_distance, total_amount from v_cast where tpep_pickup_datetime = '2020-12-31' and trip_distance > 15 and passenger_count = 1")

# Creamos una vista del filtro para cargar
df_filter.createOrReplaceTempView("v_load_distance")

#Load de datos
spark.sql("insert into tripdata_bde.distance select * from v_load_distance")