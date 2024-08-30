
# Creación de un script en spark
# Autor: Marcos Castañeda
# Date: 16/08/2024

# Creamos la sesion para SPARK
from pyspark.sql import SparkSession

# Resulve el punto 5
#Creamos un df a partir de la ingesta del archivo yellow_tripdata_2021-01.csv 
df = spark.read.option("header", "true").csv("/ingest/yellow_tripdata_2021-01.csv")

#Creamos un df de casteo para los tipos de datos del archivo yellow_tripdata_2021-01.csv 
df_cast = df.select(df.VendorID.cast("int"), df.tpep_pickup_datetime.cast("date"), df.payment_type.cast("int"), df.total_amount.cast("double"))

#Creamos un df de filtro para los datos que se deben de cargar 
df_filter = df_cast.filter(df_cast.payment_type == 1) 

#Load de datos
df_filter.write.insertInto("tripdata_bde.payments")

# Resulve el punto 6
#Creamos un df de casteo para los tipos de datos del archivo yellow_tripdata_2021-01.csv 
df_cast = df.select(df.tpep_pickup_datetime.cast("date"), df.passenger_count.cast("int"), df.total_amount.cast("double"))

#Creamos un df de filtro para los datos que se deben de cargar 
df_filter = df_cast.filter((df_cast.passenger_count > 2) & (df_cast.total_amount > 8)) 

#Load de datos
df_filter.write.insertInto("tripdata_bde.passengers")

# Resulve el punto 7
#Creamos un df de casteo para los tipos de datos del archivo yellow_tripdata_2021-01.csv 
df_cast = df.select(df.tpep_pickup_datetime.cast("date"), df.passenger_count.cast("int"),df.tolls_amount.cast("double") ,df.total_amount.cast("double"))

#Creamos un df de filtro para los datos que se deben de cargar 
df_filter = df_cast.filter((df_cast.passenger_count > 1) & (df_cast.tolls_amount > 0.1)) 

#Load de datos
df_filter.write.insertInto("tripdata_bde.tolls")

# Resulve el punto 8
#Creamos un df de casteo para los tipos de datos del archivo yellow_tripdata_2021-01.csv 
df_cast = df.select(df.tpep_pickup_datetime.cast("date"), df.passenger_count.cast("int"), df.congestion_surcharge.cast("double") ,df.total_amount.cast("double"))

#Creamos un df de filtro para los datos que se deben de cargar 
df_filter = df_cast.filter((df_cast.tpep_pickup_datetime == '2021-01-18') & (df.congestion_surcharge > 0)) 

#Load de datos
df_filter.write.insertInto("tripdata_bde.congestion")

# Resulve el punto 9
#Creamos un df de casteo para los tipos de datos del archivo yellow_tripdata_2021-01.csv 
df_cast = df.select(df.tpep_pickup_datetime.cast("date"), df.passenger_count.cast("int"), df.trip_distance.cast("double") ,df.total_amount.cast("double"))

#Creamos un df de filtro para los datos que se deben de cargar 
df_filter = df_cast.filter((df_cast.tpep_pickup_datetime == '2020-12-31') & (df.passenger_count == 1) & (df.trip_distance > 15)) 

#Load de datos
df_filter.write.insertInto("tripdata_bde.distance")