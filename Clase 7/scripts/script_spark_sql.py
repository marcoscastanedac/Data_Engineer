
# Creación de un script en spark sql para la practica #7
# Autor: Marcos Castañeda
# Date: 03/09/2024

# Creamos la sesion para SPARK
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext('local')
spark = SparkSession (sc)

from pyspark.sql import HiveContext
hc = HiveContext(sc)

#Creamos un df a partir de la ingesta del archivo yellow_tripdata_2021-01.csv 
df1 = spark.read.parquet("hdfs://172.17.0.2:9000/ingest/yellow_tripdata_2021-01.parquet")
df2 = spark.read.parquet("hdfs://172.17.0.2:9000/ingest/yellow_tripdata_2021-02.parquet")

# Contamos los registros previamente de cada DF

#record_count_1 = df1.count()
# 1369769 registros
#record_count_2 = df2.count()
# 1371709 registros

df_union = df1.union(df2) 
#record_count_f = df_union.count()
# 2741478 registros

# Creamos una vista
df_union.createOrReplaceTempView("v_union")

#Creamos un df de filtro para los datos que se deben de cargar 
df_filter = spark.sql("select tpep_pickup_datetime,airport_fee,payment_type,tolls_amount,total_amount from v_union where YEAR(tpep_pickup_datetime) = 2021 and store_and_fwd_flag = 'Y' and payment_type = 2") 

#record_filter = df_filter.count()
# 2737976 registros
# 631,260
# 9478

# Creamos una vista del filtro para que se cargue
df_filter.createOrReplaceTempView("v_load_airport_trips")

#Load de datos hacia HIVE
hc.sql("insert into tripdata_bde.airport_trips select * from v_load_airport_trips;")

