
# Creación de un script en spark sql para el examen final.
# Autor: Marcos Castañeda.
# Date: 23/09/2024
# BDE

# Importamos las librerias a utilizar.
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import translate, col, trim, when, ltrim

# Creamos la sesion para SPARK
sc = SparkContext('local')
spark = SparkSession (sc)

from pyspark.sql import HiveContext
hc = HiveContext(sc)

# Definimos los catalogos de letras acentuadas y sus equivalencias sin acento.
acentos = "áéíóúÁÉÍÓÚ"
sin_acentos = "aeiouAEIOU"

#Creamos un df a partir de la ingesta del archivos indicados.
df2021 = spark.read.option("header","true").option("delimiter", ";").csv("hdfs://172.17.0.2:9000/ingest/2021-informe-ministerio.csv")
df2022 = spark.read.option("header","true").option("delimiter", ";").csv("hdfs://172.17.0.2:9000/ingest/202206-informe-ministerio.csv")
dfairport = spark.read.option("header","true").option("delimiter", ";").csv("hdfs://172.17.0.2:9000/ingest/aeropuertos_detalle.csv")
#final = spark.read.option("header","true").option("delimiter", ",").csv("hdfs://172.17.0.2:9000/ingest/final_anac.csv")

# Vemos la descripción inicial de los df creados.
#df2021.describe()
#df2022.describe()
#dfairports.describe()

# Realizamos el proceso de limpieza de los datos y transformaciones.

# Eliminamos las columnas que no vamos a necesitar y eliminamos los registros duplicados
df2021_clean = (df2021
                 .dropDuplicates()
                 .drop("Calidad dato"))
                 
df2022_clean = (df2022
                 .dropDuplicates()
                 .drop("Calidad dato"))

df_airport_clean = (dfairport
                     .dropDuplicates()
                     .drop("inhab")
                     .drop("fir"))

# Aplicamos limpieza de acentos a columnas del DF 2021 y 2022

columns_to_clean = ["Clase de Vuelo (todos los vuelos)", "Clasificación Vuelo", "Tipo de Movimiento", "Aeropuerto","Origen / Destino","Aerolinea Nombre","Aeronave"]
# Recorremos todas las columnas indicadas en la lista anterior
for column in columns_to_clean:
    df2021_clean = df2021_clean.withColumn(column, translate(col(column), acentos, sin_acentos))
    
for column in columns_to_clean:    
    df2022_clean = df2022_clean.withColumn(column, translate(col(column), acentos, sin_acentos))

# Aplicamos limpieza de acentos a columnas del DataFrame de aeropuertos
airport_columns_to_clean = ["tipo","denominacion" ,"ref","provincia"]

for column in airport_columns_to_clean:
    df_airport_clean = df_airport_clean.withColumn(column, translate(col(column), acentos, sin_acentos))
  
# Creamos una vista para el DATAFRAME
df2021_clean.createOrReplaceTempView("vdf_2021")
df2022_clean.createOrReplaceTempView("vdf_2022")
df_airport_clean.createOrReplaceTempView("vdf_airports")

# Casteamos las columnas del DF respecto al esquema definido para la base de datos.
df_cast_2021 = spark.sql("""select 
                        to_date(`Fecha`, 'dd/MM/yyyy') as fecha,
                        cast(`Hora UTC` as string) as horaUTC , 
                        cast(`Clase de Vuelo (todos los vuelos)` as string) as clase_de_vuelo, 
                        cast(`Clasificación Vuelo` as string) as clasificacion_de_vuelo , 
                        cast(`Tipo de Movimiento` as string) as tipo_de_movimiento, 
                        cast(`Aeropuerto` as string) as aeropuerto , 
                        cast(`Origen / Destino` as string) as origen_destino, 
                        cast(`Aerolinea Nombre` as string) as aerolinea_nombre, 
                        cast(trim(`Aeronave`) as string) as aeronave, 
                        cast(`Pasajeros` as int) as pasajeros 
                    from vdf_2021""")

df_cast_2022 = spark.sql("""select 
                        to_date(`Fecha`, 'dd/MM/yyyy') as fecha,
                        cast(`Hora UTC` as string) as horaUTC , 
                        cast(`Clase de Vuelo (todos los vuelos)` as string) as clase_de_vuelo, 
                        cast(`Clasificación Vuelo` as string) as clasificacion_de_vuelo , 
                        cast(`Tipo de Movimiento` as string) as tipo_de_movimiento, 
                        cast(`Aeropuerto` as string) as aeropuerto , 
                        cast(`Origen / Destino` as string) as origen_destino, 
                        cast(`Aerolinea Nombre` as string) as aerolinea_nombre, 
                        cast(trim(`Aeronave`) as string) as aeronave, 
                        cast(`Pasajeros` as int) as pasajeros 
                    from vdf_2022""")

df_cast_airport = spark.sql("""select 
                        cast(`local` as string) as aeropuerto , 
                        cast(`oaci` as string) as oac, 
                        iata, 
                        tipo, 
                        denominacion, 
                        coordenadas, 
                        latitud, 
                        longitud,
                        cast(`elev` as float) , 
                        uom_elev,
                        ref,
                        cast(`distancia_ref` as float) ,
                        direccion_ref,
                        condicion,
                        control,
                        region,
                        uso,
                        trafico,
                        sna,
                        concesionado,
                        provincia                  
                    from vdf_airports""")

# Contamos los registros previamente de cada DF
#record_count_2021 = df_cast_2021.count()
# 327323 registros

#record_count_2022 = df_cast_2022.count()
# 222499 registros

#record_count_airports = df_cast_airports.count()
# 693 registros

#Tratamos los valores null por ceros (0)
df_cast_2021 = (df_cast_2021
                 .fillna({'pasajeros': 0})) 
                 
df_cast_2022 = (df_cast_2022
                .fillna({'pasajeros': 0})) 

df_cast_airport = (df_cast_airport
                     .fillna({'distancia_ref': 0}))

#Unimos los 2 DF de 2021 y 2022.
df_union = df_cast_2021.union(df_cast_2022) 

#record_count_f = df_union.count()
# 549822 total de registros de la union de los 2DF

# Creamos una vista.
df_union.createOrReplaceTempView("v_union")

# Guardamos el DataFrame final como archivo CSV
df_union.coalesce(1).write.mode("overwrite").option("header", "true").csv("hdfs://172.17.0.2:9000/ingest/final_anac.csv")

# Filtrar los vuelos internacionales ya que solamente se analizarán los vuelos domésticos.
df_filter = spark.sql("""
                      SELECT * 
                      FROM v_union 
                      WHERE clasificacion_de_vuelo = 'Domestico' """)
                        #and fecha between TO_DATE('01-01-2021', 'dd-MM-yyyy') and TO_DATE('30-06-2022', 'dd-MM-yyyy') """)
#df_filter.count()
#481891 registros

# Creamos una vista del filtro para que se cargue
df_filter.createOrReplaceTempView("v_load_flights")
df_cast_airport.createOrReplaceTempView("v_load_airport_details")

# Load de datos hacia HIVE
hc.sql("insert into anac_db.flights select * from v_load_flights;")
hc.sql("insert into anac_db.airport_details select * from v_load_airport_details;")

#spark.sql("insert into anac_db.flights select * from v_load_flights;")
#spark.sql("insert into anac_db.airport_details select * from v_load_airport_details;")