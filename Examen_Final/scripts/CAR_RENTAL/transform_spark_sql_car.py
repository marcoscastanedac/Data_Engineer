
# Creación de un script en spark sql para el examen final.
# Autor: Marcos Castañeda.
# Date: 23/09/2024
# BDE

# Importamos las librerias a utilizar.
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import  col, trim, round, lower 

# Creamos la sesion para SPARK
sc = SparkContext('local')
spark = SparkSession (sc)

from pyspark.sql import HiveContext
hc = HiveContext(sc)

# Definimos los catalogos de letras acentuadas y sus equivalencias sin acento.
acentos = "áéíóúÁÉÍÓÚ"
sin_acentos = "aeiouAEIOU"

#Creamos un df a partir de la ingesta del archivos indicados.
dfcar = spark.read.option("header","true").option("delimiter", ",").csv("hdfs://172.17.0.2:9000/ingest/CarRentalData.csv")
dfuscar = spark.read.option("header","true").option("delimiter", ";").csv("hdfs://172.17.0.2:9000/ingest/georef-united-states-of-america-state.csv")

#dfcar.count()
# 5851
#dfuscar.count()
# 56

# Realizamos el proceso de limpieza de los datos y transformaciones.

# Eliminamos las columnas que no vamos a necesitar y remplazamos valores NULL por 0 (ceros)
dfcar_clean = (dfcar
                 .dropDuplicates()
                 .drop("location.latitude","location.longitude"))
                 
dfuscar_clean = (dfuscar
                 .dropDuplicates()
                 .drop("Geo Point","Geo Shape","State FIPS Code","State GNIS Code"))

# Aplicamos limpieza de acentos a columnas del DF 2021 y 2022

# Creamos una vista para el DATAFRAME
dfcar_clean.createOrReplaceTempView("vdf_car")
dfuscar_clean.createOrReplaceTempView("vdf_uscar")


# Casteamos las columnas del DF respecto al esquema definido para la base de datos.
df_cast_car = spark.sql("""select 
                        cast(`fuelType` as string) as Fuel_Type,
                        cast(round(`rating`,0) as int) as Rating , 
                        cast(`renterTripsTaken` as int) as Renter_Trips, 
                        cast(`reviewCount` as int) as Review_Count , 
                        cast(`location.city` as string) as City, 
                        cast(`location.country` as string) as Country , 
                        cast(`location.state` as string) as State,
                        cast(`owner.id` as int) as Owner_ID, 
                        cast(`rate.daily` as int) as Rate_Daily, 
                        cast(`vehicle.make` as string)as Vahicule_Make, 
                        cast(`vehicle.model` as string) as Vahicule_Model,
                        cast(`vehicle.type` as string) as Vahicule_Type,
                        cast(`vehicle.year` as int) as Vehicule_Year 
                    from vdf_car""")

df_cast_uscar = spark.sql("""select 
                        cast(`Official Code State` as string) as Code_State, 
                        cast(`United States Postal Service state abbreviation` as string) as State,
                        cast(`Official Name State` as string) as Name_State, 
                        cast(`Iso 3166-3 Area Code` as string) as Area_Code , 
                        Type,
                        cast(`Year` as int) as Year 
                    from vdf_uscar """)


#Tratamos los valores 
df_cast_car = (df_cast_car
                 .fillna({'Fuel_Type': 'Other'})
                 .filter(df_cast_car['Rating'].isNotNull())
                 .withColumn('Fuel_Type', lower(col('Fuel_Type')))
                 .filter(col('State') != 'TX')
              ) 

df_cast_uscar = (df_cast_uscar
                 .filter(col('State') != 'TX')
                 ) 
                 
#df_cast_car.count()
# 5851
#df_cast_uscar.count()
# 16

#Unimos los 2 DF..
df_join = df_cast_car.join(df_cast_uscar, on = "State", how = "left") 

# Creamos una vista.
df_join.createOrReplaceTempView("v_join")

df_filter = spark.sql("""
                      SELECT 
                        Fuel_Type,
                        Rating,
                        Renter_Trips,
                        Review_Count,
                        City,
                        Name_State,
                        Owner_ID,
                        Rate_Daily,
                        Vahicule_Make,
                        Vahicule_Model,
                        Vehicule_Year
                      FROM v_join """)

#5851 registros

# Guardamos el DataFrame final como archivo CSV
#df_union.coalesce(1).write.mode("overwrite").option("header", "true").csv("D:/Desarrollo/GitHub/Data_Engineer/Examen_Final/data/final_anac.csv")

# Creamos una vista del filtro para que se cargue
df_filter.createOrReplaceTempView("v_final")

# Load de datos hacia HIVE
hc.sql("insert into car_rental_db.car_rental_analytics select * from v_final;")

#spark.sql("insert into car_rental_db.car_rental_analytics select * from v_final;")
