# Montar Google Drive para definir la ubicacion de los archivos
#drive.mount('/content/drive/')

# Lista de archivos CSV en Google Drive
files = [
    '/content/drive/MyDrive/CURSOS/DATA ENGINEER - EDVAI/Clase 1/Ejercicio Clase 1/Utimas Desvinculaciones - Row data.csv',
    '/content/drive/MyDrive/CURSOS/DATA ENGINEER - EDVAI/Clase 1/Ejercicio Clase 1/Utimas Desvinculaciones - Rangos.csv',
    '/content/drive/MyDrive/CURSOS/DATA ENGINEER - EDVAI/Clase 1/Ejercicio Clase 1/Utimas Desvinculaciones - Managers.csv'
]

# Leemos y concatenamos todos los archivos CSV en un solo DataFrame
df_list = [pd.read_csv(file) for file in files]
d_ir = pd.concat(df_list, ignore_index=True) # ignore_index=True sirve para definir un nuevo indice respecto a la union de los archivos

#from google.colab import drive
#drive.mount('/content/drive')

# Cruzar 2 variables para obtener mejor resultados
# Que me van ayudar los negocios 
# Ocultar los nombres y dejar los ID
# Procesamiento en paralelo con herramientas de Big Data

# Chismear
# Esxi Vmware

# En la actualidad todas las app corren sobre dockers y kubernetes
# Siempre levantar el ambiente de haddop

# docker import

# docker run --name edvai_postgres -e POSTGRES_PASSWORD=edvai -d -p 5432:5432 fedepineyro/edvai_postgres:v1

# e la variable
# -d la imagen queda encendida
#-p son los puertos
# imagen 

# docker ps - valida si esta corriendo el contendor
# docker ps -a lista los contendores
# docker stop edvai_hadoop
# docker inspect #nombre_del_contendor - valida las caracteristicas de mi contendor 
    # Importa en que dirección IP esta corriendo
# Se pueden copiar archivos de mi computadora a el contendor

# docker rmi <imagen>
# docker rm <contendor>

# docker commit edvai_postgres <nombre del contendor>:v1
# docker exec -it edvai_postgres /bin/bash EL COMANDO MAS IMPORTANTE QUE SE EJECUTA TODOS LOS DIAS A PRIMERA HORA






