# Se determina el intérprete que debe usar para ejecutar el script
#!/bin/bash 

# Configuración de variables
# Ruta definida en la variable $HADOOP_HOME.
HADOOP_HOME=/home/hadoop/hadoop
# El directorio temporal donde se descargará el archivo.
LOCAL_DIR=/home/hadoop/landing
# El directorio en HDFS donde se moverá el archivo.
HDFS_DIR=/ingest
# La URL desde donde se descagará el archivo.
FILE_URL=https://dataengineerpublic.blob.core.windows.net/data-engineer/yellow_tripdata_2021-01.csv
# El nombre del archivo que se descargará.
FILE_NAME=yellow_tripdata_2021-01.csv

# Verifica si el directorio temporal existe y lo crea si no es así
if [ ! -d "$LOCAL_DIR" ]; then
  mkdir -p $LOCAL_DIR
fi

# Descargar el archivo, usa wget para descargar el archivo desde la URL proporcionada y guarda el archivo con el nombre yellow_tripdata_2021-01.csv.
cd $LOCAL_DIR
wget -O $FILE_NAME $FILE_URL

# Verificar si la descarga fue exitosa
if [ ! -f "$LOCAL_DIR/$FILE_NAME" ]; then
  echo "$(date) - Error al descargar $FILE_NAME de $FILE_URL" >&2
  exit 1
fi

# Crear el directorio de destino en HDFS si no existe
$HADOOP_HOME/bin/hdfs dfs -test -d $HDFS_DIR
if [ $? -ne 0 ]; then
  echo "$(date) - Creando directorio $HDFS_DIR en HDFS."
  $HADOOP_HOME/bin/hdfs dfs -mkdir -p $HDFS_DIR
fi

# Enviar el archivo, usa hdfs dfs -put para mover el archivo a HDFS. La opción -f fuerza la sobreescritura si el archivo ya existe en el destino.
$HADOOP_HOME/bin/hdfs dfs -put -f $LOCAL_DIR/$FILE_NAME $HDFS_DIR
if [ $? -eq 0 ]; then
  echo "$(date) - $FILE_NAME se movió correctamente a $HDFS_DIR en HDFS."
else
  echo "$(date) - Error al mover $FILE_NAME a $HDFS_DIR en HDFS." >&2
  exit 1
fi

# Elimina el archivo del directorio temporal después de que se haya movido a HDFS.
# Imprime mensajes en la terminal sobre el estado del proceso y posibles errores.
rm -f $LOCAL_DIR/$FILE_NAME
if [ $? -eq 0 ]; then
  echo "$(date) - $FILE_NAME se eliminó correctamente del directorio temporal $LOCAL_DIR."
else
  echo "$(date) - Error al eliminar $FILE_NAME del directorio temporal $LOCAL_DIR." >&2
  exit 1
fi

echo "$(date) - Proceso completado exitosamente."