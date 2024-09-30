# Se determina el intérprete que debe usar para ejecutar el script
#!/bin/bash 
# Autor: Marcos Castañeda
# Date: 22/09/2024

# Configuración de variables
# Ruta definida en la variable $HADOOP_HOME.
HADOOP_HOME=/home/hadoop/hadoop
# El directorio temporal donde se descargarán los archivos.
LOCAL_DIR=/home/hadoop/landing
# El directorio en HDFS donde se moverán los archivos.
HDFS_DIR=/ingest
# La URL desde donde se descagarán los archivos.
FILE_URL_1="https://dataengineerpublic.blob.core.windows.net/data-engineer/CarRentalData.csv"
FILE_URL_2="https://dataengineerpublic.blob.core.windows.net/data-engineer/georef-united-states-of-america-state.csv"
FILE_NAME_1="CarRentalData.csv"
FILE_NAME_2="georef-united-states-of-america-state.csv"


# Verifica si el directorio temporal existe y lo crea si no es así
if [ ! -d "$LOCAL_DIR" ]; then
  mkdir -p $LOCAL_DIR
fi

# Descargar el archivo, usa wget para descargar el archivo desde la URL proporcionada y guarda el archivo con el nombre definido.
cd $LOCAL_DIR
wget -O $FILE_NAME_1 $FILE_URL_1
wget -O $FILE_NAME_2 $FILE_URL_2

# Verificar si la descarga fue exitosa
for FILE_NAME in "$FILE_NAME_1" "$FILE_NAME_2" ; do
  if [ ! -f "$LOCAL_DIR/$FILE_NAME" ]; then
    echo "$(date) - Error al descargar $FILE_NAME" >&2
    exit 1
  fi
done

# ====== Paso 1: Subir los archivos a HDFS ====== 

# Crear el directorio de destino en HDFS si no existe
$HADOOP_HOME/bin/hdfs dfs -test -d $HDFS_DIR
if [ $? -ne 0 ]; then
  echo "$(date) - Creando directorio $HDFS_DIR en HDFS."
  $HADOOP_HOME/bin/hdfs dfs -mkdir -p $HDFS_DIR
fi

# Enviar los archivos, usa hdfs dfs -put para mover el archivo a HDFS. La opción -f fuerza la sobreescritura si el archivo ya existe en el destino.
for FILE_NAME in "$FILE_NAME_1" "$FILE_NAME_2" ; do
  $HADOOP_HOME/bin/hdfs dfs -put -f $LOCAL_DIR/$FILE_NAME $HDFS_DIR
  if [ $? -eq 0 ]; then
    echo "$(date) - $FILE_NAME se movió correctamente a $HDFS_DIR en HDFS."
  else
    echo "$(date) - Error al mover $FILE_NAME a $HDFS_DIR en HDFS." >&2
    exit 1
  fi
done 

# ====== Paso 2: Eliminamos los archivos locales ====== 

# Elimina el archivo del directorio temporal después de que se haya movido a HDFS.
# Imprime mensajes en la terminal sobre el estado del proceso y posibles errores.
for FILE_NAME in "$FILE_NAME_1" "$FILE_NAME_2" ; do
  rm -f $LOCAL_DIR/$FILE_NAME
  if [ $? -eq 0 ]; then
    echo "$(date) - $FILE_NAME se eliminó correctamente del directorio temporal $LOCAL_DIR."
  else
    echo "$(date) - Error al eliminar $FILE_NAME del directorio temporal $LOCAL_DIR." >&2
    exit 1
  fi
done

echo "$(date) - Proceso completado exitosamente."