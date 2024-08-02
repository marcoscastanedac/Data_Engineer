# Autor:Marcos Jovani Castañeda Castañon
# BDE
# 01/08/2024

# Se determina el intérprete que debe usar para ejecutar el script
#!/bin/bash 

# Configuración de variables
# El directorio temporal donde se descargará el archivo.
LOCAL_DIR=/home/nifi/ingest
# La URL desde donde se descagará el archivo.
FILE_URL=https://dataengineerpublic.blob.core.windows.net/data-engineer/starwars.csv
# El nombre del archivo que se descargará.
FILE_NAME=starwars.csv

# Verifica si el directorio temporal existe y lo crea si no es así
if [ ! -d "$LOCAL_DIR" ]; then
  mkdir -p $LOCAL_DIR
fi

# Descargar el archivo, usa wget para descargar el archivo desde la URL proporcionada y guarda el archivo con el nombre starwars.csv.
cd $LOCAL_DIR
wget -O $FILE_NAME $FILE_URL

# Verificar si la descarga fue exitosa
if [ ! -f "$LOCAL_DIR/$FILE_NAME" ]; then
  echo "$(date) - Error al descargar $FILE_NAME de $FILE_URL" >&2
  exit 1
fi

echo "$(date) - Proceso completado exitosamente."