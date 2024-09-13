# Creación de un DAG para la ingesta en HDSF y procesamiento en SPARK insertando en el DWH HIVE
# Autor: Marcos Castañeda
# Date: 04/09/2024

#Importamos las librerias a utilizar

from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

# Definimos los parametros basicos del DAG
default_args = {
    'owner' : 'airflow',
    # Indicamos el numero de reintentos
    #'retries': 1,
}

# Inicializamos el DAG
with DAG(
    dag_id='dag_ingest_transform_airport', # Nombre del DAG
    default_args=default_args, # Pasamos los argumentos por Default
    #schedule_interval='@daily', # Este es el intervalo para ejecutar todos los dias
    # Se define la frecuencia de ejecución. (minuto 0 - 59),(hora 0- 23),(dia del mes 1 - 31),(mes 1 - 12),(dia de la semana 0 - 6 (Domingo = 0))
    # Indicamos que la tarea se va ejecutará todos los dias a las 00:00 horas (medianoche)
    schedule_interval='0 0 * * *', 
    # Definimos la fecha
    #'start_date': datetime(2024, 9 , 4 ),
    # Programamos latarea a ejecutar después de definir el intervalo especificado en el (schedule_interval).
    start_date=days_ago(2),
    # Deefinimos el limite de tiempo que se debe de completar el DAG.
    dagrun_timeout=timedelta(minutes=60),
    # Se utiliza para controlar si Airflow debe ejecutar todas las ejecuciones pasadas de un DAG desde su start_date hasta la fecha actual de activación.
    catchup=False,
) as dag:

    #Mandamos un mensaje de inicio
	inicia_proceso = DummyOperator(
		task_id = 'Inicia_proceso',
	)

    # Paso1 Ejecutar el scprit de ingesta (ingest_to_hdfs_c7.sh)
	ingest_task = BashOperator(
        	#Nombre de la tarea
        	task_id = 'ingest_data',
        	#Ruta del script
        	bash_command = '/usr/bin/sh /home/hadoop/scripts/ingest_to_hdfs_c7.sh ',
	)
    # Paso2 Ejecutar el scprit de transformacion (script_spark_sql.py)
	transform_task = BashOperator(
        	#Nombre de la tarea
        	task_id = 'transform_data',
        	#Ruta del script
        	bash_command = 'ssh hadoop@172.17.0.2 /home/hadoop/spark/bin/spark-submit --files /home/hadoop/hive/conf/hive-site.xml /home/hadoop/scripts/script_spark_sql.py ',
	)

	# Mandamos un mensaje de inicio
	termina_proceso = DummyOperator(
		task_id = 'Termina_proceso',
	)

	# Definimos la secuencia
	inicia_proceso >> ingest_task >> transform_task >> termina_proceso

# Se utiliza para proporcionar una interfaz de línea de comandos, cuando se ejecuta un archivo DAG directamente desde el intérprete de Python.
if __name__ == "__main__":
    dag.cli()