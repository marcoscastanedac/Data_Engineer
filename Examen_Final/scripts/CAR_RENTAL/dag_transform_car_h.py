# Creación de un DAG para la ingesta en HDSF y procesamiento en SPARK insertando en el DWH HIVE para el exámen final.
# Autor: Marcos Castañeda
# Date: 23/09/2024
# BDE

#Importamos las librerias a utilizar

from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# Definimos los parametros basicos del DAG
default_args = {
    'owner' : 'airflow',
    # Definimos la fecha indicada 01/01/2021
    'start_date': datetime(2024, 1 , 1 ),
    # Indicamos el numero de reintentos
    #'retries': 1,
}

# Inicializamos el DAG Hijo
with DAG(
    dag_id='dag_transform_car_h', # Nombre del DAG
    default_args=default_args, # Pasamos los argumentos por Default
    #schedule_interval='@daily', # Este es el intervalo para ejecutar todos los dias
    # Se define la frecuencia de ejecución. (minuto 0 - 59),(hora 0- 23),(dia del mes 1 - 31),(mes 1 - 12),(dia de la semana 0 - 6 (Domingo = 0))
    # Indicamos que la tarea se va ejecutará todos los dias a las 00:00 horas (medianoche)
    schedule_interval='0 0 * * *', 
    # Programamos la tarea a ejecutar después de definir el intervalo especificado en el (schedule_interval).
    #start_date=days_ago(2),
    # Deefinimos el limite de tiempo que se debe de completar el DAG.
    dagrun_timeout=timedelta(minutes=60),
    # Se utiliza para controlar si Airflow debe ejecutar todas las ejecuciones pasadas de un DAG desde su start_date hasta la fecha actual de activación.
    catchup=False,
) as dag:

    # Llamamos la DAG hijo para que realice el proceso de transformación (transform_spark_sql_car.py)
	transform_task = BashOperator(
        	#Nombre de la tarea
        	task_id = 'transform_data',
        	#Ruta del script
        	bash_command = 'ssh hadoop@172.17.0.2 /home/hadoop/spark/bin/spark-submit --files /home/hadoop/hive/conf/hive-site.xml /home/hadoop/scripts/transform_spark_sql_car.py ',
	)

	# Mandamos un mensaje de inicio
	termina_proceso = DummyOperator(
		task_id = 'Termina_proceso',
	)

	# Definimos la secuencia
	transform_task >> termina_proceso

# Se utiliza para proporcionar una interfaz de línea de comandos, cuando se ejecuta un archivo DAG directamente desde el intérprete de Python.
if __name__ == "__main__":
    dag.cli()