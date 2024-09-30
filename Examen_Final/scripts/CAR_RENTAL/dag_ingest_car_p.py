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
    # Definimos la fecha
    'start_date': datetime(2023, 1 , 1 ),
    # Indicamos el numero de reintentos
    #'retries': 1,
}

# Inicializamos el DAG Padre
with DAG(
    dag_id='dag_ingest_car_p', # Nombre del DAG
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

    #Mandamos un mensaje de inicio
	inicia_proceso = DummyOperator(
		task_id = 'Inicia_proceso',
	)

    # Paso1 Ejecutar el scprit de ingesta (ingest_to_hdfs_c7.sh)
	ingest_task = BashOperator(
        	#Nombre de la tarea
        	task_id = 'ingest_data',
        	#Ruta del script
        	bash_command = '/usr/bin/sh /home/hadoop/scripts/ingest_to_hdfs_car.sh ',
	)
 
    # Llamamos la DAG hijo para que realice el proceso de transformación (transform_spark_sql_car.py)
	trigger_target = TriggerDagRunOperator(
        	#Nombre de la tarea
        	task_id = 'trigger_target',
            #Nombre del Dag hijo
            trigger_dag_id = 'dag_transform_car_h',
            execution_date = '{{ ds }}',
            #Limpia la ejecución del Dag existente
            reset_dag_run = True,
            #Indicamos que espere hasta que finalice
            wait_for_completion = True,
            # Consulta si finalizo cada 30 segundos
            poke_interval = 30            
	)

	# Definimos la secuencia
	inicia_proceso >> ingest_task >> trigger_target 

# Se utiliza para proporcionar una interfaz de línea de comandos, cuando se ejecuta un archivo DAG directamente desde el intérprete de Python.
if __name__ == "__main__":
    dag.cli()