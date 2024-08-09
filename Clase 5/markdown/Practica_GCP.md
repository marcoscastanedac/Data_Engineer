# Practica ingest GCP

### 1. Crear un Bucket Regional standard en Finlandia llamado *demo-bucket-edvai-bde*

* Creamos un bucket desde la interfaz de GCP 

![alt text](../images/image.png)

* Mostramos que ya se haya creado

![alt text](../images/image2.png)

### 2. Crear un bucket multiregional standard en US llamado *data-bucket-demo-bde*

* Creamos un bucket desde la interfaz de GCP

![alt text](../images/image-3.png)

### 3. Hacer ingest con la herramienta CLI Gsutil de 5 archivos csv en el bucket *data-bucket-demo-bde*

* Vamos a ingestar los 5 archivos con extensión .csv

![alt text](../images/image-6.png)

* Validamos la dirección del bucket destino 

![alt text](../images/image-5.png)

~~~
Realizamos el ingest con el comando gsutil cp **nombre_archivo** destino_bucket  
~~~

![alt text](../images/image-7.png)

![alt text](../images/image-8.png)

* Validamos que se haya realizado el ingest correctamente.

![alt text](../images/image-9.png)


### 4. Utilizar el servicio de storage transfer para crear un job que copie los archivos que se encuentran en *data-bucket-demo-bde* a *demo-bucket-edvai-edvai*

* Creamos un transferjob 

![alt text](../images/image-10.png)

* Seleccionamos desde donde vamos a tomar los archivos.

![alt text](../images/image-11.png)

* Seleccionamos la carpeta de destino.

![alt text](../images/image-13.png)

* ¿Cuando queremos que se corra el job?

![alt text](../images/image-14.png)

* Validamos la creación del job.

![alt text](../images/image-15.png)

* Validamos que se haya corrido sin ningun problema.

![alt text](../images/image-16.png)

* Checamos la carpeta destino para visualizar los archivos.

![alt text](../images/image-17.png)

![alt text](../images/image-18.png)