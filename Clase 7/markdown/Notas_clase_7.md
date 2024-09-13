# Notas de la clase 7

## Orquestando el pipeline

* En la realidad no se utiliza el orquestador para procesos de streaming process, por que cuando disparas el proceso ya no para hasta que se genera un errorm ahí es cuando lo paras.
* DAG significa (Directed Acyclic Graph) son grafos unidos por distintas aristas y no van a tener Loops, solo van hacia un unico sentido y no se van a volver a repetir.

![alt text](image-1.png)


* Los operadores es la herramienta que me va permitir conectarme o realizar una tarea especifica para un fin.
* Ejemplos
    * Operadores de BashOperator
    * Operadores de PythonOperator
    * Operadores de EmailOperator

![alt text](image.png)


* Las Tasks son las operaciones que va realizar el DAG