## EJERCICIO 1:

a^b. 

Archivo subscriptions.json
       |
       | Tipo: String (Ruta del archivo)
       v
Leer archivo de suscripciones (en el Driver)
       |
       | Tipo: List(Option[Subscription])
       v
Paralelizar 
       |
       | Tipo: RDD[Subscription]
       v
Descargar feeds (flatMap)
       |
       | Tipo: RDD[Post]
       v
Extraer entidades (flatMap)
       |
       | Tipo: RDD[NamedEntity]
       v
Mapear para conteo (map)
       |
       | Tipo: RDD[((String, String), Int)]  <-- Representa ((Tipo, Nombre), 1)
       v
Contar por clave (reduceByKey)
       |
       | Tipo: RDD[((String, String), Int)]  <-- Representa ((Tipo, Nombre), Total)
       v
Traer datos ordenados al Driver y mostrar 
       |
       | Tipo: Array[((String, String), Int)]
       v
Consola (impresion final)          


Los pasos de paralelizar y ordenar (sortBy) no pueden describirse mediante map, flatMap o reduceByKey porque cumplen funciones de distribución, coordinación, recolección u ordenamiento global de los datos.

c. Barreras de sincronización: los conteos realizados mediante reduceByKey y el ordenamiento global con sortBy. En ambos casos es necesario combinar o reorganizar información proveniente de múltiples workers, por lo que Spark debe esperar a que finalicen las etapas anteriores antes de poder producir el resultado final.

d. Las funciones que se pasan a Spark deben ser serializables para que puedan enviarse a los workers. Además, no deben depender de estado compartido, ya que cada worker ejecuta su propia copia de la función. También es recomendable evitar efectos secundarios, porque las tareas pueden ejecutarse en paralelo o reintentarse ante fallos.


## EJERCICIO 2:
Si la excepción se propaga sin ser capturada dentro de la función del flatMap, Spark interpretaria que la tarea en ejecución fallo. El error interrumpiria la ejecución de todo el pipeline y cancelaria el procesamiento de los demás feeds que si eran válidos.
