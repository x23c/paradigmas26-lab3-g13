## EJERCICIO 1:

a^b. 

## Flujo de procesamiento

```text
Archivo subscriptions.json
       |
       | Tipo: String (ruta del archivo)
       v
Leer archivo de suscripciones (en el Driver)
       |
       | Tipo: List[Option[Subscription]]
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
       | Tipo: RDD[((String, String), Int)]
       |       Representa: ((Tipo, Nombre), 1)
       v
Contar por clave (reduceByKey)
       |
       | Tipo: RDD[((String, String), Int)]
       |       Representa: ((Tipo, Nombre), Total)
       v
Traer datos ordenados al Driver y mostrar
       |
       | Tipo: Array[((String, String), Int)]
       v
Consola (impresión final)
```

Los pasos de paralelizar y ordenar (sortBy) no pueden describirse mediante map, flatMap o reduceByKey porque cumplen funciones de distribución, coordinación, recolección u ordenamiento global de los datos.

c. Barreras de sincronización: los conteos realizados mediante reduceByKey y el ordenamiento global con sortBy. En ambos casos es necesario combinar o reorganizar información proveniente de múltiples workers, por lo que Spark debe esperar a que finalicen las etapas anteriores antes de poder producir el resultado final.

d. Las funciones que se pasan a Spark deben ser serializables para que puedan enviarse a los workers. Además, no deben depender de estado compartido, ya que cada worker ejecuta su propia copia de la función. También es recomendable evitar efectos secundarios, porque las tareas pueden ejecutarse en paralelo o reintentarse ante fallos.


## EJERCICIO 2:
Si la excepción se propaga sin ser capturada dentro de la función del flatMap, Spark interpretaria que la tarea en ejecución fallo. El error interrumpiria la ejecución de todo el pipeline y cancelaria el procesamiento de los demás feeds que si eran válidos.

## EJERCICIO 3:
a. reduceByKey es una barrera de sincronización. ¿Qué ocurre en el cluster en ese punto? ¿Por qué es inevitable para este problema?
Cuando ejecutamos reduceByKey Spark debe reunir todas las ocurrencias de una misma clave (tipo,entidad) para poder sumarlas. Para lograrlo realiza unshuffle para redistribuir los datos entre los workers demodo que todas las tuplas con la misma clabe terminen en el mismo worker. Es una barrera de sincronizacion porque antes de continuar se debe esperar a que todos los workers envien los datos correspondientes. Es un problema inevitable porque las apariciones de una misma entidad pueden encontrarse en distintos workers. Para obtener el conteo de todo se necesita reunir y combinar los resultados.

b. ¿Qué restricciones debe cumplir la función que se le pasa a reduceByKey? Piensen en conmutatividad y asociatividad.

Como Spark combina los valores en cualquier ordeny en distintos nodos del cluster la funcion debe ser asocitiva y conmutativa.

En la funcion reduceByKey uso explicitamente _ + _ que cumple ambas propiedades para los numeros enteros.

c.¿Dónde se hace la lectura del diccionario de entidades? ¿En el driver o los workers?

Inicialmente en el driver, luego a la hora de realizar los analisis/transformaciones Spark envia una copia del diccionario a los workers para que puedan usarlo durante el procesamiento distribuido.

EJERCICIO 5:
a. Sin usar .cache() en filteredPosts, la descarga de feeds se repetiría 5 veces porque hay 5 acciones terminales que dependen directa o indirectamente de filteredPosts.

b. Llamar a collect() entre los pasos "a" y "b" del ejercicio 3 es incorrecto porque convierte un RDD distribuido en una colección local en el driver, destruyendo la naturaleza paralela del pipeline. En resumen, collect() debe ser la última operación del pipeline, solo cuando se necesitan resultados finales pequeños. Interrumpir un pipeline distribuido con collect() anula la paralelización y la escalabilidad.

c. El RDD se almacena en memoria recién en la PRIMERA acción terminal que lo usa.