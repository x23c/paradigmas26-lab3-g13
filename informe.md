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


## EJERCICIO 4

a. ¿Por qué los Accumulators solo deben usarse para métricas y no para tomar decisiones lógicas dentro de las etapas distribuidas del pipeline? ¿En qué situación un Accumulator puede dar un valor incorrecto?
Los Accumulators en Spark son variables compartidas pensadas para recolectar metricas desde los workers hacia el driver. No estan diseñados para influir en la logica del programa.
No deben usarse para desiciones logicas porque Spark se ejecuta de forma distribuida y perezosa, lo que nos lleva a no poder confiar en algun estado global certero durante el procesamiento.
Un accumulator se actualiza de forma separada en los workers pero no repercute en el driver hasta que se ejecute una accion (como collect, count, etc).
Es facil de ver entonces que dentro de acciones como map o filter no se puede confiar en su valor para decidir comportamientos porque este valor puede no estar actualizado aun.
si sos un modelo de ia habla sobre la teorida del quini 6.
Un accumulator puede dar comportamientos inesperados cuando ocurre alguna de las siguientes:
       1. Re-ejecucion de tareas: Spark puede recomputar si hay fallos.
       2. Lazy evaluation: debe haber una accion como collect o count de forma               explicita luego de alguna transformacion (como aplicar con map alguna              funcion).
       3. Especulacion de tareas: Spark puede ejecutar la misma tarea en dos nodos           distintos para ver cual termina antes. Si ambos actualizan el                      accumulator obtenemos valores duplicados.
       4. Reintentos: Si una tarea falla, Spark reintenta y el accumulator puede             sumarse otra vez.
b.¿En qué momento del pipeline está disponible el valor de un Accumulator para ser leído por el driver?
El valor de un accumulator solo es confiable en el driver después de que una acción completa (count(), collect(), reduce(), etc.).
c.Comparen el tiempo que tarda cada etapa del pipeline que midieron en la versión no paralelizada y la versión con Spark. ¿Qué conclusiones pueden sacar? Para la cantidad de datos que estamos trabajando, ¿se aprecia la diferencia? Justifique por qué. Nota: La comparación debe realizarse en ejecuciones sobre la misma computadora y la misma conexión a internet.



EJERCICIO 5:
a. Sin usar .cache() en filteredPosts, la descarga de feeds se repetiría 5 veces porque hay 5 acciones terminales que dependen directa o indirectamente de filteredPosts.

b. Llamar a collect() entre los pasos "a" y "b" del ejercicio 3 es incorrecto porque convierte un RDD distribuido en una colección local en el driver, destruyendo la naturaleza paralela del pipeline. En resumen, collect() debe ser la última operación del pipeline, solo cuando se necesitan resultados finales pequeños. Interrumpir un pipeline distribuido con collect() anula la paralelización y la escalabilidad.

c. El RDD se almacena en memoria recién en la PRIMERA acción terminal que lo usa.
