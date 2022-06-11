package knn

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.broadcast._
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField}
import org.apache.spark.storage.StorageLevel
import knn.AuxiliaryClass._
import knn.Distance._

object KNN {


    //Creando objeto logger necesario para almacenamiento de trazas de la aplicacion
    //private final val mylogger: Logger = LogManager.getLogger("KnnwoOD")

    /**
     * exce es la función que se encarga de ejecutar el algoritmo KNNW_BigData.
     *
     * @param data  es un Dataset de Row que contiene la base de datos a la que se le aplica el algoritmo KNNW_BigData
     * @param spark es el SparkSession de la aplicación
     * @param K     es un valor entero que representa la cantidad de vecinos cercanos para una instancia
     * @param P     es un valor Double que representa el por ciento de instancias que de mayor índice de anomalías que se seleccionan para la segunda fase del algoritmo.
     * @return Retorna un Dataset que contiene el identificador único de las tuplas y su índice de anomalía correspondiente
     */


    def clasificar(data: Dataset[Resultado], spark: SparkSession): Dataset[Clasificacion] = {

        import spark.implicits._
        val Stats = data.select("ia").describe().drop("summary").collect().slice(1, 3)
        val classData = data.map { x =>
            var tipo = ""
            val value = x.ia
            val mean = Stats(0).getString(0).toDouble
            val StDev = Stats(1).getString(0).toDouble
            if (value > (mean + 3 * StDev))
                tipo = "anomalia"
            else
                tipo = "normal"
            Clasificacion(x.ID, x.ia, x.distance, x.data, tipo)
        }
        classData
    }


    /** fase1 es una función que determina el indice de anomalía de una instancia en su partición. Inicialmente se obtiene las vecindades de las
     *  instancias en la partición en que se encuentran.
     * Luego a paritr de esta vecindad local se determina el índice de anomalia de la instancia.
     *
     * @param lista es un arreglo de tipo Tupla que representa una partición de los datos
     * @param spark es el SparkSession de la aplicación
     * @return Retorna un iterador de tipo TuplaFase1 que representa la partición de los datos que recibió la función con el índice de anomalía agregado a cada instancia.
     */
    def fase1(lista: Array[Tupla], k:Int, spark: SparkSession): Iterator[TuplaFase1] = {

        val iter = lista.map { x =>
            var l = Array[Double]()

            l = lista.aggregate(l)(
                (v1, v2) => insert(euclidean(x.valores, v2.valores, spark), v1, k, spark),
                (p, set) => insertAll(p, set, k, spark)
            )

            TuplaFase1(x.id, x.valores, IA(l, spark), l)
        }
        iter.iterator

    }

    def fase1(lista: Array[Tupla], listaTrained: Dataset[TuplaTrain], k:Int, spark: SparkSession): Iterator[TuplaFase1] = {

    }


    /**
     * fase2 es la función que se encarga de ajustar el índice de anomalía de las instancias seleccionadas para la segunda fase del algoritmo KNNW_BigData. Inicialmente esta función determina
     * las vecindades, en todas las particiones de la base de datos, de las instancias seleccionadas. Posteriormente se agrupan las vecindades por el identificador de las instancias.
     * Luego se reducen estas vecindades en una sola con los k vecinos más cercanos de toda la base de datos. Por último, se define a partir de la nueva vecindad de la instancia el nuevo índice de anomalía.
     *
     * @param lista es un Broadcast que contiene un arreglo de TuplaFase1. El arreglo representa las P instancias de mayor índice de anomalías seleccionadas para ajustar sus
     *              respectivos índices. El tipo de dato Broadcast permite al programador  mantener una variable de solo lectura almacenada en caché en cada máquina en lugar de enviar una
     *              copia de ella con tareas. Spark distribuye variables Broadcast utilizando algoritmos de difusión eficientes para reducir el costo de comunicación
     * @param spark es el SparkSession de la aplicación
     * @param rdd   es un Dataset de tipo TuplaFase1 que representa la base de datos asignada al KNNW_BigData
     * @return Retorna un Dataset de tipo TuplaFase1. Este Dataset es el conjunto de datosseleccionados para la segunda fase con sus índices de anomalías ajustados.
     */
    def fase2(lista: Broadcast[Array[(TuplaFase1)]], rdd: Dataset[TuplaFase1], k: Int, spark: SparkSession): Dataset[TuplaFase1] = {

        import spark.implicits._
        val result = rdd.mapPartitions { iterator =>
            val arr = iterator.toArray
            val r = lista.value.map { x =>
                val l = Array[Double]()

                val iter = arr.aggregate(l)(
                    (v1, v2) => insert(euclidean(x.valores.toArray, v2.valores.toArray, spark), v1, k, spark),
                    (p, set) => insertAll(p, set, k, spark)
                )

                TuplaFase2(x.id, x.valores, iter)
            }
            r.iterator
        }
        val reduce = result.groupByKey(_.id).reduceGroups((a, b) => TuplaFase2(a.id, a.valores, insertAll(a.distancias.toArray, b.distancias.toArray, k, spark).toSeq))

        val maper = reduce.map { f => TuplaFase1(f._1, f._2.valores, IA(f._2.distancias.toArray, spark), f._2.distancias) }
        maper
    }


    /** update es una función que actualiza los índices de anomalías de las instancias que fueron seleccionadas para la segunda fase en la partición en que se encuentran.
     *
     * @param lista es un Broadcast que contiene un arreglo de TuplaFase1. El arreglo representa las P instancias de mayor índice de anomalías seleccionadas con sus
     *              respectivos índices ajustados. El tipo de dato Broadcast permite al programador  mantener una variable de solo lectura almacenada en caché en cada máquina en lugar de enviar una
     *              copia de ella con tareas. Spark distribuye variables Broadcast utilizando algoritmos de difusión eficientes para reducir el costo de comunicación
     * @param spark es el SparkSession de la aplicación
     * @param rdd   es un arreglo de tipo TuplaFase1 que representa una partición de la base de datos asignada al KNNW_BigData
     * @return Retorna un iterador de tipo Resultado que representa la partición con las instancias actualizadas.
     */
    def update(lista: Broadcast[Array[(TuplaFase1)]], rdd: Array[TuplaFase1], spark: SparkSession): Iterator[Resultado] = {

        var pos = 0
        val result = rdd.map { iterator =>
            var iter = iterator
            val lis = lista.value
            var encontrado = false
            var i = 0
            while (i < lis.length && !encontrado) {

                if (lis.apply(i).id == iterator.id) {
                    iter = lis.apply(i)
                    encontrado = true
                }

                i = i + 1
            }
            pos = pos + 1
            val res = Resultado(iter.id, iter.ia, iter.valores, iter.distance)
            res
        }
        result.iterator
    }


    /** insert es una función que inserta de manera ordenada en un arreglo un valor de tipo double. Esta función se emplea para determinar las k distancias más cercanas de una instancia.
     *
     * @param x     es un Double que representa una distancia
     * @param list  es un arreglo de Double que representa las distancias más cercanas de una instancia
     * @param spark es el SparkSession de la aplicación
     * @return Retorna un arreglo de Double que representa las k distancias más cercanas de una instancia.
     */
    def insert(x: Double, list: Array[Double], k: Int, spark: SparkSession): Array[Double] = {

        var tempL: Array[Double] = list
        if (!x.isNaN) {
            if (tempL.isEmpty) {
                tempL = tempL.+:(x)
                tempL
            } else if (x < tempL.last) {
                if (k > tempL.length) {
                    insert(x, tempL.init, k, spark) ++ (tempL.takeRight(1))
                }
                else {
                    tempL = insert(x, tempL.init, k, spark)
                    tempL
                }
            }
            else if (k > tempL.length) {
                tempL = tempL.+:(x)
                tempL
            }
            else
                tempL
        }
        else
            tempL

    }


    /** insertAll es una función que combina dos vecindades de una instancia. El resultado es una k vecindad de las distancias más cercanas a una instancia.
     *
     * @param a     es un arreglo de tipo Double que representa una vecindad de una instancia. Esta compuesto por las distancias de las instancias cercanas.
     * @param b     es un arreglo de tipo Double que representa una vecindad de una instancia. Esta compuesto por las distancias de las instancias cercanas.
     * @param spark es el SparkSession de la aplicación
     * @return Retorna un arreglo de Double que representa las k distancias más cercanas de una instancia.
     */
    def insertAll(a: Array[Double], b: Array[Double], k: Int, spark: SparkSession): Array[Double] = {

        var l = Array[Double]()
        if (a.length > 0 && b.length > 0) {
            l = a
            for (i <- b.indices)
                l = insert(b.apply(i), l, k, spark)
            l
        }
        else if (a.length > 0) {
            l = a
            l
        }
        else {
            l = b
            l
        }
    }


    /** IA es una función que determina el índice de anomalía a partir de una vecindad de una instancia. El índice de anomalia no es mas que la suma de todas las distancias de los k vecinos cercanos.
     *
     * @param d     es un arreglo de tipo Double que representa las distancias de los k vecinos de una instancia a esta
     * @param spark es el SparkSession de la aplicación
     * @return Retorna un Double que representa el índice de anomalía.
     */
    def IA(d: Array[Double], spark: SparkSession): Double = {
        Math.round(d.sum * 100).toDouble / 100
    }

    /** parseTupla es una función que convirte el tipo de dato Row al tipo de dato Tupla
     *
     * @param row   es una fila de la base de datos del tipo Row
     * @param spark es el SparkSession de la aplicación
     * @return Retorna una objeto de tipo Tupla
     */
    def parseTupla(row: Row, spark: SparkSession, ID: String = "ID"): Tupla = {
        var valores = Array[Double]()
        val id = row.getString(row.fieldIndex(ID))
        valores = row.toSeq.filter(_.toString != id).map(_.toString.toDouble).toArray
        Tupla(id, valores)
    }


    /** parseTuplaNew es una función que convirte el tipo de dato Row al tipo de dato TuplaNew
     *
     * @param row   es una fila de la base de datos del tipo Row
     * @param spark es el SparkSession de la aplicación
     * @return Retorna una objeto de tipo Tupla
     */
    def parseTuplaTrain(row: Row, spark: SparkSession, ID: String = "ID"): TuplaTrain = {
        val id = row.getString(row.fieldIndex(ID))
        val distances = row.getString(row.fieldIndex("distances")).toArray.map{ x => x.toDouble }
        val values = row.getString(row.fieldIndex("data")).toArray.map{ x => x.toDouble }
        TuplaTrain(id, distances, values)
    }

}
