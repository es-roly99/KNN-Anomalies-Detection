package knn

import org.apache.spark.broadcast._
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import knn.AuxiliaryClass._
import knn.Distance._

object KNN {

    /** parseTupla es una función que convirte el tipo de dato Row al tipo de dato Tupla
     *
     * @param row   es una fila de la base de datos del tipo Row
     * @param spark es el SparkSession de la aplicación
     * @return Retorna una objeto de tipo Tupla
     */
    def parseTupla(row: Row, spark: SparkSession, ID: String = "ID"): Tupla = {
        val id = row.getString(row.fieldIndex(ID))
        try {
            val d = row.getString(row.fieldIndex("distance"))
            val distance = d.substring(1, d.length - 1).split(',').map { x => x.toDouble }
            val v = row.getString(row.fieldIndex("data"))
            val values = v.substring(1, v.length - 1).split(',').map { x => x.toDouble }
            Tupla(id, values, distance)
        }
        catch {
            case _: Exception =>
                val values = row.toSeq.filter(_.toString != id).map(_.toString.toDouble).toArray
                Tupla(id, values, null)
        }
    }


    /**
     * exce es la función que se encarga de ejecutar el algoritmo KNNW_BigData.
     *
     * @param data  es un Dataset de Row que contiene la base de datos a la que se le aplica el algoritmo KNNW_BigData
     * @param spark es el SparkSession de la aplicación
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


    /** fase1 es una función que determina el indice de anomalía de una instancia en su partición.
     *        Inicialmente se obtiene las vecindades de las instancias en la partición en que se encuentran.
     *        Luego a partir de esta vecindad local se determina el índice de anomalia de la instancia.
     *
     * @param lista es un arreglo de tipo Tupla que representa una partición de los datos
     * @param spark es el SparkSession de la aplicación
     * @return Retorna un iterador de tipo TuplaFase1 que representa la partición de los datos que recibió la función
     *         con el índice de anomalía agregado a cada instancia.
     */
    // Fase 1 primera iteracion
    def stage1(neighborhoods: Iterator[Neighborhood], k: Int, spark: SparkSession): Iterator[TuplaFase1] = {
        neighborhoods.flatMap { neighborhood =>
            neighborhood.neighbors.map { x =>
                var distances = Array[Double]()
                neighborhood.neighbors.foreach { y =>
                    if (x.id != y.id) distances = insert(euclidean(x.values, y.values, spark), distances, k, spark)
                }
                TuplaFase1(x.id, x.values, IA(distances, spark), distances)
            }
        }
    }

    // Fase 1 varias iteraciones
    def stage1m(neighborhoods: Iterator[Neighborhood], k: Int, spark: SparkSession): Iterator[TuplaFase1] = {
        neighborhoods.flatMap { neighborhood =>
            neighborhood.neighbors.map { neighbor =>
                var distances =  neighbor.distance
                neighborhood.neighborsNew.foreach { neighborNew =>
                    distances = insert(euclidean(neighbor.values, neighborNew.values, spark), distances, k, spark)
                }
                TuplaFase1(neighbor.id, neighbor.values, IA(distances, spark), distances)
            }
        }
    }

    def stage1mm(neighborhoods: Iterator[Neighborhood], k: Int, spark: SparkSession): Iterator[TuplaFase1] = {
        neighborhoods.flatMap { neighborhood =>
            neighborhood.neighborsNew.map { neighborNew =>
                var distances = Array[Double]()
                neighborhood.neighbors.foreach {x =>
                    distances = insert(euclidean(x.values, neighborNew.values, spark), distances, k, spark)
                }
                TuplaFase1(neighborNew.id, neighborNew.values, IA(distances, spark), distances)
            }
        }
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
                tempL.+:(x)
            } else if (x < tempL.last) {
                if (k > tempL.length) {
                    insert(x, tempL.init, k, spark) ++ (tempL.takeRight(1))
                }
                else {
                    insert(x, tempL.init, k, spark)
                }
            }
            else if (k > tempL.length) {
                tempL.+:(x)
            }
            else
                tempL
        }
        else
            tempL
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



}
