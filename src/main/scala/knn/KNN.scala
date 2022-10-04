package knn

import org.apache.spark.broadcast._
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import knn.AuxiliaryClass._
import knn.Distance._

object KNN {

    /** parseTuple es una función que convirte el tipo de dato Row al tipo de dato Tupla
     *
     * @param row es una fila de la base de datos del tipo Row
     * @param spark SparkSession
     * @return Retorna una objeto de tipo Tuple
     */
    def parseTuple(row: Row, spark: SparkSession, ID: String = "ID"): Tuple = {
        val id = row.getString(row.fieldIndex(ID))
        try {
            val d = row.getString(row.fieldIndex("distances"))
            val distance = d.substring(1, d.length - 1).split(',').map { x => x.toDouble }
            val v = row.getString(row.fieldIndex("values"))
            val values = v.substring(1, v.length - 1).split(',').map { x => x.toDouble }
            Tuple(id, values, distance)
        }
        catch {
            case _: Exception =>
                val values = row.toSeq.filter(_.toString != id).map(_.toString.toDouble).toArray
                Tuple(id, values, null)
        }
    }


    /**
     * classify es la función que se encarga de clasificar las Tuplas de la fase 1 segun sus distancias
     *
     * @param data  es un Dataset de Tupla Stage1 que contiene la base de datos a clasificar
     * @param spark es el SparkSession de la aplicación
     * @return Retorna un Dataset de Result que contiene la base de datos clasificada
     */
    def classify(data: Dataset[TupleStage1], spark: SparkSession): Dataset[Result] = {

        import spark.implicits._
        val Stats = data.select("ia").describe().drop("summary").collect().slice(1, 3)

        data.map { x =>
            var category = ""
            val value = x.ia
            val mean = Stats(0).getString(0).toDouble
            val StDev = Stats(1).getString(0).toDouble
            if (value > (mean + Configuration.p * StDev))
                category = "anomaly"
            else
                category = "normal"
            Result(x.id, x.ia, x.distances, x.values, category)
        }
    }


    /** stage1 es una función que calcula las k distancias mas cercanas de cada Tupla en su vecindad.
     *  Esta funcion es utilizada en la primera iteracion del algoritmo
     *
     * @param neighborhoods Iterador de las vecindades
     * @param k Entero que representa la cantidad de vecinos cercanos
     * @param spark SparkSession
     * @return Iterador de TupleStage1 que representan las tuplas con las distancias mas cercanas a cada vecindad
     */
    def stage1(neighborhoods: Iterator[Neighborhood], k: Int, spark: SparkSession): Iterator[TupleStage1] = {
        neighborhoods.flatMap { neighborhood =>
            neighborhood.neighbors.map { x =>
                var distances = Array[Double]()
                neighborhood.neighbors.foreach { y =>
                    if (x.id != y.id) distances = insert(euclidean(x.values, y.values, spark), distances, k, spark)
                }
                TupleStage1(x.id, x.values, IA(distances, spark), distances)
            }
        }
    }


    /** stage1Neighbors es una función que calcula las k distancias mas cercanas de cada Tupla en su vecindad.
     * Esta funcion es utilizada para actualizar las distancias de las Tuplas del flujo anterior
     *
     * @param neighborhoods Iterador de las vecindades
     * @param k             Entero que representa la cantidad de vecinos cercanos
     * @param spark         SparkSession
     * @return Iterador de TupleStage1 que representan las tuplas con las distancias mas cercanas a cada vecindad
     */
    def stage1Neighbors(neighborhoods: Iterator[Neighborhood], k: Int, spark: SparkSession): Iterator[TupleStage1] = {
        neighborhoods.flatMap { neighborhood =>
            neighborhood.neighbors.map { neighbor =>
                var distances =  neighbor.distances
                neighborhood.neighborsNew.foreach { neighborNew =>
                    distances = insert(euclidean(neighbor.values, neighborNew.values, spark), distances, k, spark)
                }
                TupleStage1(neighbor.id, neighbor.values, IA(distances, spark), distances)
            }
        }
    }


    /** stage1NeighborsNew es una función que calcula las k distancias mas cercanas de cada Tupla en su vecindad.
     * Esta funcion es utilizada para calcular las distancias de las Tuplas del nuevo flujo
     *
     * @param neighborhoods Iterador de las vecindades
     * @param k             Entero que representa la cantidad de vecinos cercanos
     * @param spark         SparkSession
     * @return Iterador de TupleStage1 que representan las tuplas con las distancias mas cercanas a cada vecindad
     */
    def stage1NeighborsNew(neighborhoods: Iterator[Neighborhood], k: Int, spark: SparkSession): Iterator[TupleStage1] = {
        neighborhoods.flatMap { neighborhood =>
            neighborhood.neighborsNew.map { neighborNew =>
                var distances = Array[Double]()
                neighborhood.neighbors.foreach {x =>
                    distances = insert(euclidean(x.values, neighborNew.values, spark), distances, k, spark)
                }
                TupleStage1(neighborNew.id, neighborNew.values, IA(distances, spark), distances)
            }
        }
    }


    /** insert es una función que inserta de manera ordenada en un arreglo un valor de tipo double
     * Esta función se emplea para determinar las k distancias más cercanas de una instancia.
     *
     * @param x Double que representa una distancia
     * @param list arreglo de Double que representa las distancias más cercanas de una instancia
     * @param spark SparkSession
     * @return Retorna un arreglo de Double que representa las k distancias más cercanas de una instancia.
     */
    def insert(x: Double, list: Array[Double], k: Int, spark: SparkSession): Array[Double] = {

        val tempL: Array[Double] = list
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
     * @param distances arreglo de Double que representa las distancias de los k vecinos de una instancia a esta
     * @param spark SparkSession
     * @return Retorna un Double que representa el índice de anomalía.
     */
    def IA(distances: Array[Double], spark: SparkSession): Double = { distances.sum }



}
