package knn

import org.apache.spark.sql.SparkSession

object Distance {


    /** distanceds es una función que determina la distancia euclidiana entre dos filas
     *
     * @param row1  es un arreglo de tipo Double que representa una fila de la base de datos
     * @param row2  es un arreglo de tipo Double que representa una fila de la base de datos
     * @param spark es el SparkSession de la aplicación
     * @return Retorna un Double que representa la distancia euclidiana entre las filas row1 y row2.
     */

    def euclidean(row1: Array[Double], row2: Array[Double], spark: SparkSession): Double = {
        var i = 0
        var sum: Double = 0
        val tam = row2.length
        while (i < tam) {
            sum += math.pow(row1.apply(i) - row2.apply(i), 2)
            i += 1
        }
        Math.round(math.sqrt(sum) * 1000).toDouble / 1000
    }


    def manhattan(row1: Array[Double], row2: Array[Double], spark: SparkSession): Double = {
        var i = 0
        var sum: Double = 0
        val tam = row2.length
        while (i < tam) {
            sum += math.abs(row1.apply(i) - row2.apply(i))
            i += 1
        }
        sum
    }


    def max_distance(row1: Array[Double], row2: Array[Double], spark: SparkSession): Double = {
        var i = 0
        var sum: Double = 0
        var max: Double = 0
        val tam = row2.length
        while (i < tam) {
            sum += math.abs(row1.apply(i) - row2.apply(i))
            if (max < sum)
                max = sum
            i += 1
        }
        max
    }


    }
