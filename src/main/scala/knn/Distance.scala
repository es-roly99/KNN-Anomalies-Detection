package knn

import org.apache.spark.sql.SparkSession

object Distance {

    class Distance{def calculate(x: Array[Double], y: Array[Double], sparkSession: SparkSession): Double = {0}}

    case class Euclidean() extends Distance{
        /** euclidean es una función que determina la distancia euclidiana entre dos filas
         *
         * @param row1  es un arreglo de tipo Double que representa una fila de la base de datos
         * @param row2  es un arreglo de tipo Double que representa una fila de la base de datos
         * @param spark es el SparkSession de la aplicación
         * @return Retorna un Double que representa la distancia euclidiana entre las filas row1 y row2.
         */
        override def calculate(row1: Array[Double], row2: Array[Double], spark: SparkSession): Double ={
            var i = 0
            var sum: Double = 0
            val tam = row2.length
            while (i < tam) {
                sum += math.pow(row1.apply(i) - row2.apply(i), 2)
                i += 1
            }
            Math.round(math.sqrt(sum) * 1000000000).toDouble / 1000000000
        }
    }

    case class Manhattan() extends Distance {
        /** manhattan es una función que determina la distancia manhattan entre dos filas
         *
         * @param row1  es un arreglo de tipo Double que representa una fila de la base de datos
         * @param row2  es un arreglo de tipo Double que representa una fila de la base de datos
         * @param spark es el SparkSession de la aplicación
         * @return Retorna un Double que representa la distancia manhattan entre las filas row1 y row2.
         */
        override def calculate(row1: Array[Double], row2: Array[Double], spark: SparkSession): Double = {
            var i = 0
            var sum: Double = 0
            val tam = row2.length
            while (i < tam) {
                sum += math.abs(row1.apply(i) - row2.apply(i))
                i += 1
            }
            sum
        }
    }

    case class MaxDistance() extends Distance {
        /** max_distance es una función que determina la distancia máxima entre dos filas
         *
         * @param row1  es un arreglo de tipo Double que representa una fila de la base de datos
         * @param row2  es un arreglo de tipo Double que representa una fila de la base de datos
         * @param spark es el SparkSession de la aplicación
         * @return Retorna un Double que representa la distancia máxima entre las filas row1 y row2.
         */
        override def calculate(row1: Array[Double], row2: Array[Double], spark: SparkSession): Double = {
            var i = 0
            var sum: Double = 0
            var max: Double = 0
            val tam = row2.length
            while (i < tam) {
                sum = math.abs(row1.apply(i) - row2.apply(i))
                if (max < sum)
                    max = sum
                i += 1
            }
            max
        }
    }

}
