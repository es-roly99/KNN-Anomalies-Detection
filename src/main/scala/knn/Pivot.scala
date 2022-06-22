package knn

import knn.AuxiliaryClass.Tupla
import knn.Distance.euclidean
import org.apache.spark.sql.SparkSession

import scala.util.Random

object Pivot {

    def aleatoryPivot(point: Array[Double], list: Array[Array[Double]], spark: SparkSession): Double ={
        euclidean(point, list(Random.nextInt(list.length)), spark)
    }


    def averageDistancePivot(list: Array[Tupla], spark: SparkSession): Double ={
        val elements = Random.shuffle(list.toList).take((list.length * 0.05).toInt).toArray
        elements.map { x =>
            elements.map { y => euclidean(x.valores, y.valores, spark) }.sum / (elements.length - 1)
        }.sum / elements.length
    }


}
