package knn

import knn.AuxiliaryClass.Tupla
import knn.Distance.euclidean
import org.apache.spark.sql.SparkSession

import scala.util.Random

object Pivot {

    def aleatoryPivot(point: Tupla, list: Array[Tupla], spark: SparkSession): Double ={
        euclidean(point.valores, list(Random.nextInt(list.length)).valores, spark)
    }



}
