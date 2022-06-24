package knn

import knn.AuxiliaryClass._
import knn.Distance.euclidean
import org.apache.spark.sql.SparkSession

import scala.util.Random

object PivotSearch {

    def aleatoryPivot(list: Array[Tupla], spark: SparkSession,  pivotQuantity: Int = 5): Array[Pivot] = {
        val pivots = Random.shuffle(list.toList).take(pivotQuantity).toArray

        val set = list.map { x =>
            pivots.map { y => (y.id, x.id, euclidean(x.valores, y.valores, spark)) }
              .reduce { (a, b) => if (a._3 < b._3) (a._1, a._2, a._3) else (b._1, b._2, b._3) }
        }.groupBy(_._1)

        set.map { x => Pivot( x._1, x._2.map(_._2)) }.toArray
    }


    def piaesaPivot(spark: SparkSession): Unit ={
       // some code
    }


}
