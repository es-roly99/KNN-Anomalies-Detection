package knn

import knn.AuxiliaryClass._
import knn.Distance.euclidean
import org.apache.spark.sql.{Dataset, SparkSession}
import scala.util.control.Breaks._


object PivotSearch {


    def findNeighborhood(pivots: Array[Pivot], id: String): Array[Tupla] = {
        var list: Array[Tupla] = null
        pivots.foreach { x => x.nearestNeighbor.foreach { y =>
            if (y.id == id) list = x.nearestNeighbor.toArray
        }}
        list
    }

    def aleatoryPivot(list: Dataset[Tupla], spark: SparkSession): Array[Pivot] = {
        val pivots = list.sample(0.001).collect()

        val set = list.collect().map { x =>
            pivots.map { y => (y.id, x, euclidean(x.valores, y.valores, spark)) }
              .reduce { (a, b) => if (a._3 < b._3) (a._1, x, a._3) else (b._1, x, a._3) }
        }.groupBy(_._1)

        set.map { x => Pivot( x._1, x._2.map(_._2)) }.toArray
    }


    def piaesaPivot(spark: SparkSession): Unit ={
       // some code
    }


}
