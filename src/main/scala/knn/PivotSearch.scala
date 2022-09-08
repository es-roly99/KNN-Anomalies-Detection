package knn

import knn.AuxiliaryClass._
import knn.Distance._
import org.apache.spark.sql.{Dataset, SparkSession}


object PivotSearch {


    def aleatoryPivot(list: Dataset[Tupla], spark: SparkSession): Dataset[Neighborhood] = {
        val pivots = list.sample(0.0005).collect()
        import spark.implicits._
        list.map { x =>
            pivots.map { y => (y, x, euclidean(x.valores, y.valores, spark)) }
              .reduce { (a, b) => if (a._3 < b._3) (a._1, a._2, a._3) else (b._1, a._2, a._3) }
        }.groupByKey(_._1)
          .mapGroups { (key, value) => Neighborhood( key, value.map(_._2).toSeq) }
    }


    def AESAPivot(list: Dataset[Tupla], spark: SparkSession): Dataset[Neighborhood] ={
       null
    }

    def setCloserNeighborhood(neighborhoods: Dataset[Neighborhood], newNeighbors: Seq[Tupla], spark: SparkSession): Dataset[Neighborhood] = {
//        import spark.implicits._
//        val newNeighborsGrouped = newNeighbors.map { newNeighbor =>
//            neighborhoods.map { neighborhood =>
//                (neighborhood.pivot, newNeighbor, euclidean(neighborhood.pivot.valores, newNeighbor.valores, spark))
//            }.reduce { (x,y) => if(x._3 < y._3) x else y }
//        }.groupBy(_._1)
//
//        neighborhoods.map { neighborhood =>
//          var a = newNeighborsGrouped.find( x => {x._1 == neighborhood.pivot})
//            Neighborhood(neighborhood.pivot, neighborhood.neighbors, )
//        }
        neighborhoods
    }



}
