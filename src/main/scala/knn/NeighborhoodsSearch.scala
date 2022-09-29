package knn

import knn.AuxiliaryClass._
import knn.Distance._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{Dataset, SparkSession}


object NeighborhoodsSearch {


    def aleatoryNeighborhoods(pivots: Broadcast[Array[Tuple]], list: Dataset[Tuple], spark: SparkSession): Dataset[Neighborhood] = {
        import spark.implicits._
        list.mapPartitions { x =>
            findNeighborhood(x.toSeq, pivots, spark)
        }.groupByKey(_.pivot).mapGroups { (key, value) =>
            Neighborhood(key, value.flatMap(_.neighbors).toSeq, Seq[Tuple]())
        }
    }


    def mergeNeighborhoods(closetNeighbors: Dataset[Neighborhood], neighborhoods: Array[Neighborhood], spark: SparkSession): Dataset[Neighborhood] = {
        import spark.implicits._
        closetNeighbors.groupByKey(_.pivot).mapGroups { (key, value) =>
            val neighbors = neighborhoods.find(_.pivot.id == key.id).toSeq.head.neighbors
            val neighborsNew = value.flatMap(_.neighbors).toSeq
            Neighborhood(key, neighbors, neighborsNew)
        }
    }


    def findNeighborhood(neighbors: Seq[Tuple], pivots: Broadcast[Array[Tuple]], spark: SparkSession): Iterator[Neighborhood] = {
        neighbors.map { neighbor =>
            pivots.value.map { pivot =>
                (pivot, neighbor, euclidean(pivot.values, neighbor.values, spark))
            }.reduce { (x,y) => if(x._3 < y._3) x else y}
        }.groupBy(_._1)
          .map { x => Neighborhood(x._1, x._2.map(_._2), Seq[Tuple]())}
          .iterator
    }





    }
