package knn

import knn.AuxiliaryClass._
import knn.Distance._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{Dataset, SparkSession}


object NeighborhoodsSearch {


    def aleatoryNeighborhoods(pivots: Broadcast[Array[Tupla]] ,list: Dataset[Tupla], spark: SparkSession): Dataset[Neighborhood] = {
        import spark.implicits._
        list.mapPartitions { x =>
            findNeighborhood(x.toSeq, pivots, spark)
        }.groupByKey(_.pivot).mapGroups { (key, value) =>
            Neighborhood(key, value.flatMap(_.neighbors).toSeq, Seq[Tupla]())
        }
    }


    def mergeNeighborhoods(closetNeighbors: Dataset[ClosetNeighbors], neighborhoods: Broadcast[Array[Neighborhood]], spark: SparkSession): Dataset[Neighborhood] = {
        import spark.implicits._
        closetNeighbors.groupByKey(_.pivot).mapGroups { (key, value) =>
            val neighbors = neighborhoods.value.find(_.pivot.id == key.id).toSeq.head.neighbors
            val neighborsNew = value.flatMap(_.neighbors).toSeq
            Neighborhood(key, neighbors, neighborsNew)
        }
    }


    def findClosetNeighborhood(newNeighbors: Seq[Tupla], neighborhoods: Broadcast[Array[Neighborhood]], spark: SparkSession): Iterator[ClosetNeighbors] = {
        newNeighbors.map { newNeighbor =>
            neighborhoods.value.map { neighborhood =>
                (neighborhood.pivot, newNeighbor, euclidean(neighborhood.pivot.values, newNeighbor.values, spark))
            }.reduce { (x, y) => if (x._3 < y._3) x else y }
        }.groupBy(_._1)
          .map { x => ClosetNeighbors(x._1, x._2.map(_._2)) }
          .iterator
    }


    def findNeighborhood(neighbors: Seq[Tupla], pivots: Broadcast[Array[Tupla]], spark: SparkSession): Iterator[ClosetNeighbors] = {
        neighbors.map { neighbor =>
            pivots.value.map { pivot =>
                (pivot, neighbor, euclidean(pivot.values, neighbor.values, spark))
            }.reduce { (x,y) => if(x._3 < y._3) x else y}
        }.groupBy(_._1)
          .map { x => ClosetNeighbors(x._1, x._2.map(_._2))}
          .iterator
    }





    }
