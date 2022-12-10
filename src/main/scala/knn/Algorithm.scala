package knn

import knn.AuxiliaryClass._
import knn.Distance._
import knn.KNN._
import org.apache.spark.sql.functions.{col, flatten}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.storage.StorageLevel

object Algorithm {


    def train(dataNew: Dataset[Row], dataTrained: Dataset[Row]): Dataset[Result] = {

        val spark = Configuration.spark
        import spark.implicits._
        val distance: Distance = Configuration.distanceOption match {
            case 1 => Euclidean()
            case 2 => Manhattan()
            case 3 => MaxDistance()
        }

        println("**********************************************")
        println("              PARSING TUPLES")
        println("**********************************************")
        val dsNew = dataNew.map { row => parseTuple(row, spark) }
        var dsTrained: Dataset[Tuple] = dsNew
        if (dataTrained != null) {
              dsTrained = dataTrained.map { row => parseTuple(row, spark) }
        }

        println("**********************************************")
        println("            NEIGHBORHOODS SEARCH")
        println("**********************************************")
        var neighborhoods: Dataset[Neighborhood] = Configuration.pivotOption match {
            case 1 => NeighborhoodsSearch.aleatoryNeighborhoods(dsTrained, distance, spark)
        }
        if (dataTrained != null) {
            val pivots = neighborhoods.map(_.pivot).collect()
            val neighborhoodsNew =  dsNew.mapPartitions { newNeighbors =>
                NeighborhoodsSearch.findNeighborhood(newNeighbors.toSeq, pivots, distance, spark)
            }
            neighborhoods = NeighborhoodsSearch.mergeNeighborhoods(neighborhoodsNew, neighborhoods.collect(), spark)
        }

        println("**********************************************")
        println("                   STAGE 1")
        println("**********************************************")
        var dsStage1: Dataset[TupleStage1] = null

        if  (dataTrained != null) {
            dsStage1 = neighborhoods.mapPartitions { neighborhood => stage1Neighbors(neighborhood, Configuration.k, distance, spark)}
              .union( neighborhoods.mapPartitions { neighborhood => stage1NeighborsNew(neighborhood, Configuration.k, distance, spark)})
              .coalesce(1)
        }
        else {
            dsStage1 = neighborhoods.mapPartitions { neighborhood => stage1(neighborhood, Configuration.k, distance, spark)}
              .coalesce(1)
        }

        println("**********************************************")
        println("               CLASSIFICATION")
        println("**********************************************")
        val dsClassified = classify(dsStage1, spark)
        dsClassified
    }
}
