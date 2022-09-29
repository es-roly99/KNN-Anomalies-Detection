package knn

import knn.AuxiliaryClass._
import knn.KNN._
import org.apache.spark.sql.functions.{col, flatten}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.storage.StorageLevel

object Algorithm {


    def train(dataNew: Dataset[Row], dataTrained: Dataset[Row], spark: SparkSession, k: Int, pivotOption:Int, ID: String = "ID"): Dataset[Result] = {


        import spark.implicits._

        println("**********************************************")
        println("              PARSING TUPLES")
        println("**********************************************")

        val dsNew = dataNew.map { row => parseTuple(row, spark, ID) }
        var dsTrained: Dataset[Tuple] = dsNew
        if (dataTrained != null) {
              dsTrained = dataTrained.map { row => parseTuple(row, spark, "id") }
        }


        println("**********************************************")
        println("            NEIGHBORHOODS SEARCH")
        println("**********************************************")

        val pivotsBroadcast = spark.sparkContext.broadcast(dsTrained.sample(0.001, seed = Configuration.seed).collect())
        var neighborhoods: Dataset[Neighborhood] = pivotOption match {
            case 1 => NeighborhoodsSearch.aleatoryNeighborhoods(pivotsBroadcast, dsTrained, spark)
        }

        if (dataTrained != null) {
            val neighborhoodsNew =  dsNew.mapPartitions { newNeighbors =>
                NeighborhoodsSearch.findNeighborhood(newNeighbors.toSeq, pivotsBroadcast, spark)
            }
            neighborhoods = NeighborhoodsSearch.mergeNeighborhoods(neighborhoodsNew ,neighborhoods.collect(), spark)
        }


        println("**********************************************")
        println("                   STAGE 1")
        println("**********************************************")
        var dsStage1: Dataset[TupleStage1] = null

        if  (dataTrained != null) {
            dsStage1 = neighborhoods.mapPartitions { neighborhood => stage1m(neighborhood, k, spark)}
              .union( neighborhoods.mapPartitions { neighborhood => stage1mm(neighborhood, k, spark)})
        }
        else {
            dsStage1 = neighborhoods.mapPartitions { neighborhood => stage1(neighborhood, k, spark)}
        }


        println("**********************************************")
        println("               CLASSIFICATION")
        println("**********************************************")

        val dsClassified = classify(dsStage1, spark)
        dsClassified

    }

}
