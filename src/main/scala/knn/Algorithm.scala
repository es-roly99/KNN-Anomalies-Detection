package knn

import knn.AuxiliaryClass._
import knn.KNN._
import org.apache.spark.sql.functions.{col, flatten}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.storage.StorageLevel

object Algorithm {


    def train(dataNew: Dataset[Row], dataTrained: Dataset[Row], spark: SparkSession, k: Int, pivotOption:Int, ID: String = "ID"): Dataset[Clasificacion] = {


        import spark.implicits._

        println("**********************************************")
        println("              PARSEANDO TUPLAS")
        println("**********************************************")
        val dsNew = dataNew.map { row => parseTupla(row, spark, ID) }
        var dsTrained: Dataset[Tupla] = dsNew
        if (dataTrained != null) {
              dsTrained = dataTrained.map { row => parseTupla(row, spark, "id") }
        }


        println("**********************************************")
        println("            NEIGHBORHOODS SEARCH")
        println("**********************************************")

        val pivots = spark.sparkContext.broadcast(dsTrained.sample(0.001).collect())
        var neighborhoods: Dataset[Neighborhood] = pivotOption match {
            case 1 => NeighborhoodsSearch.aleatoryNeighborhoods(pivots, dsTrained, spark)
        }

        val neighborhoodsBroadcast = spark.sparkContext.broadcast(neighborhoods.collect())
        if (dataTrained != null) {
            val ppp =  dsNew.mapPartitions { newNeighbors =>
                NeighborhoodsSearch.findClosetNeighborhood(newNeighbors.toSeq, neighborhoodsBroadcast, spark)
            }
            neighborhoods = NeighborhoodsSearch.mergeNeighborhoods(ppp ,neighborhoodsBroadcast, spark)
        }


        println("**********************************************")
        println("                   FASE 1")
        println("**********************************************")
        var dsStage1: Dataset[TuplaFase1] = null

        if  (dataTrained != null) {
            dsStage1 = neighborhoods.mapPartitions { neighborhood => stage1m(neighborhood, k, spark)}
              .union( neighborhoods.mapPartitions { neighborhood => stage1mm(neighborhood, k, spark)})
        }
        else {
            dsStage1 = neighborhoods.mapPartitions { neighborhood => stage1(neighborhood, k, spark)}
              .coalesce(1)
        }

        val dsClassified = clasificar(dsStage1.map { x=>Resultado(x.id,x.ia,x.valores, x.distance) }, spark)
        dsClassified

    }

}
