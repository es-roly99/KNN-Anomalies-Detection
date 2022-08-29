package knn

import knn.AuxiliaryClass._
import knn.KNN._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.storage.StorageLevel

object Algorithm {


    def train(dataNew: Dataset[Row], dataTrained: Dataset[Row], spark: SparkSession, k: Int, p: Double, pivotOption:Int, ID: String = "ID"): Dataset[Clasificacion] = {

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
        println("                PIVOT SEARCH")
        println("**********************************************")
        var neighborhoods: Dataset[Neighborhood] = pivotOption match {
            case 1 => PivotSearch.aleatoryPivot(dsTrained, spark)
           // case 2 => PivotSearch.piaesaPivot(spark)
        }
        if (dataTrained != null) {
            neighborhoods = PivotSearch.setCloserNeighborhood(neighborhoods, dsNew.collect(), spark)
        }


        println("**********************************************")
        println("                   FASE 1")
        println("**********************************************")
        var dsfase1: Dataset[TuplaFase1] = null

        if (dataTrained != null) {
            val dsNewBroadcast = spark.sparkContext.broadcast(dsNew.collect())
            dsfase1 = dsTrained.mapPartitions { x => stage1(x.toArray, dsNewBroadcast, k, spark) }.persist(StorageLevel.MEMORY_AND_DISK_SER)
           //   .union()
            //dsfase1 = dsfase1.union (ds.mapPartitions { x => fase1(x.toArray, k, spark, pivots) }.persist(StorageLevel.MEMORY_AND_DISK_SER))
        }

        else {
            dsfase1 = neighborhoods.mapPartitions { neighborhood => stage1(neighborhood, k, spark)}.coalesce(1).persist(StorageLevel.MEMORY_AND_DISK_SER)
        }


        println("**********************************************")
        println("                ORDENANDO FASE1")
        println("**********************************************")
        val filtro = (dsfase1.count() * p).toInt
        val lim = dsfase1.sort(col("ia").desc).limit(filtro).persist(StorageLevel.MEMORY_AND_DISK_SER)
        val broadcast = spark.sparkContext.broadcast(lim.collect())


        println("**********************************************")
        println("                    FASE2")
        println("**********************************************")
        val outlier = fase2(broadcast, dsfase1, k, spark).persist(StorageLevel.MEMORY_AND_DISK_SER)
        val bc = spark.sparkContext.broadcast(outlier.collect())


        println("**********************************************")
        println("                    UPDATE")
        println("**********************************************")
        val resultado = dsfase1.mapPartitions { iter => update(bc, iter.toArray, spark) }.persist(StorageLevel.MEMORY_AND_DISK_SER)
        val classData = clasificar(resultado, spark)


        resultado.count()
        outlier.unpersist()
        lim.unpersist()
        dsfase1.unpersist()
        dsNew.unpersist()
        println("Ejecutado algoritmo de deteccion de anomalias")
        classData

    }

}
