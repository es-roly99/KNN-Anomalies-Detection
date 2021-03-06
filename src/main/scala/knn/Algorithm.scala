package knn

import knn.AuxiliaryClass._
import knn.KNN._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.storage.StorageLevel

object Algorithm {


    def train(data: Dataset[Row], dataTrain: Dataset[Row], spark: SparkSession, k: Int, p: Double, pivotOption:Int, ID: String = "ID"): Dataset[Clasificacion] = {

        import spark.implicits._


        println("**********************************************")
        println("              PARSEANDO TUPLAS")
        println("**********************************************")
        val ds = data.map { row => parseTupla(row, spark, ID) }
        var dsTrain: Dataset[Tupla] = null
        var dataset: Dataset[Tupla] = ds
        if (dataTrain != null) {
              dsTrain = dataTrain.map { row => parseTupla(row, spark, "id") }
              dataset = dsTrain.union(ds)
        }

        println("**********************************************")
        println("                PIVOT SEARCH")
        println("**********************************************")

        val pivots: Broadcast[Array[Pivot]] = pivotOption match {
            case 1 => spark.sparkContext.broadcast(PivotSearch.aleatoryPivot(dataset, spark))
           // case 2 => spark.sparkContext.broadcast(PivotSearch.piaesaPivot(spark))
        }

        println("**********************************************")
        println("                    FASE1")
        println("**********************************************")
        var dsfase1: Dataset[TuplaFase1] = null

        if (dataTrain != null) {
            val dsBroadcast = spark.sparkContext.broadcast(ds.collect())
            dsfase1 = dsTrain.mapPartitions { x => fase1(x.toArray, dsBroadcast, k, spark) }.persist(StorageLevel.MEMORY_AND_DISK_SER)
            dsfase1 = dsfase1.union (ds.mapPartitions { x => fase1(x.toArray, k, spark, pivots) }.persist(StorageLevel.MEMORY_AND_DISK_SER))
        }

        else
            dsfase1 = ds.mapPartitions { x => fase1(x.toArray, k, spark, pivots) }.persist(StorageLevel.MEMORY_AND_DISK_SER)

        val filtro = (dsfase1.count() * p).toInt


        println("**********************************************")
        println("                ORDENANDO FASE1")
        println("**********************************************")
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
        ds.unpersist()
        println("Ejecutado algoritmo de deteccion de anomalias")
        classData


    }


}
