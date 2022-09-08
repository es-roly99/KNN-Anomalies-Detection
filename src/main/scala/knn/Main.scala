package knn

import knn.AuxiliaryClass.Clasificacion
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import scala.concurrent.duration.{Duration, NANOSECONDS}

object Main {

    def main(args: Array[String]): Unit = {

        val spark = SparkSession
          .builder
          .appName("Spark KNN")
          .config("spark.master", "local")
          .getOrCreate()

        val db = "satimage"
        val k = 5
        val p = 0.1
        val pivotOption = 1
        val dataNew = spark.read.options(Map("delimiter"->",", "header"->"true")).csv("db/"+db+".csv")
        var dataResult: Dataset[Clasificacion] = null


        val iniTime = System.nanoTime()
        try {
            val dataTrained = spark.read.options(Map("delimiter"->",", "header"->"true")).csv("output/result/"+db +"_"+k)
            dataResult = Algorithm.train(dataNew, dataTrained, spark, k, p, pivotOption)
        }
        catch {
            case _: AnalysisException => dataResult = Algorithm.train(dataNew, null, spark, k, p, pivotOption)
        }
        val endTime = System.nanoTime()


        dataResult
          .withColumn("data", stringify(dataResult.col("data")))
          .withColumn("distance", stringify(dataResult.col("distance")))
          .write.mode(SaveMode.Overwrite).option("header", "true").csv("output/result/" + db + "_" + k)

        import spark.implicits._
        Seq(Duration(endTime-iniTime,NANOSECONDS).toMillis.toDouble/1000).toDF("Time")
          .write.mode(SaveMode.Overwrite).option("header", "true").csv("output/time/" + db + "_" + k)


        def stringify(c: Column) = concat(lit("["), concat_ws(",",c), lit("]"))

    }

}
