package knn

import knn.AuxiliaryClass.Clasificacion
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import java.io.FileNotFoundException
import scala.concurrent.duration.{Duration, NANOSECONDS}

object Main {

    def main(args: Array[String]): Unit = {

        val spark = SparkSession
          .builder
          .appName("Spark KNN")
          .config("spark.master", "local")
          .getOrCreate()

        val db = "annthyroid"
        val k = 3
        val p = 0.01
        var df_classified: Dataset[Clasificacion] = null
        val data = spark.read.options(Map("delimiter"->",", "header"->"true")).csv("db/"+db+".csv")


        val ini_time = System.nanoTime()

        try {
            val trained_data = spark.read.options(Map("delimiter"->",", "header"->"true")).csv("output/result/"+db +"_"+k)
            df_classified = Algorithm.train(data, trained_data, spark, k, p)
        }
        catch {
            case _: AnalysisException => df_classified = Algorithm.train(data, null, spark, k, p)
        }

        val end_time = System.nanoTime()


        df_classified
          .withColumn("data", stringify(df_classified.col("data")))
          .withColumn("distance", stringify(df_classified.col("distance")))
          .write.mode(SaveMode.Overwrite).option("header", "true").csv("output/result/" + db + "_" + k)

        import spark.implicits._
        Seq(Duration(end_time-ini_time,NANOSECONDS).toMillis.toDouble/1000).toDF("Time")
          .write.mode(SaveMode.Overwrite).option("header", "true").csv("output/time/" + db + "_" + k)


        def stringify(c: Column) = concat(lit("["), concat_ws(",",c), lit("]"))

    }

}
