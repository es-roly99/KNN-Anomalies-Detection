package knn

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import java.io.{BufferedWriter, File, FileWriter}

object Main {

    def main(args: Array[String]): Unit = {

        val db = "satimage_id"

        val spark = SparkSession
          .builder
          .appName("Spark KNN")
          .config("spark.master", "local")
          .getOrCreate()

        val df = spark.read.options(Map("delimiter"->",", "header"->"true")).csv("db/"+db+".csv")

        val k = 2
        val p = 0.01

        val ini_time = System.nanoTime()
        val df_classified = Algorithm.train(df, spark, k, p)
        val end_time = System.nanoTime()

        df_classified.withColumn("data", stringify(df_classified.col("data")))
          .write.mode(SaveMode.Overwrite).option("header", "true").csv("output/result/"+db)

        import spark.implicits._
        Seq(end_time-ini_time/1000000000.toDouble).toDF("Time")
          .write.mode(SaveMode.Overwrite).option("header", "true").csv("output/time/"+db)

        def stringify(c: Column) = concat(lit("["), concat_ws(",",c), lit("]"))

    }

}
