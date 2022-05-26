package knn

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object Main {

    def main(args: Array[String]): Unit = {

        val db = "satimage_id"

        val spark = SparkSession
          .builder
          .appName("Spark KNN")
          .config("spark.master", "local")
          .getOrCreate()

        var df = spark.read.options(Map("delimiter"->",", "header"->"true")).csv("db/"+db+".csv")
        val df_temporary = spark.read.options(Map("delimiter"->",", "header"->"true")).csv("results/"+db)

        val k = 2
        val p = 0.01

        val df_classified = Algorithm.train(df, spark, k, p)
        df_classified.withColumn("data", stringify(df_classified.col("data")))
          .write.mode(SaveMode.Overwrite).option("header", "true").csv("results/"+db)

        def stringify(c: Column) = concat(lit("["), concat_ws(",",c), lit("]"))




    }

}
