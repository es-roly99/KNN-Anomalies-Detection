package knn

import org.apache.spark.sql.SparkSession

object Main {

    def main(args: Array[String]): Unit = {

        val db = "iris.csv"

        val spark = SparkSession
          .builder
          .appName("Spark KNN")
          .config("spark.master", "local")
          .getOrCreate()

        val df = spark.read.options(Map("delimiter"->",", "header"->"true")).csv("db/"+db)
        val k = 2
        val p = 0.01

        val df_classified = Algorithm.train(df, spark, k, p, "label")
    }

}
