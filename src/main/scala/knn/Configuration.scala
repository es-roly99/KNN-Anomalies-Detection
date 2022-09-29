package knn

import org.apache.spark.sql.SparkSession

object Configuration {

    val database: String = "satimage"
    val pivotOption: Int = 1
    val seed: Int = 92627198
    val k: Int = 50
    val p: Double = 3

    val spark: SparkSession = SparkSession
      .builder
      .appName("Spark KNN")
      .config("spark.master", "local")
      .getOrCreate()

}
