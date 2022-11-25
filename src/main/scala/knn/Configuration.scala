package knn

import org.apache.spark.sql.SparkSession

object Configuration {


    val database: String = "satimage"
    val pivotOption: Int = 1
    val distanceOption: Int = 1
    //val seed: Int = 12341234
    //val seed: Int = 83272864
    val seed: Int = 18029682

    val k: Int = 10
    val p: Double = 3

    val spark: SparkSession = SparkSession
      .builder
      .appName("Spark KNN")
      .config("spark.master", "local")
      .getOrCreate()

}
