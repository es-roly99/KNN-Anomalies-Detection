package knn

import org.apache.spark.sql.SparkSession

object Configuration {

    /*
     id -> column
     database -> database name
     pivotOption -> 1(Aleatory Pivot)
     distanceOption -> 1(Euclidean) 2(Manhattan) 3(Max Distance)
     seed -> seed for aleatory
     k -> number of k-neighbors
     p -> median + p * standard deviation < anomaly index = ANOMALY
     spark -> Spark Session configuration
     */

    val id: String = "id"
    val database: String = "mammography"
    val pivotOption: Int = 1
    val distanceOption: Int = 4

    //val seed: Int = 12341234
    //val seed: Int = 83272864
    val seed: Int = 18029682

    val k: Int = 20
    val p: Double = 3

    val spark: SparkSession = SparkSession
      .builder
      .appName("Spark KNN")
      .config("spark.master", "local")
      .getOrCreate()
}
