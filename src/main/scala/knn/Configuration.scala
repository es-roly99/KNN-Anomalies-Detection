package knn

import org.apache.spark.sql.SparkSession

object Configuration {

    /*
     id -> column
     database -> database name
     pivotOption -> 1(Aleatory Pivot)
     distanceOption -> 1(Euclidean) 2(Manhattan) 3(Max Distance)
     partitions -> number of partitions from 1 to 10
     seed -> seed for aleatory
     k -> number of k-neighbors
     p -> median + p * standard deviation < anomaly index = ANOMALY
     spark -> Spark Session configuration
     */

    val id: String = "id"
    val database: String = "satimage"
    val results: String = "results"
    val temporary: String = "temporary"

    //val seed: Int = 12341234
    //val seed: Int = 83272864
    val seed: Int = 18029682

    val k: Int = 5
    val p: Double = 3
    val pivotOption: Int = 1
    val distanceOption: Int = 2
    val partitions: Int = 5

    val spark: SparkSession = SparkSession
      .builder
      .appName("Spark KNN")
      .config("spark.master", "local")
      .getOrCreate()
}
