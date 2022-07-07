import knn.Algorithm
import knn.AuxiliaryClass.Clasificacion
import org.apache.spark.sql.{AnalysisException, Column, Dataset, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{concat, concat_ws, lit}
import scala.concurrent.duration.{Duration, NANOSECONDS}


object MetricsByExecution {

    def main(args: Array[String]): Unit = {

        val spark = SparkSession
          .builder
          .appName("Spark KNN")
          .config("spark.master", "local")
          .getOrCreate()

        val db = "mammography"
        val k = 5
        val p = 0.1
        var df_classified: Dataset[Clasificacion] = null
        val data = spark.read.options(Map("delimiter" -> ",", "header" -> "true")).csv("db/" + db + ".csv")

        val splitData = data.randomSplit(Array(0.8, 0.1, 0.1))

        val totalTime = splitData.map { partition =>

            val ini_time = System.nanoTime()

            try {
                val trained_data = spark.read.options(Map("delimiter" -> ",", "header" -> "true")).csv("output/result/" + db + "_" + k)
                df_classified = Algorithm.train(partition, trained_data, spark, k, p, 1)
            }
            catch {
                case _: AnalysisException => df_classified = Algorithm.train(partition, null, spark, k, p, 1)
            }

            val end_time = System.nanoTime()


            df_classified
              .withColumn("data", stringify(df_classified.col("data")))
              .withColumn("distance", stringify(df_classified.col("distance")))
              .write.mode(SaveMode.Overwrite).option("header", "true").csv("output/result/" + db + "_" + k)


            def stringify(c: Column) = concat(lit("["), concat_ws(",", c), lit("]"))


            val data = spark.read.options(Map("delimiter" -> ",", "header" -> "true")).csv("output/result/" + db + "_" + k)

            val metrics = Metrics.confusionMatrix(data, spark)
            val tp = metrics(0).toDouble
            val tn = metrics(1).toDouble
            val fp = metrics(2).toDouble
            val fn = metrics(3).toDouble

            println("************************************************")
            println("************************************************")
            println("************************************************")
            println("True Positives: " + tp)
            println("True Negatives: " + tn)
            println("False Positives: " + fp)
            println("False Negatives: " + fn)

            println("Accuracy: " + Metrics.accuracy(tp, tn, fp, fn) )
            println("Precision: " + Metrics.precision(tp, fp) )
            println("Recall: " + Metrics.recall(tp, fn) )
            println("************************************************")
            println("************************************************")
            println("************************************************")
            println("************************************************")


            Duration(end_time - ini_time, NANOSECONDS).toMillis.toDouble / 1000

        }

        import spark.implicits._
        totalTime.toSeq.toDF("Time")
          .write.mode(SaveMode.Overwrite).option("header", "true").csv("output/time/" + db + "_" + k)

    }



}
