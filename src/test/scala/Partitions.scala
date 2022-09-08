import knn.Algorithm
import knn.AuxiliaryClass.Clasificacion
import org.apache.spark.sql.{AnalysisException, Column, Dataset, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{concat, concat_ws, lit}

import scala.concurrent.duration.{Duration, NANOSECONDS}

object Partitions {

    def main(args: Array[String]): Unit = {

        val spark = SparkSession
          .builder
          .appName("Spark KNN")
          .config("spark.master", "local")
          .getOrCreate()

        val db = "satimage"
        val k = 20
        val p = 0.1
        val pivotOption = 1
        var df_classified: Dataset[Clasificacion] = null
        val data = spark.read.options(Map("delimiter"->",", "header"->"true")).csv("db/"+db+".csv")

        val splitData = data.randomSplit(Array(0.8, 0.1, 0.1))

        val totalTime  = splitData.map { partition =>

            val ini_time = System.nanoTime()

            try {
                val trained_data = spark.read.options(Map("delimiter" -> ",", "header" -> "true")).csv("output/result/" + db + "_" + k)
                df_classified = Algorithm.train(partition, trained_data, spark, k, p, pivotOption )
            }
            catch {
                case _: AnalysisException => df_classified = Algorithm.train(partition, null, spark, k, p, pivotOption)
            }

            val end_time = System.nanoTime()


            df_classified
              .withColumn("data", stringify(df_classified.col("data")))
              .withColumn("distance", stringify(df_classified.col("distance")))
              .write.mode(SaveMode.Overwrite).option("header", "true").csv("output/result/" + db + "_" + k)


            def stringify(c: Column) = concat(lit("["), concat_ws(",",c), lit("]"))
            Duration(end_time - ini_time, NANOSECONDS).toMillis.toDouble / 1000

        }

        import spark.implicits._
          totalTime.toSeq.toDF("Time")
            .write.mode(SaveMode.Overwrite).option("header", "true").csv("output/time/" + db + "_" + k)

    }




}
