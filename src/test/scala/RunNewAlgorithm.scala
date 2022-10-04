import knn.{Algorithm, Configuration}
import knn.AuxiliaryClass.Result
import org.apache.spark.sql.{AnalysisException, Column, Dataset, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{concat, concat_ws, lit}

import scala.concurrent.duration.{Duration, NANOSECONDS}


object RunNewAlgorithm {

    def main(args: Array[String]): Unit = {

        val spark = Configuration.spark
        val db = Configuration.database
        val k = Configuration.k
        val pivotOption = Configuration.pivotOption

        var df_classified: Dataset[Result] = null
        val data = spark.read.options(Map("delimiter" -> ",", "header" -> "true")).csv("db/" + db + ".csv")
        val splitData = data.randomSplit(Array(0.7, 0.1, 0.1, 0.1), seed = Configuration.seed)


        val dataResult = splitData.map { partition =>

            val ini_time = System.nanoTime()

            try {
                val trained_data = spark.read.options(Map("delimiter" -> ",", "header" -> "true")).csv("result/" + db + "_" + k)
                df_classified = Algorithm.train(partition, trained_data, spark, k, pivotOption)
            }
            catch {
                case _: AnalysisException => df_classified = Algorithm.train(partition, null, spark, k, pivotOption)
            }

            val end_time = System.nanoTime()


            df_classified
              .withColumn("values", stringify(df_classified.col("values")))
              .withColumn("distances", stringify(df_classified.col("distances")))
              .write.mode(SaveMode.Overwrite).option("header", "true").csv("result/" + db + "_" + k)


            def stringify(c: Column) = concat(lit("["), concat_ws(",", c), lit("]"))


            val data = spark.read.options(Map("delimiter" -> ",", "header" -> "true")).csv("result/" + db + "_" + k)

            val metrics = Metrics.confusionMatrix(data, spark)

            val tp = metrics(0).toDouble
            val tn = metrics(1).toDouble
            val fp = metrics(2).toDouble
            val fn = metrics(3).toDouble
            val accuracy = BigDecimal (Metrics.accuracy(tp, tn, fp, fn) * 100).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
            val precision = BigDecimal (Metrics.precision(tp, fp) * 100).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
            val recall = BigDecimal (Metrics.recall(tp, fn) * 100).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble

            (Duration(end_time - ini_time, NANOSECONDS).toMillis.toDouble / 1000, tp, tn, fp, fn, accuracy, precision, recall)
        }


        println("************************************************")
        println("************************************************")
        println("************************************************")
        println("Time: " + dataResult.map(_._1).mkString("", "seg , ", ""))
        println("True Positives: " + dataResult.map(_._2).mkString("", ", ", ""))
        println("True Negatives: " + dataResult.map(_._3).mkString("", ", ", ""))
        println("False Positives: " + dataResult.map(_._4).mkString("", ", ", ""))
        println("False Negatives: " + dataResult.map(_._5).mkString("", ", ", ""))
        println("Accuracy: " + dataResult.map(_._6).mkString("", ", ", ""))
        println("Precision: " + dataResult.map(_._7).mkString("", ", ", ""))
        println("Recall: " + dataResult.map(_._8).mkString("", ", ", ""))
        println("************************************************")
        println("************************************************")
        println("************************************************")
        println("************************************************")

    }



}
