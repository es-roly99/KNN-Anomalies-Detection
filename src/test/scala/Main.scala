import OldAlgorithm.Clasificacion
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import scala.concurrent.duration.{Duration, NANOSECONDS}

object Main {

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
        val data = spark.read.options(Map("delimiter"->",", "header"->"true")).csv("db/"+db+".csv")
          .randomSplit(Array(x1))


        val ini_time = System.nanoTime()

        df_classified = OldAlgorithm.train(data(0), spark, k, p)

        val end_time = System.nanoTime()


        df_classified
          .withColumn("data", stringify(df_classified.col("data")))
          .write.mode(SaveMode.Overwrite).option("header", "true").csv("output/result/" + db + "_" + k)

        import spark.implicits._
        Seq(Duration(end_time-ini_time,NANOSECONDS).toMillis.toDouble/1000).toDF("Time")
          .write.mode(SaveMode.Overwrite).option("header", "true").csv("output/time/" + db + "_" + k)


        def stringify(c: Column) = concat(lit("["), concat_ws(",",c), lit("]"))

    }

}
