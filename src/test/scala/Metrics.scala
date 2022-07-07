import org.apache.spark.sql._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD._



object Metrics {


    def main(args: Array[String]): Unit = {

        val spark = SparkSession
          .builder
          .appName("Spark KNN")
          .config("spark.master", "local")
          .getOrCreate()

        val db = "mammography"
        val k = 5

        val data = spark.read.options(Map("delimiter" -> ",", "header" -> "true")).csv("output/result/" + db + "_" + k)

        val metrics = Metrics.confusionMatrix(data, spark)
        val tp = metrics(0).toDouble
        val tn = metrics(1).toDouble
        val fp = metrics(2).toDouble
        val fn = metrics(3).toDouble

        println("True Positives: " + tp)
        println("True Negatives: " + tn)
        println("False Positives: " + fp)
        println("False Negatives: " + fn)

        println("Accuracy: " + Metrics.accuracy(tp, tn, fp, fn) )
        println("Precision: " + Metrics.precision(tp, fp) )
        println("Recall: " + Metrics.recall(tp, fn) )


    }


    def confusionMatrix(dataset: Dataset[Row], spark: SparkSession): Array[Int] = {

        import spark.implicits._

        val a = dataset.map { x =>

            val v = x.getString(x.fieldIndex("data"))
            val realValue = v.substring(1, v.length-1).split(',').map{ x => x.toDouble}.last.toInt
            val prediction = if(x.getString(x.fieldIndex("tipo")) == "normal") 0 else 1

            if (realValue == 0 && prediction == 0) "tn"
            else if (realValue == 1 && prediction == 1) "tp"
            else if (realValue == 0 && prediction == 1) "fp"
            else "fn"

        }.collect()

        Array(a.count(_=="tp") , a.count(_=="tn") , a.count(_=="fp") , a.count(_=="fn") )
    }


    def accuracy (tp: Double, tn: Double, fp: Double, fn: Double): Double = (tp + tn) / (tp + tn + fp + fn)

    def precision (tp: Double, fp: Double): Double = tp / (tp + fp)

    def recall (tp: Double, fn: Double): Double = tp / (tp + fn)

}
