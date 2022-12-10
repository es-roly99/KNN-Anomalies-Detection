package knn

import knn.AuxiliaryClass._
import knn.Distance._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{Dataset, SparkSession}


object NeighborhoodsSearch {

    /** aleatoryNeighborhoods es la funcion encargada de crear vacindades a partir de la seleccionde pivotes aleatorios
     *
     * @param list Dataset de Tuplas que representan todas las instancias de la base de datos
     * @param spark SparkSession
     * @return Dataset de Neighborhood con las vecindades creadas
     */
    def aleatoryNeighborhoods(list: Dataset[Tuple], distance: Distance, spark: SparkSession): Dataset[Neighborhood] = {
        val pivots = list.sample(0.001, seed = Configuration.seed).collect()
        import spark.implicits._
        list.mapPartitions { x =>
            findNeighborhood(x.toSeq, pivots, distance, spark)
        }.groupByKey(_.pivot).mapGroups { (key, value) =>
            Neighborhood(key, value.flatMap(_.neighbors).toSeq, Seq[Tuple]())
        }
    }


    /** mergeNeighborhoods es la funcion encargada de mezclar vecindades que tienen el mismo pivote como centro
     *
     * @param closetNeighbors Dataset de las nuevas vecindades de todas las particiones
     * @param neighborhoods Array de las anterirores vecindades
     * @param spark SparkSession
     * @return Dataset de las vecindades con los anteriores vecinos y los nuevos vecinos cercanos
     */
    def mergeNeighborhoods(closetNeighbors: Dataset[Neighborhood], neighborhoods: Array[Neighborhood], spark: SparkSession): Dataset[Neighborhood] = {
        import spark.implicits._
        closetNeighbors.groupByKey(_.pivot).mapGroups { (key, value) =>
            val neighbors = neighborhoods.find(_.pivot.id == key.id).toSeq.head.neighbors
            val neighborsNew = value.flatMap(_.neighbors).toSeq
            Neighborhood(key, neighbors, neighborsNew)
        }
    }


    /** findNeighborhood es la funcion encargadad de encontar la vecindad a la que pertenece cada Tupla
     *
     * @param neighbors Sequencia Tuplas que representan los vecinos
     * @param pivots Array de Tupla que representan los centros de vecindades
     * @param spark SparkSession
     * @return
     */
    def findNeighborhood(neighbors: Seq[Tuple], pivots: Array[Tuple], distance: Distance, spark: SparkSession): Iterator[Neighborhood] = {
        neighbors.map { neighbor =>
            pivots.map { pivot =>
                (pivot, neighbor, distance.calculate(pivot.values, neighbor.values, spark))
            }.reduce { (x,y) => if(x._3 < y._3) x else y}
        }.groupBy(_._1)
          .map { x => Neighborhood(x._1, x._2.map(_._2), Seq[Tuple]())}
          .iterator
    }


    }
